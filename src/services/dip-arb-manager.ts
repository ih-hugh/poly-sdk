/**
 * DipArbManager - Multi-Market Orchestrator
 *
 * Manages multiple DipArbService instances for parallel trading across
 * different underlyings (BTC, ETH, SOL, XRP).
 *
 * Key features:
 * - Shared resources (RealtimeService, TradingService, MarketService)
 * - Independent state per market
 * - Event aggregation with market context
 */

import { EventEmitter } from 'events';
import { DipArbService } from './dip-arb-service.js';
import { RealtimeServiceV2 } from './realtime-service-v2.js';
import { TradingService } from './trading-service.js';
import { MarketService } from './market-service.js';
import { BinanceService } from './binance-service.js';
import {
  type DipArbMarketConfig,
  type DipArbServiceConfig,
  type DipArbStats,
  type DipArbUnderlying,
  type DipArbDurationString,
  type DipArbAutoRotateConfig,
} from './dip-arb-types.js';

// ===== Types =====

export interface DipArbManagerConfig {
  /** Shared config applied to all markets */
  sharedConfig?: Partial<DipArbServiceConfig>;
  /** Enable debug logging */
  debug?: boolean;
}

export interface ActiveMarket {
  /** The underlying asset */
  coin: DipArbUnderlying;
  /** Market condition ID */
  conditionId: string;
  /** Market configuration */
  market: DipArbMarketConfig;
  /** The DipArbService instance for this market */
  service: DipArbService;
  /** Start time */
  startTime: number;
  /** User ID (for paper trading) */
  userId: string | null;
}

export interface MarketStatus {
  coin: DipArbUnderlying;
  conditionId: string;
  marketName: string;
  isRunning: boolean;
  stats: DipArbStats;
  round?: {
    roundId: string;
    phase: string;
    priceToBeat: number;
    startTime: number;
    endTime: number;
    upOpen: number;
    downOpen: number;
  };
}

// ===== DipArbManager Class =====

export class DipArbManager extends EventEmitter {
  // Shared services (passed to all DipArbService instances)
  private realtimeService: RealtimeServiceV2;
  private tradingService: TradingService | null;
  private marketService: MarketService;
  private binanceService: BinanceService | null;
  private privateKey?: string;
  private chainId: number;

  // Active markets map: coin -> ActiveMarket
  private activeMarkets: Map<DipArbUnderlying, ActiveMarket> = new Map();

  // Configuration
  private config: DipArbManagerConfig;
  private sharedServiceConfig: Partial<DipArbServiceConfig> = {};

  // Health monitoring
  private healthCheckInterval: ReturnType<typeof setInterval> | null = null;
  private readonly SEARCH_TIMEOUT_MS = 60000; // 60 seconds max search time

  // Debug logging
  private debug: boolean;

  constructor(
    realtimeService: RealtimeServiceV2,
    tradingService: TradingService | null,
    marketService: MarketService,
    privateKey?: string,
    chainId: number = 137,
    binanceService?: BinanceService,
    config: DipArbManagerConfig = {}
  ) {
    super();

    this.realtimeService = realtimeService;
    this.tradingService = tradingService;
    this.marketService = marketService;
    this.binanceService = binanceService ?? null;
    this.privateKey = privateKey;
    this.chainId = chainId;
    this.config = config;
    this.debug = config.debug ?? false;

    if (config.sharedConfig) {
      this.sharedServiceConfig = config.sharedConfig;
    }

    this.log('DipArbManager initialized');
  }

  private log(message: string): void {
    if (this.debug) {
      console.log(`[DipArbManager] ${message}`);
    }
  }

  // ===== Public API: Market Management =====

  /**
   * Start trading a specific coin
   * Finds the best available market and starts a DipArbService for it
   */
  async startCoin(
    coin: DipArbUnderlying,
    options: {
      userId?: string;
      preferDuration?: DipArbDurationString;
      config?: Partial<DipArbServiceConfig>;
    } = {}
  ): Promise<DipArbMarketConfig | null> {
    // Check if already running
    if (this.activeMarkets.has(coin)) {
      this.log(`${coin} already running, skipping`);
      return this.activeMarkets.get(coin)!.market;
    }

    this.log(`Starting ${coin}...`);

    // Create a NEW RealtimeServiceV2 for each coin to avoid Polymarket's
    // per-connection subscription limit bug (only first clob_market sub works)
    const coinRealtimeService = new RealtimeServiceV2({ debug: this.debug });

    // Create a new DipArbService instance for this coin with its own WebSocket
    const service = new DipArbService(
      coinRealtimeService,
      this.tradingService,
      this.marketService,
      this.privateKey,
      this.chainId,
      this.binanceService ?? undefined
    );

    // Apply shared config + per-market overrides
    // Note: debug comes from sharedServiceConfig (set via updateSharedConfig) or options.config
    const mergedConfig = {
      ...this.sharedServiceConfig,
      ...options.config,
    };
    // Also update manager's debug flag if set in config
    if (mergedConfig.debug !== undefined) {
      this.debug = mergedConfig.debug;
    }
    service.updateConfig(mergedConfig);

    // Setup event forwarding with market context
    this.setupEventForwarding(service, coin);

    // Find and start the market
    const market = await service.findAndStart({
      coin,
      preferDuration: options.preferDuration ?? '15m',
    });

    if (!market) {
      this.log(`No market found for ${coin}`);
      return null;
    }

    // Track the active market
    this.activeMarkets.set(coin, {
      coin,
      conditionId: market.conditionId,
      market,
      service,
      startTime: Date.now(),
      userId: options.userId ?? null,
    });

    this.log(`${coin} started on market: ${market.name}`);
    this.emit('marketStarted', { coin, market });

    // Start health monitoring if not already running
    this.startHealthCheck();

    return market;
  }

  /**
   * Start multiple coins sequentially with throttling to avoid rate limits
   * Each coin startup involves multiple REST API calls for market discovery,
   * so we stagger them to avoid hitting Polymarket's rate limits.
   */
  async startCoins(
    coins: DipArbUnderlying[],
    options: {
      userId?: string;
      preferDuration?: DipArbDurationString;
      config?: Partial<DipArbServiceConfig>;
      /** Delay between starting each coin in ms (default: 2000) */
      staggerDelayMs?: number;
    } = {}
  ): Promise<Map<DipArbUnderlying, DipArbMarketConfig | null>> {
    const results = new Map<DipArbUnderlying, DipArbMarketConfig | null>();
    const staggerDelay = options.staggerDelayMs ?? 2000;

    // Start coins sequentially with delay to avoid rate limiting
    for (let i = 0; i < coins.length; i++) {
      const coin = coins[i];

      // Add delay between coins (not before first one)
      if (i > 0 && staggerDelay > 0) {
        this.log(`Waiting ${staggerDelay}ms before starting ${coin}...`);
        await new Promise(resolve => setTimeout(resolve, staggerDelay));
      }

      try {
        const market = await this.startCoin(coin, options);
        results.set(coin, market);
      } catch (error) {
        this.log(`[ERROR] Failed to start ${coin}: ${error}`);
        results.set(coin, null);
      }
    }

    return results;
  }

  /**
   * Stop trading a specific coin
   */
  async stopCoin(coin: DipArbUnderlying): Promise<void> {
    const active = this.activeMarkets.get(coin);
    if (!active) {
      this.log(`${coin} not running, nothing to stop`);
      return;
    }

    this.log(`Stopping ${coin}...`);

    // Stop the service
    await active.service.stop();

    // Remove from active markets
    this.activeMarkets.delete(coin);

    this.log(`${coin} stopped`);
    this.emit('marketStopped', { coin, conditionId: active.conditionId });
  }

  /**
   * Stop all running markets
   */
  async stopAll(): Promise<void> {
    this.log('Stopping all markets...');

    // Stop health monitoring
    this.stopHealthCheck();

    const stopPromises = Array.from(this.activeMarkets.keys()).map((coin) =>
      this.stopCoin(coin)
    );

    await Promise.all(stopPromises);

    this.log('All markets stopped');
    this.emit('allStopped');
  }

  // ===== Public API: Status & Info =====

  /**
   * Get status of a specific coin
   */
  getMarketStatus(coin: DipArbUnderlying): MarketStatus | null {
    const active = this.activeMarkets.get(coin);
    if (!active) return null;

    const stats = active.service.getStats();
    const currentRound = active.service.getCurrentRound();

    return {
      coin,
      conditionId: active.conditionId,
      marketName: active.market.name,
      isRunning: active.service.isActive(),
      stats,
      round: currentRound
        ? {
            roundId: currentRound.roundId,
            phase: currentRound.phase,
            priceToBeat: currentRound.priceToBeat ?? 0,
            startTime: currentRound.startTime,
            endTime: currentRound.endTime,
            upOpen: currentRound.openPrices?.up ?? 0,
            downOpen: currentRound.openPrices?.down ?? 0,
          }
        : undefined,
    };
  }

  /**
   * Get status of all running markets
   */
  getAllMarketsStatus(): MarketStatus[] {
    return Array.from(this.activeMarkets.keys())
      .map((coin) => this.getMarketStatus(coin))
      .filter((status): status is MarketStatus => status !== null);
  }

  /**
   * Get list of running coins
   */
  getRunningCoins(): DipArbUnderlying[] {
    return Array.from(this.activeMarkets.keys());
  }

  /**
   * Check if a coin is running
   */
  isCoinRunning(coin: DipArbUnderlying): boolean {
    return this.activeMarkets.has(coin);
  }

  /**
   * Check if any market is running
   */
  isAnyRunning(): boolean {
    return this.activeMarkets.size > 0;
  }

  /**
   * Get service for a specific coin (for advanced use)
   */
  getService(coin: DipArbUnderlying): DipArbService | null {
    return this.activeMarkets.get(coin)?.service ?? null;
  }

  // ===== Public API: Configuration =====

  /**
   * Update shared config (applies to future markets)
   */
  updateSharedConfig(config: Partial<DipArbServiceConfig>): void {
    this.sharedServiceConfig = {
      ...this.sharedServiceConfig,
      ...config,
    };
    this.log(`Shared config updated: ${JSON.stringify(config)}`);
  }

  /**
   * Update config for a specific running market
   */
  updateMarketConfig(
    coin: DipArbUnderlying,
    config: Partial<DipArbServiceConfig>
  ): void {
    const active = this.activeMarkets.get(coin);
    if (!active) {
      this.log(`Cannot update config: ${coin} not running`);
      return;
    }
    active.service.updateConfig(config);
    this.log(`${coin} config updated`);
  }

  /**
   * Enable auto-rotate for a running market
   */
  enableAutoRotate(
    coin: DipArbUnderlying,
    config: Partial<DipArbAutoRotateConfig> = {}
  ): void {
    const active = this.activeMarkets.get(coin);
    if (!active) {
      this.log(`Cannot enable auto-rotate: ${coin} not running`);
      return;
    }
    active.service.enableAutoRotate({
      ...config,
      underlyings: [coin], // Only rotate within this coin's markets
    });
    this.log(`${coin} auto-rotate enabled`);
  }

  // ===== Public API: Health Monitoring =====

  /**
   * Start health monitoring to detect and recover from stuck states
   * Checks all active markets for:
   * - Stuck searching state (> 60 seconds)
   * - Inactive services that should be running
   */
  startHealthCheck(): void {
    if (this.healthCheckInterval) {
      return; // Already running
    }

    this.log('Starting health check (every 15s)');

    this.healthCheckInterval = setInterval(() => {
      this.performHealthCheck();
    }, 15000);
  }

  /**
   * Stop health monitoring
   */
  stopHealthCheck(): void {
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
      this.healthCheckInterval = null;
      this.log('Health check stopped');
    }
  }

  /**
   * Perform a single health check across all markets
   */
  private performHealthCheck(): void {
    const now = Date.now();

    for (const [coin, active] of this.activeMarkets) {
      const service = active.service;

      // Check for stuck searching state
      if (service.isSearching()) {
        const searchStartTime = service.getSearchStartTime();
        if (searchStartTime && now - searchStartTime > this.SEARCH_TIMEOUT_MS) {
          const stuckDuration = Math.round((now - searchStartTime) / 1000);
          this.log(`⚠️ ${coin} stuck searching for ${stuckDuration}s, forcing recovery`);

          // Force cleanup
          service.forceStopSearching();
          this.activeMarkets.delete(coin);

          // Emit event so backend/frontend can react
          this.emit('stuckDetected', {
            coin,
            state: 'searching',
            stuckDurationMs: now - searchStartTime,
          });

          this.emit('marketStopped', {
            coin,
            conditionId: active.conditionId,
            unexpected: true,
            reason: 'stuck_searching',
          });
        }
      }

      // Check for inactive service that should be running
      if (!service.isActive() && this.activeMarkets.has(coin)) {
        this.log(`⚠️ ${coin} service inactive but still in activeMarkets, cleaning up`);
        this.activeMarkets.delete(coin);

        this.emit('marketStopped', {
          coin,
          conditionId: active.conditionId,
          unexpected: true,
          reason: 'service_inactive',
        });
      }
    }
  }

  // ===== Private: Event Forwarding =====

  /**
   * Setup event forwarding from a DipArbService to the manager
   * All events are enriched with coin/market context
   */
  private setupEventForwarding(
    service: DipArbService,
    coin: DipArbUnderlying
  ): void {
    // Forward signals with market context
    service.on('signal', (signal) => {
      this.emit('signal', {
        ...signal,
        coin,
        marketConditionId: this.activeMarkets.get(coin)?.conditionId,
        marketName: this.activeMarkets.get(coin)?.market.name,
      });
    });

    // Forward executions with market context
    service.on('execution', (execution) => {
      this.emit('execution', {
        ...execution,
        coin,
        marketConditionId: this.activeMarkets.get(coin)?.conditionId,
        marketName: this.activeMarkets.get(coin)?.market.name,
      });
    });

    // Forward paper trades with market context
    service.on('paperTrade', (trade) => {
      this.emit('paperTrade', {
        ...trade,
        coin,
        marketConditionId: this.activeMarkets.get(coin)?.conditionId,
        marketName: this.activeMarkets.get(coin)?.market.name,
      });
    });

    // Forward round events with market context
    // Note: DipArbService emits 'newRound', we forward as 'round' for backend compatibility
    service.on('newRound', (round) => {
      this.emit('round', {
        ...round,
        coin,
        marketConditionId: this.activeMarkets.get(coin)?.conditionId,
        marketName: this.activeMarkets.get(coin)?.market.name,
      });
    });

    // Forward price updates with market context
    service.on('priceUpdate', (update) => {
      this.emit('priceUpdate', {
        ...update,
        coin,
      });
    });

    // Forward rotation events
    service.on('rotate', (event) => {
      // Update the active market reference
      const active = this.activeMarkets.get(coin);
      if (active) {
        // The market config has changed due to rotation
        // We need to get the new market from the service
        const newMarket = service.getMarket();
        if (newMarket) {
          active.conditionId = newMarket.conditionId;
          active.market = newMarket;
        }
      }
      this.emit('rotate', {
        ...event,
        coin,
      });
    });

    // Forward settle events
    service.on('settled', (result) => {
      this.emit('settled', {
        ...result,
        coin,
        marketConditionId: this.activeMarkets.get(coin)?.conditionId,
      });
    });

    // Forward errors
    service.on('error', (error) => {
      this.emit('error', {
        error,
        coin,
        marketConditionId: this.activeMarkets.get(coin)?.conditionId,
      });
    });

    // Handle stopped event (service stopped itself, e.g., due to rotation failure)
    service.on('stopped', () => {
      // If the service stopped itself unexpectedly, clean up
      if (this.activeMarkets.has(coin)) {
        this.log(`${coin} service stopped unexpectedly`);
        this.activeMarkets.delete(coin);
        this.emit('marketStopped', {
          coin,
          conditionId: this.activeMarkets.get(coin)?.conditionId,
          unexpected: true,
        });
      }
    });

    // Forward searching events
    service.on('searching', (info) => {
      this.emit('searching', {
        ...info,
        coin,
      });
    });

    // Forward marketFound events
    service.on('marketFound', (info) => {
      this.emit('marketFound', {
        ...info,
        coin,
      });
    });

    // Forward noMarketsAvailable events
    service.on('noMarketsAvailable', (info) => {
      this.emit('noMarketsAvailable', {
        ...info,
        coin,
      });
    });

    // Forward searchComplete events (for tracking search success/failure)
    service.on('searchComplete', (info) => {
      this.emit('searchComplete', {
        ...info,
        coin,
      });
    });

    // Forward rotationFailed events (for tracking rotation failures)
    service.on('rotationFailed', (info) => {
      this.emit('rotationFailed', {
        ...info,
        coin,
      });

      // Clean up the market on rotation failure
      if (this.activeMarkets.has(coin)) {
        this.log(`${coin} rotation failed, cleaning up`);
        this.activeMarkets.delete(coin);
        this.emit('marketStopped', {
          coin,
          conditionId: info.previousMarket,
          unexpected: true,
          reason: 'rotation_failed',
        });
      }
    });

    // Forward open position events
    service.on('openPositionCreated', (position) => {
      this.emit('openPositionCreated', {
        ...position,
        coin,
      });
    });

    service.on('openPositionClosed', (position) => {
      this.emit('openPositionClosed', {
        ...position,
        coin,
      });
    });
  }
}

export default DipArbManager;
