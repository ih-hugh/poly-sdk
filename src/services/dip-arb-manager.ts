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
import { ClobWebSocketService } from './clob-websocket-service.js';
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

/**
 * Coin-specific configuration overrides
 * Allows tuning parameters per asset based on market dynamics
 */
export interface CoinSpecificConfig {
  /** Override dipThreshold for this coin */
  dipThreshold?: number;
  /** Override sumTarget for this coin */
  sumTarget?: number;
  /** Override maxRequiredLeg2Drop for this coin */
  maxRequiredLeg2Drop?: number;
  /** Disable trading for this coin */
  enabled?: boolean;
  /** Override shares for this coin */
  shares?: number;
}

export interface DipArbManagerConfig {
  /** Shared config applied to all markets */
  sharedConfig?: Partial<DipArbServiceConfig>;
  /** Coin-specific configuration overrides */
  coinConfigs?: Record<DipArbUnderlying, CoinSpecificConfig>;
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
  /** The ClobWebSocketService instance for orderbook data */
  clobService: ClobWebSocketService;
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
  private coinConfigs: Partial<Record<DipArbUnderlying, CoinSpecificConfig>> = {};

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

    if (config.coinConfigs) {
      this.coinConfigs = config.coinConfigs;
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
    console.log(`[DipArbManager] startCoin called for ${coin}`);

    // Check if coin is enabled via coinConfigs
    const coinConfig = this.coinConfigs[coin];
    if (coinConfig?.enabled === false) {
      console.log(`[DipArbManager] ${coin} is disabled via coinConfigs, skipping`);
      this.log(`${coin} is disabled via coinConfigs, skipping`);
      return null;
    }

    // Check if already running
    if (this.activeMarkets.has(coin)) {
      console.log(`[DipArbManager] ${coin} already running, skipping`);
      this.log(`${coin} already running, skipping`);
      return this.activeMarkets.get(coin)!.market;
    }

    console.log(`[DipArbManager] Starting ${coin}...`);
    this.log(`Starting ${coin}...`);

    // Create a NEW RealtimeServiceV2 for each coin for Chainlink price feeds (RTDS)
    const coinRealtimeService = new RealtimeServiceV2({ debug: this.debug });

    // Create a NEW ClobWebSocketService for each coin for orderbook data
    // This uses the new CLOB WebSocket endpoint (wss://ws-subscriptions-clob.polymarket.com/ws/market)
    // which replaced the deprecated CLOB messages on RTDS
    const coinClobService = new ClobWebSocketService({ debug: this.debug });

    // Connect the CLOB WebSocket (realtimeService connects inside DipArbService.start())
    coinClobService.connect();

    // Create a new DipArbService instance for this coin with its own WebSocket connections
    const service = new DipArbService(
      coinRealtimeService,
      this.tradingService,
      this.marketService,
      this.privateKey,
      this.chainId,
      this.binanceService ?? undefined,
      coinClobService  // NEW: pass CLOB WebSocket service for orderbook
    );

    // Apply config in priority order: sharedServiceConfig < coinConfigs[coin] < options.config
    // This allows per-coin tuning while still allowing runtime overrides
    const mergedConfig = {
      ...this.sharedServiceConfig,
      ...coinConfig,  // Coin-specific overrides (excludes 'enabled' flag)
      ...options.config,
    };
    // Remove 'enabled' from merged config as it's not a DipArbServiceConfig field
    delete (mergedConfig as any).enabled;
    // Also update manager's debug flag if set in config
    if (mergedConfig.debug !== undefined) {
      this.debug = mergedConfig.debug;
    }
    service.updateConfig(mergedConfig);

    // Setup event forwarding with market context
    this.setupEventForwarding(service, coin);

    // Find and start the market
    console.log(`[DipArbManager] Calling service.findAndStart for ${coin}...`);
    const findStartTime = Date.now();
    const market = await service.findAndStart({
      coin,
      preferDuration: options.preferDuration ?? '15m',
    });
    console.log(
      `[DipArbManager] service.findAndStart completed for ${coin} in ${Date.now() - findStartTime}ms, market=${market ? market.name : 'null'}`
    );

    if (!market) {
      console.log(`[DipArbManager] No market found for ${coin}`);
      this.log(`No market found for ${coin}`);
      return null;
    }

    // Track the active market
    this.activeMarkets.set(coin, {
      coin,
      conditionId: market.conditionId,
      market,
      service,
      clobService: coinClobService,
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

    // Stop the DipArbService
    await active.service.stop();

    // Disconnect the CLOB WebSocket for this coin
    active.clobService.disconnect();

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
   * Update coin-specific configurations
   *
   * Allows tuning parameters per asset based on market dynamics:
   * - BTC: May need stricter maxRequiredLeg2Drop due to efficient markets
   * - XRP: May allow looser sumTarget due to higher volatility
   *
   * Example:
   * ```typescript
   * manager.updateCoinConfigs({
   *   BTC: { enabled: false }, // Disable BTC trading
   *   XRP: { sumTarget: 0.90 }, // Looser target for XRP
   * });
   * ```
   */
  updateCoinConfigs(configs: Partial<Record<DipArbUnderlying, CoinSpecificConfig>>): void {
    // Merge with existing configs (deep merge per coin)
    for (const [coin, config] of Object.entries(configs) as [DipArbUnderlying, CoinSpecificConfig][]) {
      this.coinConfigs[coin] = {
        ...this.coinConfigs[coin],
        ...config,
      };
    }
    this.log(`Coin configs updated: ${JSON.stringify(configs)}`);
  }

  /**
   * Get current coin-specific configurations
   */
  getCoinConfigs(): Partial<Record<DipArbUnderlying, CoinSpecificConfig>> {
    return { ...this.coinConfigs };
  }

  /**
   * Check if a coin is enabled
   */
  isCoinEnabled(coin: DipArbUnderlying): boolean {
    return this.coinConfigs[coin]?.enabled !== false;
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
      // Explicit field mapping to ensure no data is lost
      const activeMarket = this.activeMarkets.get(coin);
      const paperTradeEvent = {
        type: trade.type,
        side: trade.side,
        shares: trade.shares,
        price: trade.price,
        cost: trade.cost,
        totalCost: trade.totalCost,
        profit: trade.profit,
        profitRate: trade.profitRate,
        expectedPrice: trade.expectedPrice,
        slippagePercent: trade.slippagePercent,
        marketName: trade.marketName ?? activeMarket?.market.name,
        conditionId: trade.conditionId ?? activeMarket?.conditionId,
        timestamp: trade.timestamp ?? Date.now(),
        coin,
        marketConditionId: activeMarket?.conditionId,
        roundId: trade.roundId,  // For round grouping in execution log
      };
      this.log(`[${coin}] Forwarding paperTrade: type=${paperTradeEvent.type}, side=${paperTradeEvent.side}, shares=${paperTradeEvent.shares}, price=${paperTradeEvent.price}`);
      this.emit('paperTrade', paperTradeEvent);
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

    // ===== Settlement Awareness Event Forwarding =====

    // Forward settlement decision events (legacy - for backward compatibility)
    service.on('settlementDecision', (decision) => {
      this.log(`[${coin}] Forwarding settlementDecision: ${decision.decision} - ${decision.reason}`);
      this.emit('settlementDecision', {
        ...decision,
        coin,
        marketConditionId: this.activeMarkets.get(coin)?.conditionId,
        marketName: this.activeMarkets.get(coin)?.market.name,
      });
    });

    // Forward enhanced settlement decision events (for observability & auto-tuning)
    service.on('enhancedSettlementDecision', (decision) => {
      this.log(`[${coin}] Forwarding enhancedSettlementDecision: ${decision.decision} - ${decision.reason}`);
      this.emit('enhancedSettlementDecision', {
        ...decision,
        coin,
        marketConditionId: this.activeMarkets.get(coin)?.conditionId,
        marketName: this.activeMarkets.get(coin)?.market.name,
      });
    });

    // Forward settlement outcome events (after market settles)
    service.on('settlementOutcome', (outcome) => {
      const profitStr = outcome.actualProfit >= 0 ? `+$${outcome.actualProfit.toFixed(2)}` : `-$${Math.abs(outcome.actualProfit).toFixed(2)}`;
      this.log(`[${coin}] Forwarding settlementOutcome: ${outcome.outcome} (${outcome.decision}) ${profitStr}`);
      this.emit('settlementOutcome', {
        ...outcome,
        coin,
        marketConditionId: this.activeMarkets.get(coin)?.conditionId,
        marketName: this.activeMarkets.get(coin)?.market.name,
      });
    });
  }
}

export default DipArbManager;
