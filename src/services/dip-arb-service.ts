/**
 * DipArbService - Dip Arbitrage Service
 *
 * 暴跌套利服务 - 针对 Polymarket 15分钟/5分钟 UP/DOWN 市场
 *
 * 策略原理：
 * 1. 每个市场有一个 "price to beat" = Chainlink价格在窗口开始时的快照
 *    - 固定值，在市场开始时确定，存储在 groupItemThreshold 字段
 * 2. "current price" = 实时 Chainlink 价格 (via crypto_prices_chainlink WebSocket)
 *    - 用于显示资产当前走势
 * 3. 结算规则：
 *    - UP 赢：窗口结束时的 Chainlink 价格 >= price to beat
 *    - DOWN 赢：窗口结束时的 Chainlink 价格 < price to beat
 *
 * 4. 套利流程：
 *    - Leg1：检测暴跌 → 买入暴跌侧
 *    - Leg2：等待对冲条件 → 买入另一侧
 *    - 利润：总成本 < $1 时获得无风险利润
 *
 * 使用示例：
 * ```typescript
 * const sdk = await PolymarketSDK.create({ privateKey: '...' });
 *
 * // 自动找到并启动
 * await sdk.dipArb.findAndStart({ coin: 'BTC' });
 *
 * // 监听信号
 * sdk.dipArb.on('signal', (signal) => {
 *   console.log(`Signal: ${signal.type} ${signal.side}`);
 * });
 * ```
 */

import { EventEmitter } from 'events';
import {
  RealtimeServiceV2,
  type MarketSubscription,
  type OrderbookSnapshot,
  type Subscription,
  type CryptoPrice,
} from './realtime-service-v2.js';
import { TradingService, type MarketOrderParams } from './trading-service.js';
import { MarketService } from './market-service.js';
import { CTFClient } from '../clients/ctf-client.js';
import { BinanceService, type BinanceSymbol } from './binance-service.js';
import type { Side } from '../core/types.js';
import {
  type DipArbServiceConfig,
  type DipArbConfigInternal,
  type DipArbMarketConfig,
  type DipArbRoundState,
  type DipArbStats,
  type DipArbSignal,
  type DipArbLeg1Signal,
  type DipArbLeg2Signal,
  type DipArbExecutionResult,
  type DipArbRoundResult,
  type DipArbNewRoundEvent,
  type DipArbPriceUpdateEvent,
  type DipArbScanOptions,
  type DipArbFindAndStartOptions,
  type DipArbSide,
  type DipArbAutoRotateConfig,
  type DipArbSettleResult,
  type DipArbRotateEvent,
  type DipArbUnderlying,
  type DipArbDuration,
  type DipArbDurationString,
  type DipArbPendingRedemption,
  // Settlement awareness types
  type EnhancedSettlementDecision,
  type SettlementOutcome,
  type SettlementRuleEvaluations,
  type SettlementEntryContext,
  type SettlementMarketSnapshot,
  DEFAULT_DIP_ARB_CONFIG,
  DEFAULT_AUTO_ROTATE_CONFIG,
  DIP_ARB_CRYPTO_TAKER_FEE,
  DURATION_PRIORITY,
  DURATION_FALLBACK_CHAIN,
  DURATION_MINUTES,
  createDipArbInitialStats,
  createDipArbRoundState,
  calculateDipArbProfitRate,
  calculateDipArbNetProfitRate,
  estimateUpWinRate,
  detectMispricing,
  parseUnderlyingFromSlug,
  parseDurationFromSlug,
  isDipArbLeg1Signal,
} from './dip-arb-types.js';

// ===== DipArbService =====

/**
 * Maps DipArb underlying assets to Binance trading pair symbols
 */
const UNDERLYING_TO_BINANCE: Record<DipArbUnderlying, BinanceSymbol | null> = {
  'BTC': 'BTCUSDT',
  'ETH': 'ETHUSDT',
  'SOL': 'SOLUSDT',
  'XRP': 'XRPUSDT',
};

/**
 * Calculate realistic slippage for paper trading simulation
 *
 * Real market slippage is typically 0.5-2%+, not the previous 0-0.5% uniform distribution.
 * Factors affecting slippage:
 * - Base market conditions: 0.3-0.7%
 * - Order size impact: ~0.1% per $100 order value
 * - Capped at 3% to avoid unrealistic extreme values
 *
 * @param shares - Number of shares in the order
 * @param price - Price per share
 * @returns Slippage as a decimal (e.g., 0.01 = 1%)
 */
function calculateRealisticSlippage(shares: number, price: number): number {
  // Base slippage: 0.3-0.7% (market conditions, bid-ask spread)
  const baseSlippage = 0.003 + Math.random() * 0.004;

  // Size impact: ~0.1% per $100 order value (market depth impact)
  const orderValue = shares * price;
  const sizeImpact = (orderValue / 100) * 0.001;

  // Total slippage, capped at 3% to avoid unrealistic extreme values
  return Math.min(baseSlippage + sizeImpact, 0.03);
}

/**
 * Calculate dynamic timeout loss based on elapsed time
 *
 * Timeout losses should vary based on how long the position was open:
 * - Early timeout (< 30% of timeout period): Lower loss (2-5%)
 * - Mid timeout (30-70% of timeout period): Medium loss (5-10%)
 * - Late timeout (> 70% of timeout period): Higher loss (10-15%)
 *
 * This replaces the previous hardcoded 7.5% loss estimate.
 *
 * @param elapsedSeconds - Time since leg1 was filled
 * @param timeoutSeconds - Total timeout period
 * @returns Estimated loss rate as a decimal (e.g., 0.075 = 7.5%)
 */
function calculateTimeoutLossRate(elapsedSeconds: number, timeoutSeconds: number): number {
  const elapsedRatio = elapsedSeconds / timeoutSeconds;

  // Base loss rate varies by how long we waited
  let baseLoss: number;
  if (elapsedRatio < 0.3) {
    // Early exit: market probably moved against us quickly (2-5%)
    baseLoss = 0.02 + Math.random() * 0.03;
  } else if (elapsedRatio < 0.7) {
    // Mid exit: moderate adverse movement (5-10%)
    baseLoss = 0.05 + Math.random() * 0.05;
  } else {
    // Late exit: significant adverse movement (10-15%)
    baseLoss = 0.10 + Math.random() * 0.05;
  }

  // Add some randomness for market volatility
  const volatilityFactor = 0.9 + Math.random() * 0.2; // ±10%

  return Math.min(baseLoss * volatilityFactor, 0.20); // Cap at 20%
}

export class DipArbService extends EventEmitter {
  // Dependencies
  private realtimeService: RealtimeServiceV2;
  private tradingService: TradingService | null = null;
  private marketService: MarketService;
  private ctf: CTFClient | null = null;
  private binanceService: BinanceService | null = null;

  // Configuration
  private config: DipArbConfigInternal;
  private autoRotateConfig: Required<DipArbAutoRotateConfig>;

  // State
  private market: DipArbMarketConfig | null = null;
  private currentRound: DipArbRoundState | null = null;
  private isRunning = false;
  private isExecuting = false;
  private lastExecutionTime = 0;
  private stats: DipArbStats;
  private openPositionCount = 0;  // Track open (unhedged) positions for maxOpenPositions check

  // Subscriptions
  private marketSubscription: MarketSubscription | null = null;
  private priceSubscription: Subscription | null = null;

  // Auto-rotate state
  private rotateCheckInterval: ReturnType<typeof setInterval> | null = null;
  private nextMarket: DipArbMarketConfig | null = null;
  private lastRotatedCoin: DipArbUnderlying | null = null;

  // Duration fallback state
  private currentDuration: DipArbDurationString = '15m';
  private preferredDuration: DipArbDurationString = '15m';
  private isSearchingMarket = false;
  private searchStartTime: number | null = null;  // For health monitoring timeout detection
  private upgradeCheckInterval: ReturnType<typeof setInterval> | null = null;

  // Pending redemption state (for background redemption after market resolution)
  private pendingRedemptions: DipArbPendingRedemption[] = [];
  private redeemCheckInterval: ReturnType<typeof setInterval> | null = null;

  // Orderbook state
  private upAsks: Array<{ price: number; size: number }> = [];
  private downAsks: Array<{ price: number; size: number }> = [];

  // Price history for sliding window detection
  // Each entry: { timestamp: number, upAsk: number, downAsk: number }
  private priceHistory: Array<{ timestamp: number; upAsk: number; downAsk: number }> = [];
  private readonly MAX_HISTORY_LENGTH = 100;  // Keep last 100 price points

  // Price state
  private currentUnderlyingPrice = 0;

  // Signal state - prevent duplicate signals within same round
  private leg1SignalEmitted = false;

  // Smart logging state - reduce orderbook noise
  private lastOrderbookLogTime = 0;
  private readonly ORDERBOOK_LOG_INTERVAL_MS = 10000;  // Log orderbook every 10 seconds
  private orderbookBuffer: Array<{ timestamp: number; upAsk: number; downAsk: number; upDepth: number; downDepth: number }> = [];
  private readonly ORDERBOOK_BUFFER_SIZE = 50;  // Keep 5 seconds of data at ~10 updates/sec
  
  // Orderbook emission throttling
  private lastOrderbookEmitTime = 0;

  // FIX #5: Warm-up period for sliding window
  // Skip detection until price history has enough data for sliding window comparison
  private isWarmedUp = false;
  private warmupStartTime = 0;

  constructor(
    realtimeService: RealtimeServiceV2,
    tradingService: TradingService | null,
    marketService: MarketService,
    privateKey?: string,
    chainId: number = 137,
    binanceService?: BinanceService
  ) {
    super();

    this.realtimeService = realtimeService;
    this.tradingService = tradingService;
    this.marketService = marketService;
    this.binanceService = binanceService ?? null;

    // Initialize with default config
    this.config = { ...DEFAULT_DIP_ARB_CONFIG };
    this.autoRotateConfig = { ...DEFAULT_AUTO_ROTATE_CONFIG };
    this.stats = createDipArbInitialStats();

    // Initialize CTF if private key provided
    if (privateKey) {
      this.ctf = new CTFClient({
        privateKey,
        rpcUrl: 'https://polygon-rpc.com',
        chainId,
      });
    }
  }

  /**
   * Set Binance service for momentum detection
   * Can be called after construction to enable momentum validation
   */
  setBinanceService(binanceService: BinanceService): void {
    this.binanceService = binanceService;
  }

  // ===== Public API: Configuration =====

  /**
   * Update configuration
   */
  updateConfig(config: Partial<DipArbServiceConfig>): void {
    this.config = {
      ...this.config,
      ...config,
    };
    this.log(`Config updated: ${JSON.stringify(config)}`);
  }

  /**
   * Get current configuration
   */
  getConfig(): DipArbConfigInternal {
    return { ...this.config };
  }

  // ===== Public API: Market Discovery =====

  /**
   * Scan for upcoming UP/DOWN markets
   *
   * Supports all durations: 5m, 15m, 1h, 4h, daily
   * Uses MarketService.scanCryptoMarkets() for unified scanning
   */
  async scanUpcomingMarkets(options: DipArbScanOptions = {}): Promise<DipArbMarketConfig[]> {
    const {
      coin = 'all',
      duration = 'all',
      minMinutesUntilEnd = 5,
      maxMinutesUntilEnd = 60,
      limit = 20,
    } = options;

    console.log(`[DipArbService] scanUpcomingMarkets: coin=${coin}, duration=${duration}`);

    try {
      // For 'all' duration, try short-term markets first (legacy behavior)
      // For specific durations, use the new unified scanner
      let gammaMarkets;

      if (duration === 'all' || duration === '5m' || duration === '15m') {
        // Use existing short-term scanner for backwards compatibility
        console.log(`[DipArbService] Calling marketService.scanCryptoShortTermMarkets...`);
        const apiStartTime = Date.now();
        gammaMarkets = await this.marketService.scanCryptoShortTermMarkets({
          coin: coin as 'BTC' | 'ETH' | 'SOL' | 'XRP' | 'all',
          duration: duration === 'all' ? 'all' : duration as '5m' | '15m',
          minMinutesUntilEnd,
          maxMinutesUntilEnd,
          limit,
          sortBy: 'endDate',
        });
        console.log(`[DipArbService] scanCryptoShortTermMarkets completed in ${Date.now() - apiStartTime}ms, found ${gammaMarkets.length} markets`);
      } else {
        // Use new unified scanner for longer durations (1h, 4h, daily)
        console.log(`[DipArbService] Calling marketService.scanCryptoMarkets...`);
        const apiStartTime = Date.now();
        gammaMarkets = await this.marketService.scanCryptoMarkets({
          coin: coin as 'BTC' | 'ETH' | 'SOL' | 'XRP' | 'all',
          duration: duration as '1h' | '4h' | 'daily',
          minMinutesUntilEnd,
          maxMinutesUntilEnd,
          limit,
          sortBy: 'endDate',
        });
        console.log(`[DipArbService] scanCryptoMarkets completed in ${Date.now() - apiStartTime}ms, found ${gammaMarkets.length} markets`);
      }

      this.log(`scanUpcomingMarkets: marketService returned ${gammaMarkets.length} market(s) for duration=${duration}`);

      // Get full market info with token IDs for each market
      const results: DipArbMarketConfig[] = [];

      for (const gm of gammaMarkets) {
        // Retry up to 3 times for network errors
        let retries = 3;
        while (retries > 0) {
          try {
            // Get full market info from CLOB API via MarketService
            const market = await this.marketService.getMarket(gm.conditionId);

            // Find UP and DOWN tokens
            const upToken = market.tokens.find(t =>
              t.outcome.toLowerCase() === 'up' || t.outcome.toLowerCase() === 'yes'
            );
            const downToken = market.tokens.find(t =>
              t.outcome.toLowerCase() === 'down' || t.outcome.toLowerCase() === 'no'
            );

            if (upToken?.tokenId && downToken?.tokenId) {
              // Note: priceToBeat is fetched dynamically via /prices-history API in start()
              // It's the Chainlink price at window start timestamp
              results.push({
                name: gm.question,
                slug: gm.slug,
                conditionId: gm.conditionId,
                upTokenId: upToken.tokenId,
                downTokenId: downToken.tokenId,
                underlying: parseUnderlyingFromSlug(gm.slug),
                durationMinutes: parseDurationFromSlug(gm.slug),
                endTime: gm.endDate,
                // priceToBeat is set in start() via fetchPriceToBeat()
              });
            }
            break; // Success, exit retry loop
          } catch (error) {
            retries--;
            if (retries > 0) {
              // Wait 1 second before retry
              await new Promise(r => setTimeout(r, 1000));
            }
          }
        }
      }

      return results;
    } catch (error) {
      this.emit('error', error instanceof Error ? error : new Error(String(error)));
      return [];
    }
  }

  /**
   * Find the best market and start monitoring
   *
   * Implements duration fallback: 15m → 1h → 4h → daily
   * When preferred duration unavailable, falls back to longer durations
   */
  async findAndStart(options: DipArbFindAndStartOptions = {}): Promise<DipArbMarketConfig | null> {
    const { coin, preferDuration = '15m' } = options;
    console.log(`[DipArbService] findAndStart called for coin=${coin}, preferDuration=${preferDuration}`);

    // Store preferred duration for upgrade checking
    this.preferredDuration = preferDuration;

    // Get duration priority chain (use config or default)
    const durationPriority = this.autoRotateConfig.durationPriority || DURATION_FALLBACK_CHAIN;
    const enableFallback = this.autoRotateConfig.enableFallback ?? true;

    // Find the starting index in the priority chain
    const startIndex = durationPriority.indexOf(preferDuration);
    const durationsToTry = startIndex >= 0
      ? durationPriority.slice(startIndex)
      : durationPriority;

    // Track search start time for health monitoring
    this.searchStartTime = Date.now();
    let foundMarket: DipArbMarketConfig | null = null;

    try {
      // Emit searching event
      this.isSearchingMarket = true;
      this.emit('searching', { duration: preferDuration, message: `Searching for ${preferDuration} markets...`, coin });

      // Try each duration in priority order
      for (const duration of durationsToTry) {
        // Skip if fallback disabled and not preferred duration
        if (!enableFallback && duration !== preferDuration) {
          continue;
        }

        const scanOptions: DipArbScanOptions = {
          coin: coin || 'all',
          duration,
          minMinutesUntilEnd: 3,
          maxMinutesUntilEnd: this.getMaxMinutesForDuration(duration),
          limit: 10,
        };

        this.log(`Scanning for ${duration} markets: coin=${scanOptions.coin}, window=${scanOptions.minMinutesUntilEnd}-${scanOptions.maxMinutesUntilEnd}min`);
        console.log(`[DipArbService] Scanning for ${duration} markets...`);

        // Emit searching event for current duration
        if (duration !== preferDuration) {
          this.emit('searching', { duration, message: `Trying ${duration} markets (${preferDuration} unavailable)...`, isFallback: true, coin });
        }

        console.log(`[DipArbService] Calling scanUpcomingMarkets...`);
        const scanStartTime = Date.now();
        const markets = await this.scanUpcomingMarkets(scanOptions);
        console.log(`[DipArbService] scanUpcomingMarkets returned ${markets.length} markets in ${Date.now() - scanStartTime}ms`);

        if (markets.length > 0) {
          this.log(`Found ${markets.length} ${duration} market(s): ${markets.map(m => `${m.underlying} (ends ${Math.round((m.endTime.getTime() - Date.now()) / 60000)}min)`).join(', ')}`);

          // Find the best market (prefer specified coin, then by time)
          let bestMarket = markets[0];
          if (coin) {
            const coinMarket = markets.find(m => m.underlying === coin);
            if (coinMarket) {
              bestMarket = coinMarket;
            }
          }

          // Track current duration for upgrade checking
          this.currentDuration = duration;

          // Log if we're falling back to a lower priority duration
          if (duration !== preferDuration) {
            this.log(`⚠️ Falling back to ${duration} market (${preferDuration} unavailable)`);
            // Start checking for higher priority markets
            this.startUpgradeCheck();
          } else {
            // At preferred duration, stop any upgrade checks
            this.stopUpgradeCheck();
          }

          this.emit('marketFound', {
            market: bestMarket,
            duration: this.currentDuration,
            isFallback: duration !== preferDuration,
            coin,
          });

          await this.start(bestMarket);
          foundMarket = bestMarket;
          return foundMarket;
        }

        this.log(`No ${duration} markets found, trying next duration...`);
      }

      // No markets found at any duration
      this.log('No suitable markets found at any duration');
      this.emit('noMarketsAvailable', {
        triedDurations: durationsToTry,
        willRetry: true,
        retryIntervalMs: 30000,
        coin,
      });

      return null;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      this.log(`❌ findAndStart failed: ${errorMessage}`);
      this.emit('error', { error, phase: 'search', coin });
      return null;
    } finally {
      // ALWAYS clear searching state - this prevents stuck "Finding round" states
      this.isSearchingMarket = false;
      this.searchStartTime = null;
      this.emit('searchComplete', { success: !!foundMarket, coin });
    }
  }

  /**
   * Get appropriate max minutes to look ahead for each duration
   */
  private getMaxMinutesForDuration(duration: DipArbDurationString): number {
    switch (duration) {
      case '5m': return 30;
      case '15m': return 120;
      case '1h': return 180;
      case '4h': return 480;
      case 'daily': return 1440;
      default: return 120;
    }
  }

  /**
   * Start checking for higher-priority markets (when in fallback mode)
   */
  private startUpgradeCheck(): void {
    if (this.upgradeCheckInterval) {
      return; // Already running
    }

    const intervalMs = this.autoRotateConfig.upgradeCheckIntervalMs || 60000;

    this.log(`🔍 Starting upgrade check (every ${intervalMs / 1000}s) - looking for ${this.preferredDuration} markets`);

    this.upgradeCheckInterval = setInterval(() => {
      this.checkForUpgrade();
    }, intervalMs);
  }

  /**
   * Stop upgrade checking
   */
  private stopUpgradeCheck(): void {
    if (this.upgradeCheckInterval) {
      clearInterval(this.upgradeCheckInterval);
      this.upgradeCheckInterval = null;
      this.log('Stopped upgrade check');
    }
  }

  /**
   * Fetch the Price to Beat from Polymarket's crypto-price API
   *
   * Price to Beat = Chainlink price at window START timestamp
   * API: GET https://polymarket.com/api/crypto/crypto-price
   *      ?symbol={BTC|ETH|SOL|XRP}
   *      &eventStartTime={windowStart ISO}
   *      &variant={fifteen|hourly}
   *      &endDate={windowEnd ISO}
   *
   * Response: { openPrice: number, closePrice: number|null, ... }
   *
   * @param market - The market config
   * @returns The price to beat (openPrice), or 0 if unavailable
   */
  private async fetchPriceToBeat(market: DipArbMarketConfig): Promise<number> {
    try {
      // Calculate window times
      const endTimeMs = market.endTime instanceof Date
        ? market.endTime.getTime()
        : (typeof market.endTime === 'string'
          ? new Date(market.endTime).getTime()
          : market.endTime);

      const windowStartMs = endTimeMs - (market.durationMinutes * 60 * 1000);
      const eventStartTime = new Date(windowStartMs).toISOString();
      const endDate = new Date(endTimeMs).toISOString();

      // Determine variant based on duration
      const variant = market.durationMinutes <= 15 ? 'fifteen' : 'hourly';

      console.log(`[DipArb] Fetching Price to Beat for ${market.underlying}...`);
      console.log(`[DipArb]   Window: ${eventStartTime} to ${endDate} (${variant})`);

      // Build URL for Polymarket's crypto-price API
      const url = new URL('https://polymarket.com/api/crypto/crypto-price');
      url.searchParams.set('symbol', market.underlying);
      url.searchParams.set('eventStartTime', eventStartTime);
      url.searchParams.set('variant', variant);
      url.searchParams.set('endDate', endDate);

      // Retry logic for rate limiting
      let lastError: Error | null = null;
      for (let attempt = 1; attempt <= 3; attempt++) {
        try {
          const response = await fetch(url.toString());

          if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
          }

          const data = await response.json() as {
            openPrice?: number | null;
            closePrice?: number | null;
            timestamp?: number;
            completed?: boolean;
            incomplete?: boolean;
            cached?: boolean;
            error?: string;
          };

          // Check for error in response body (API returns 200 but with error field)
          if (data.error) {
            if (data.error.includes('429') && attempt < 3) {
              console.log(`[DipArb] ⚠️ Rate limited, retrying in ${attempt}s... (attempt ${attempt}/3)`);
              await new Promise(r => setTimeout(r, attempt * 1000));
              continue;
            }
            throw new Error(data.error);
          }

          if (data.openPrice && data.openPrice > 0) {
            console.log(`[DipArb] ✅ Price to Beat: $${data.openPrice.toLocaleString()} (from Polymarket API)`);
            this.log(`✅ Price to Beat: $${data.openPrice.toLocaleString()} (Chainlink @ window start)`);
            return data.openPrice;
          } else {
            console.log(`[DipArb] ⚠️ openPrice not available yet (market may not have started)`);
            this.log(`⚠️ Price to Beat not available - openPrice is ${data.openPrice}`);
            return 0;
          }
        } catch (err) {
          lastError = err instanceof Error ? err : new Error(String(err));
          if (attempt < 3) {
            console.log(`[DipArb] ⚠️ Fetch attempt ${attempt} failed: ${lastError.message}, retrying...`);
            await new Promise(r => setTimeout(r, attempt * 1000));
          }
        }
      }

      throw lastError || new Error('Failed after 3 attempts');
    } catch (error) {
      console.log(`[DipArb] ❌ Failed to fetch Price to Beat: ${error instanceof Error ? error.message : String(error)}`);
      this.log(`❌ Price to Beat fetch failed: ${error instanceof Error ? error.message : String(error)}`);
      return 0;
    }
  }

  /**
   * Check if a higher-priority market is available
   */
  private async checkForUpgrade(): Promise<void> {
    if (!this.isRunning || !this.market) return;

    // Check if we're already at preferred duration
    if (this.currentDuration === this.preferredDuration) {
      this.stopUpgradeCheck();
      return;
    }

    // Get durations with higher priority than current
    const currentPriority = DURATION_PRIORITY[this.currentDuration];
    const durationPriority = this.autoRotateConfig.durationPriority || DURATION_FALLBACK_CHAIN;

    for (const duration of durationPriority) {
      const priority = DURATION_PRIORITY[duration];
      if (priority >= currentPriority) break; // No point checking lower priority

      const markets = await this.scanUpcomingMarkets({
        coin: this.market.underlying,
        duration: duration,
        minMinutesUntilEnd: 3,
        maxMinutesUntilEnd: this.getMaxMinutesForDuration(duration),
        limit: 1,
      });

      if (markets.length > 0) {
        const betterMarket = markets[0];
        this.log(`🎯 Found higher-priority ${duration} market: ${betterMarket.name}`);

        if (this.autoRotateConfig.immediateUpgrade) {
          // Immediately switch
          this.log('Immediately upgrading to higher-priority market...');
          this.nextMarket = betterMarket;
          // Trigger rotation (will settle current and switch)
          await this.forceRotateToNextMarket();
        } else {
          // Queue for next rotation
          this.nextMarket = betterMarket;
          this.log(`Queued ${duration} market for upgrade after current market ends`);
        }

        this.stopUpgradeCheck();
        return;
      }
    }
  }

  /**
   * Force immediate rotation to the next market (used for immediate upgrade)
   * This settles current position if needed and switches to nextMarket
   */
  private async forceRotateToNextMarket(): Promise<void> {
    if (!this.nextMarket) {
      this.log('No next market queued for immediate rotation');
      return;
    }

    const previousMarket = this.market;
    const newMarket = this.nextMarket;
    this.nextMarket = null;

    // Settle if configured and has position
    if (this.autoRotateConfig.autoSettle && this.currentRound?.leg1 && this.market) {
      const strategy = this.autoRotateConfig.settleStrategy || 'redeem';
      if (strategy === 'redeem') {
        this.addPendingRedemption(this.market, this.currentRound);
        this.log(`Position added to pending redemption queue`);
      } else {
        const settleResult = await this.settle('sell');
        this.emit('settled', settleResult);
      }
    }

    // Stop current market
    await this.stop();

    // Start new market
    await this.start(newMarket);

    // Restart the rotate check interval for the new market
    this.startRotateCheck();

    const event: DipArbRotateEvent = {
      previousMarket: previousMarket?.conditionId,
      newMarket: newMarket.conditionId,
      reason: 'upgrade',
      timestamp: Date.now(),
    };
    this.emit('rotate', event);
  }

  // ===== Public API: Lifecycle =====

  /**
   * Start monitoring a market
   */
  async start(market: DipArbMarketConfig): Promise<void> {
    if (this.isRunning) {
      throw new Error('DipArbService is already running. Call stop() first.');
    }

    // Validate token IDs
    if (!market.upTokenId || !market.downTokenId) {
      throw new Error(`Invalid market config: missing token IDs. upTokenId=${market.upTokenId}, downTokenId=${market.downTokenId}`);
    }

    this.market = market;
    this.isRunning = true;
    this.stats = createDipArbInitialStats();
    this.priceHistory = [];  // Clear price history for new market

    // FIX #5: Initialize warm-up period - skip detection until slidingWindowMs has elapsed
    this.isWarmedUp = false;
    this.warmupStartTime = Date.now();

    this.log(`Starting Dip Arb monitor for: ${market.name}`);
    this.log(`Condition ID: ${market.conditionId.slice(0, 20)}...`);
    this.log(`Underlying: ${market.underlying}`);
    this.log(`Duration: ${market.durationMinutes}m`);
    this.log(`Auto Execute: ${this.config.autoExecute ? 'YES' : 'NO'}`);

    // Initialize trading service if available
    if (this.tradingService) {
      try {
        await this.tradingService.initialize();
        this.log(`Wallet: ${this.ctf?.getAddress()}`);
      } catch (error) {
        this.log(`Warning: Trading service init failed: ${error}`);
      }
    } else {
      this.log('No wallet configured - monitoring only');
    }

    // Connect realtime service with retry logic
    console.log(`[DipArb] Config state: debug=${this.config.debug}, paperMode=${this.config.paperMode}`);
    console.log(`[DipArb] Connecting to WebSocket...`);
    this.realtimeService.connect();

    // Wait for WebSocket connection with retries
    const maxRetries = 3;
    let retryCount = 0;
    let connected = false;

    while (retryCount < maxRetries && !connected) {
      await new Promise<void>((resolve) => {
        const timeout = setTimeout(() => {
          this.log(`Warning: WebSocket connection timeout (attempt ${retryCount + 1}/${maxRetries})`);
          resolve();
        }, 15000); // Increased from 10s to 15s

        // Check if already connected
        if (this.realtimeService.isConnected?.()) {
          clearTimeout(timeout);
          connected = true;
          this.log('✅ WebSocket connected successfully');
          resolve();
          return;
        }

        this.realtimeService.once('connected', () => {
          clearTimeout(timeout);
          connected = true;
          this.log('✅ WebSocket connected successfully');
          resolve();
        });
      });

      if (!connected && retryCount < maxRetries - 1) {
        this.log(`Retrying WebSocket connection in ${2 ** retryCount} seconds...`);
        await new Promise(r => setTimeout(r, 1000 * (2 ** retryCount))); // Exponential backoff: 1s, 2s, 4s
        this.realtimeService.disconnect();
        this.realtimeService.connect();
        retryCount++;
      } else if (!connected) {
        retryCount++;
      }
    }

    // If still not connected, log warning but continue
    // (Polling fallback disabled - WebSocket retry should be sufficient)
    if (!connected) {
      console.log('[DipArb] ⚠️ WebSocket failed after all retries');
      this.log('⚠️ WebSocket failed after all retries. Bot will continue but may not receive orderbook updates.');
      this.log('💡 Check network connectivity and firewall settings.');
    } else {
      console.log('[DipArb] ✅ WebSocket connected');
    }

    // Subscribe to market orderbook
    console.log('[DipArb] Subscribing to orderbook...');
    this.log(`Subscribing to tokens: UP=${market.upTokenId.slice(0, 20)}..., DOWN=${market.downTokenId.slice(0, 20)}...`);
    this.marketSubscription = this.realtimeService.subscribeMarkets(
      [market.upTokenId, market.downTokenId],
      {
        onOrderbook: (book: OrderbookSnapshot) => {
          // Handle the orderbook update (always)
          this.handleOrderbookUpdate(book);

          // Smart logging: only log at intervals, not every update
          if (this.config.debug) {
            this.updateOrderbookBuffer(book);
            this.maybeLogOrderbookSummary();
          }
        },
        onError: (error: Error) => this.emit('error', error),
      }
    );

    // Subscribe to Chainlink price feed for the underlying asset
    // Symbol format must be lowercase: eth/usd, btc/usd, sol/usd, xrp/usd
    const chainlinkSymbol = `${market.underlying.toLowerCase()}/usd`;
    console.log(`[DipArb] Subscribing to Chainlink price feed: ${chainlinkSymbol}`);
    this.log(`📡 Subscribing to Chainlink oracle: ${chainlinkSymbol}`);

    this.priceSubscription = this.realtimeService.subscribeCryptoChainlinkPrices(
      [chainlinkSymbol],
      {
        onPrice: (price) => this.handleUnderlyingPriceUpdate(price),
      }
    );
    console.log(`[DipArb] 📡 Subscribed to live Chainlink feed: ${chainlinkSymbol}`);
    this.log(`📡 Live price tracking via Chainlink: ${chainlinkSymbol}`);

    // Fetch Price to Beat from historical prices API
    // Price to Beat = Chainlink price at window START timestamp (fixed target)
    const priceToBeat = await this.fetchPriceToBeat(market);
    if (priceToBeat > 0) {
      market.priceToBeat = priceToBeat;
      this.market = market; // Update stored market with priceToBeat
    } else {
      console.log(`[DipArb] ⚠️ Could not determine Price to Beat - market resolution may be inaccurate`);
      this.log(`⚠️ WARNING: Price to Beat unavailable - will use first Chainlink price as fallback`);
    }

    // ✅ FIX: Check and merge existing pairs at startup
    if (this.ctf && this.config.autoMerge) {
      await this.scanAndMergeExistingPairs();
    }

    this.emit('started', market);
    console.log(`[DipArb] ✅ start() complete for ${market.underlying} - monitoring active`);
    this.log('Monitoring for dip arbitrage opportunities...');
  }

  /**
   * ✅ FIX: Scan and merge existing UP/DOWN pairs at startup
   *
   * When the service starts or rotates to a new market, check if there are
   * existing UP + DOWN token pairs from previous sessions and merge them.
   */
  private async scanAndMergeExistingPairs(): Promise<void> {
    if (!this.ctf || !this.market) return;
    
    // Skip in paper mode - no real tokens to merge
    if (this.config.paperMode) {
      this.log(`📝 [PAPER] Skipping startup merge scan (paper mode)`);
      return;
    }

    try {
      const tokenIds = {
        yesTokenId: this.market.upTokenId,
        noTokenId: this.market.downTokenId,
      };

      const balances = await this.ctf.getPositionBalanceByTokenIds(
        this.market.conditionId,
        tokenIds
      );

      const upBalance = parseFloat(balances.yesBalance);
      const downBalance = parseFloat(balances.noBalance);

      // Calculate how many pairs can be merged
      const pairsToMerge = Math.min(upBalance, downBalance);

      if (pairsToMerge > 0.01) {  // Minimum 0.01 to avoid dust
        this.log(`🔍 Found existing pairs: UP=${upBalance.toFixed(2)}, DOWN=${downBalance.toFixed(2)}`);
        this.log(`🔄 Auto-merging ${pairsToMerge.toFixed(2)} pairs at startup...`);

        try {
          const result = await this.ctf.mergeByTokenIds(
            this.market.conditionId,
            tokenIds,
            pairsToMerge.toString()
          );

          if (result.success) {
            this.log(`✅ Startup merge successful: ${pairsToMerge.toFixed(2)} pairs → $${result.usdcReceived || pairsToMerge.toFixed(2)} USDC.e`);
            this.log(`   TxHash: ${result.txHash?.slice(0, 20)}...`);
          } else {
            this.log(`❌ Startup merge failed`);
          }
        } catch (mergeError) {
          this.log(`❌ Startup merge error: ${mergeError instanceof Error ? mergeError.message : String(mergeError)}`);
        }
      } else if (upBalance > 0 || downBalance > 0) {
        // Has tokens but not enough pairs to merge
        this.log(`📊 Existing positions: UP=${upBalance.toFixed(2)}, DOWN=${downBalance.toFixed(2)} (no pairs to merge)`);
      }
    } catch (error) {
      this.log(`Warning: Failed to scan existing pairs: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * ✅ FIX: Restore an open position from persisted state
   * Used for recovery after bot restart
   *
   * Returns:
   *   'recovered' - Position restored successfully, monitoring for Leg2
   *   'expired' - Position's round has ended, mark as timed_out
   *   'timed_out' - Leg2 timeout exceeded, should be closed with loss
   *   'failed' - Error during recovery
   */
  async restoreOpenPosition(positionData: {
    roundId: string;
    marketConditionId: string;
    marketName: string;
    marketUpTokenId: string;
    marketDownTokenId: string;
    roundStartTime: number;
    roundEndTime: number;
    priceToBeat: number;
    leg1Side: 'UP' | 'DOWN';
    leg1Price: number;
    leg1Shares: number;
    leg1Timestamp: number;
    leg1TokenId: string;
  }): Promise<'recovered' | 'expired' | 'timed_out' | 'failed'> {
    try {
      const now = Date.now();
      const timeSinceLeg1 = (now - positionData.leg1Timestamp) / 1000;

      // Check if position's round has expired
      if (now > positionData.roundEndTime) {
        this.log(`⚠️ Skipping expired position: ${positionData.roundId}`);

        // For paper mode, emit the loss event so DB gets updated
        if (this.config.paperMode) {
          const estimatedLoss = positionData.leg1Price * positionData.leg1Shares * 0.5; // Assume ~50% loss on expiry
          this.emit('openPositionClosed', {
            roundId: positionData.roundId,
            status: 'timed_out',
            closeType: 'expired',
            loss: estimatedLoss,
          });
        }

        return 'expired';
      }

      // Check if Leg2 timeout has passed
      if (timeSinceLeg1 > this.getLeg2TimeoutSeconds()) {
        this.log(`⚠️ Position timeout exceeded: ${timeSinceLeg1.toFixed(0)}s > ${this.getLeg2TimeoutSeconds()}s`);

        // For paper mode, calculate and emit the loss (no real exit needed)
        if (this.config.paperMode) {
          // FIXED: Use dynamic timeout loss based on elapsed time instead of hardcoded 7.5%
          const timeoutSeconds = this.getLeg2TimeoutSeconds();
          const estimatedLossRate = calculateTimeoutLossRate(timeSinceLeg1, timeoutSeconds);
          const estimatedLoss = positionData.leg1Price * positionData.leg1Shares * estimatedLossRate;

          this.log(`📝 [PAPER] Recording timeout loss: ~$${estimatedLoss.toFixed(2)} (${(estimatedLossRate * 100).toFixed(1)}% of position)`);

          this.emit('paperTrade', {
            type: 'exit',
            side: positionData.leg1Side,
            shares: positionData.leg1Shares,
            price: positionData.leg1Price * (1 - estimatedLossRate),
            profit: -estimatedLoss,
            expectedPrice: positionData.leg1Price,   // Entry price as expected
            slippagePercent: estimatedLossRate,      // Dynamic timeout loss rate
            marketName: positionData.marketName,
            conditionId: positionData.marketConditionId,
            timestamp: Date.now(),
          });

          this.emit('openPositionClosed', {
            roundId: positionData.roundId,
            status: 'timed_out',
            closeType: 'timeout_recovery',
            loss: estimatedLoss,
          });
        }

        return 'timed_out';
      }

      // Position is valid - restore state
      // Extract underlying and duration from market name (e.g., "btc-updown-15m-...")
      const nameParts = positionData.marketName.toLowerCase().split('-');
      const underlying = (nameParts[0]?.toUpperCase() || 'BTC') as DipArbUnderlying;
      const durationStr = nameParts[2] || '15m';
      const durationMinutes = parseInt(durationStr) as DipArbDuration;

      this.market = {
        conditionId: positionData.marketConditionId,
        name: positionData.marketName,
        upTokenId: positionData.marketUpTokenId,
        downTokenId: positionData.marketDownTokenId,
        slug: positionData.marketName,
        underlying,
        durationMinutes,
        endTime: new Date(positionData.roundEndTime),
      };

      // Restore round state
      this.currentRound = {
        roundId: positionData.roundId,
        startTime: positionData.roundStartTime,
        endTime: positionData.roundEndTime,
        priceToBeat: positionData.priceToBeat,
        openPrices: { up: 0, down: 0 },  // Will be updated from orderbook
        phase: 'leg1_filled',
        leg1: {
          side: positionData.leg1Side,
          price: positionData.leg1Price,
          shares: positionData.leg1Shares,
          timestamp: positionData.leg1Timestamp,
          tokenId: positionData.leg1TokenId,
        },
      };

      this.log(`✅ Restored open position: ${positionData.leg1Side} x${positionData.leg1Shares} @ ${positionData.leg1Price.toFixed(4)}`);
      this.log(`   Round: ${positionData.roundId.slice(0, 30)}...`);
      this.log(`   Time since Leg1: ${timeSinceLeg1.toFixed(0)}s / ${this.getLeg2TimeoutSeconds()}s timeout`);

      // Resume monitoring for Leg2 signal
      return 'recovered';
    } catch (error) {
      this.log(`❌ Failed to restore position: ${error instanceof Error ? error.message : String(error)}`);
      return 'failed';
    }
  }

  /**
   * Stop monitoring
   * @param options.skipUnsubscribe - Skip unsubscribe for ended markets (used during rotation)
   * @param options.silent - Don't emit 'stopped' event (used during rotation to avoid "unexpected" warnings)
   */
  async stop(options?: { skipUnsubscribe?: boolean; silent?: boolean }): Promise<void> {
    if (!this.isRunning) return;

    this.isRunning = false;

    // Stop rotate check
    this.stopRotateCheck();

    // Stop HTTP polling fallback if active
    if (this.pollingInterval) {
      clearInterval(this.pollingInterval);
      this.pollingInterval = null;
      this.log('Stopped HTTP polling fallback');
    }

    // Unsubscribe (skip for ended markets during rotation to avoid "Invalid request body" errors)
    if (!options?.skipUnsubscribe) {
      if (this.marketSubscription) {
        try {
          this.marketSubscription.unsubscribe();
        } catch (err) {
          this.log(`Warning: Failed to unsubscribe from market: ${err}`);
        }
        this.marketSubscription = null;
      }

      if (this.priceSubscription) {
        try {
          this.priceSubscription.unsubscribe();
        } catch (err) {
          this.log(`Warning: Failed to unsubscribe from price feed: ${err}`);
        }
        this.priceSubscription = null;
      }
    } else {
      // Just clear references without sending unsubscribe messages
      this.log('Skipping unsubscribe (market ended)');
      this.marketSubscription = null;
      this.priceSubscription = null;
    }

    // NOTE: Do NOT disconnect WebSocket here!
    // In parallel mode, other DipArbService instances may still be using the connection.
    // Each service only unsubscribes from its own market's tokens.

    // Clear round state to prevent stale data during market rotation
    this.currentRound = null;
    this.leg1SignalEmitted = false;

    // Clean up pending settlement checks
    this.cleanupPendingSettlements();

    // Update stats
    this.stats.runningTimeMs = Date.now() - this.stats.startTime;

    this.log('Stopped');
    this.log(`Rounds monitored: ${this.stats.roundsMonitored}`);
    this.log(`Rounds completed: ${this.stats.roundsSuccessful}`);
    this.log(`Total profit: $${this.stats.totalProfit.toFixed(2)}`);

    // Don't emit 'stopped' during rotation to avoid "unexpected" warnings
    if (!options?.silent) {
      this.emit('stopped');
    }
  }

  /**
   * Check if service is running
   */
  isActive(): boolean {
    return this.isRunning;
  }

  /**
   * Get current market
   */
  getMarket(): DipArbMarketConfig | null {
    return this.market;
  }

  // ===== HTTP Polling Fallback =====
  // Note: Disabled for now due to type complexity with ProcessedOrderbook
  // WebSocket retry logic should be sufficient for most cases

  /**
   * Polling interval for HTTP fallback (currently unused)
   */
  private pollingInterval: NodeJS.Timeout | null = null;

  // ===== Public API: State Access =====

  /**
   * Get statistics
   */
  getStats(): DipArbStats {
    return {
      ...this.stats,
      runningTimeMs: this.isRunning ? Date.now() - this.stats.startTime : this.stats.runningTimeMs,
      currentRound: this.currentRound ? {
        roundId: this.currentRound.roundId,
        phase: this.currentRound.phase,
        priceToBeat: this.currentRound.priceToBeat,
        leg1: this.currentRound.leg1 ? {
          side: this.currentRound.leg1.side,
          price: this.currentRound.leg1.price,
        } : undefined,
      } : undefined,
    };
  }

  /**
   * Get current round state
   */
  getCurrentRound(): DipArbRoundState | null {
    return this.currentRound ? { ...this.currentRound } : null;
  }

  /**
   * Get current price to beat
   */
  getPriceToBeat(): number | null {
    return this.currentRound?.priceToBeat ?? null;
  }

  /**
   * Get current orderbook prices (live prices, not opening prices)
   * Used by frontend to display real-time UP/DOWN prices
   */
  getCurrentPrices(): { upAsk: number; downAsk: number; sum: number } {
    const upAsk = this.upAsks[0]?.price ?? 0;
    const downAsk = this.downAsks[0]?.price ?? 0;
    return {
      upAsk,
      downAsk,
      sum: upAsk + downAsk,
    };
  }

  /**
   * Check if service is currently searching for a market
   */
  isSearching(): boolean {
    return this.isSearchingMarket;
  }

  /**
   * Get when the current search started (for timeout detection)
   */
  getSearchStartTime(): number | null {
    return this.searchStartTime;
  }

  /**
   * Force stop searching state (used by health monitoring for stuck recovery)
   * Only use this for timeout/error recovery scenarios
   */
  forceStopSearching(): void {
    this.log('⚠️ Force stopping search (timeout/recovery)');
    this.isSearchingMarket = false;
    this.searchStartTime = null;
    this.emit('searchComplete', { success: false, forced: true });
  }

  // ===== Public API: Manual Execution =====

  /**
   * Check Binance momentum before Leg1 execution
   * 
   * The strategy from the excerpt:
   * "entering after confirmed momentum on external exchanges like Binance while Polymarket lags"
   * 
   * This validates that Binance price movement supports the expected direction:
   * - For DOWN dip: Binance should be dropping (supporting the dip)
   * - For UP surge: Binance should be rising (supporting the surge)
   * 
   * @returns Object with momentum confirmation status and details
   */
  private async checkBinanceMomentum(signal: DipArbLeg1Signal): Promise<{
    confirmed: boolean;
    binanceChangePercent: number;
    expectedDirection: 'up' | 'down';
    reason: string;
  }> {
    // Skip if Binance service not available or momentum check disabled
    if (!this.binanceService) {
      return {
        confirmed: true,
        binanceChangePercent: 0,
        expectedDirection: signal.dipSide === 'DOWN' ? 'down' : 'up',
        reason: 'Binance service not available - skipping momentum check',
      };
    }

    if (!this.config.enableBinanceMomentum) {
      return {
        confirmed: true,
        binanceChangePercent: 0,
        expectedDirection: signal.dipSide === 'DOWN' ? 'down' : 'up',
        reason: 'Binance momentum check disabled',
      };
    }

    if (!this.market?.underlying) {
      return {
        confirmed: true,
        binanceChangePercent: 0,
        expectedDirection: signal.dipSide === 'DOWN' ? 'down' : 'up',
        reason: 'No underlying asset configured',
      };
    }

    // Get Binance symbol for the underlying
    const binanceSymbol = UNDERLYING_TO_BINANCE[this.market.underlying];
    if (!binanceSymbol) {
      return {
        confirmed: true,
        binanceChangePercent: 0,
        expectedDirection: signal.dipSide === 'DOWN' ? 'down' : 'up',
        reason: `No Binance symbol for ${this.market.underlying}`,
      };
    }

    try {
      const windowMs = this.config.binanceMomentumWindowMs ?? 60000;
      const startTime = Date.now() - windowMs;
      
      const priceChange = await this.binanceService.getPriceChange(
        binanceSymbol,
        startTime
      );

      const threshold = this.config.binanceMomentumThreshold ?? 0.5;
      
      // Determine expected direction based on signal type
      // For a dip signal: if buying DOWN, expect Binance to be falling
      // For a surge signal: if buying UP, expect Binance to be rising
      let expectedDirection: 'up' | 'down';
      let confirmed: boolean;
      let reason: string;

      if (signal.source === 'dip') {
        // Dip detected on Polymarket
        // If DOWN is dipping, it means UP is surging, so Binance should be UP
        // If UP is dipping, Binance should be DOWN
        if (signal.dipSide === 'DOWN') {
          // We're buying DOWN which is dipping - expect Binance to be rising
          expectedDirection = 'up';
          confirmed = priceChange.changePercent >= threshold;
          reason = confirmed
            ? `Binance ${binanceSymbol} +${priceChange.changePercent.toFixed(2)}% confirms DOWN dip (Polymarket lagging)`
            : `Binance ${binanceSymbol} ${priceChange.changePercent.toFixed(2)}% - momentum below ${threshold}% threshold`;
        } else {
          // We're buying UP which is dipping - expect Binance to be falling
          expectedDirection = 'down';
          confirmed = priceChange.changePercent <= -threshold;
          reason = confirmed
            ? `Binance ${binanceSymbol} ${priceChange.changePercent.toFixed(2)}% confirms UP dip (Polymarket lagging)`
            : `Binance ${binanceSymbol} ${priceChange.changePercent.toFixed(2)}% - momentum below -${threshold}% threshold`;
        }
      } else if (signal.source === 'surge') {
        // Surge detected on Polymarket
        if (signal.dipSide === 'UP') {
          // We're buying UP which is surging - expect Binance to be rising
          expectedDirection = 'up';
          confirmed = priceChange.changePercent >= threshold;
          reason = confirmed
            ? `Binance ${binanceSymbol} +${priceChange.changePercent.toFixed(2)}% confirms UP surge`
            : `Binance ${binanceSymbol} ${priceChange.changePercent.toFixed(2)}% - momentum below ${threshold}% threshold`;
        } else {
          // We're buying DOWN which is surging - expect Binance to be falling
          expectedDirection = 'down';
          confirmed = priceChange.changePercent <= -threshold;
          reason = confirmed
            ? `Binance ${binanceSymbol} ${priceChange.changePercent.toFixed(2)}% confirms DOWN surge`
            : `Binance ${binanceSymbol} ${priceChange.changePercent.toFixed(2)}% - momentum below -${threshold}% threshold`;
        }
      } else {
        // Mispricing source - use underlying price direction
        expectedDirection = signal.dipSide === 'UP' ? 'up' : 'down';
        const matchesDirection = expectedDirection === 'up' 
          ? priceChange.changePercent >= threshold
          : priceChange.changePercent <= -threshold;
        confirmed = matchesDirection;
        reason = `Mispricing signal: Binance ${binanceSymbol} ${priceChange.changePercent.toFixed(2)}%`;
      }

      return {
        confirmed,
        binanceChangePercent: priceChange.changePercent,
        expectedDirection,
        reason,
      };
    } catch (error) {
      // On Binance API error, log but don't block execution
      const errorMessage = error instanceof Error ? error.message : String(error);
      this.log(`⚠️ Binance momentum check failed: ${errorMessage}`);
      return {
        confirmed: true, // Don't block on API errors
        binanceChangePercent: 0,
        expectedDirection: signal.dipSide === 'DOWN' ? 'down' : 'up',
        reason: `Binance API error: ${errorMessage}`,
      };
    }
  }

  /**
   * Execute Leg1 trade
   */
  async executeLeg1(signal: DipArbLeg1Signal): Promise<DipArbExecutionResult> {
    const startTime = Date.now();

    if (!this.tradingService || !this.market || !this.currentRound) {
      this.isExecuting = false;  // Reset in case handleSignal() set it
      return {
        success: false,
        leg: 'leg1',
        roundId: signal.roundId,
        error: 'Trading service not available or no active round',
        executionTimeMs: Date.now() - startTime,
      };
    }

    // ===== Max Open Positions Check =====
    // Prevent capital lockup by limiting concurrent unhedged positions
    const maxOpen = this.config.maxOpenPositions ?? 10;
    if (this.openPositionCount >= maxOpen) {
      this.isExecuting = false;
      this.log(`❌ Leg1 blocked: Max open positions (${maxOpen}) reached. Current: ${this.openPositionCount}`);
      // FIX #7: Emit signalRejected event for frontend visibility
      this.emit('signalRejected', {
        reason: 'maxOpenPositions',
        details: `${this.openPositionCount}/${maxOpen} positions open`,
        signal,
        timestamp: Date.now(),
      });
      return {
        success: false,
        leg: 'leg1',
        roundId: signal.roundId,
        error: `Max open positions (${maxOpen}) reached`,
        executionTimeMs: Date.now() - startTime,
      };
    }

    // ===== Binance Momentum Check =====
    // Validate that external exchange price movement supports the expected direction
    // This is the key edge from the excerpt: "entering after confirmed momentum on external exchanges"
    const momentumCheck = await this.checkBinanceMomentum(signal);
    
    if (this.config.debug || this.config.enableBinanceMomentum) {
      const statusIcon = momentumCheck.confirmed ? '✅' : '⚠️';
      this.log(`${statusIcon} Binance Momentum: ${momentumCheck.reason}`);
    }

    if (!momentumCheck.confirmed && this.config.requireBinanceMomentum) {
      this.isExecuting = false;
      this.log(`❌ Leg1 blocked: Binance momentum not confirmed (${momentumCheck.reason})`);
      return {
        success: false,
        leg: 'leg1',
        roundId: signal.roundId,
        error: `Binance momentum not confirmed: ${momentumCheck.reason}`,
        executionTimeMs: Date.now() - startTime,
      };
    }

    // ===== Paper Trading Mode =====
    // Simulate order fills without sending real orders
    if (this.config.paperMode) {
      return this.simulateLeg1Fill(signal, startTime);
    }

    try {
      this.isExecuting = true;  // Also set here for manual mode (when not called from handleSignal)

      // 计算拆分订单参数
      const splitCount = Math.max(1, this.config.splitOrders);

      // 机制保证：确保满足 $1 最低限额
      const minSharesForMinAmount = Math.ceil(1 / signal.targetPrice);
      const adjustedShares = Math.max(signal.shares, minSharesForMinAmount);

      if (adjustedShares > signal.shares) {
        this.log(`📊 Shares adjusted: ${signal.shares} → ${adjustedShares} (to meet $1 minimum at price ${signal.targetPrice.toFixed(4)})`);
      }

      const sharesPerOrder = adjustedShares / splitCount;
      const amountPerOrder = sharesPerOrder * signal.targetPrice;

      let totalSharesFilled = 0;
      let totalAmountSpent = 0;
      let lastOrderId: string | undefined;
      let failedOrders = 0;

      // 执行多笔订单
      for (let i = 0; i < splitCount; i++) {
        const orderParams: MarketOrderParams = {
          tokenId: signal.tokenId,
          side: 'BUY' as Side,
          amount: amountPerOrder,
        };

        if (this.config.debug && splitCount > 1) {
          this.log(`Leg1 order ${i + 1}/${splitCount}: ${sharesPerOrder.toFixed(2)} shares @ ${signal.targetPrice.toFixed(4)}`);
        }

        const result = await this.tradingService.createMarketOrder(orderParams);

        if (result.success) {
          totalSharesFilled += sharesPerOrder;
          totalAmountSpent += amountPerOrder;
          lastOrderId = result.orderId;
        } else {
          failedOrders++;
          this.log(`Leg1 order ${i + 1}/${splitCount} failed: ${result.errorMsg}`);
        }

        // 订单间隔
        if (i < splitCount - 1 && this.config.orderIntervalMs > 0) {
          await new Promise(resolve => setTimeout(resolve, this.config.orderIntervalMs));
        }
      }

      // 至少有一笔成功
      if (totalSharesFilled > 0) {
        const avgPrice = totalAmountSpent / totalSharesFilled;

        // Record leg1 fill
        this.currentRound.leg1 = {
          side: signal.dipSide,
          price: avgPrice,
          shares: totalSharesFilled,
          timestamp: Date.now(),
          tokenId: signal.tokenId,
        };
        this.currentRound.phase = 'leg1_filled';
        this.stats.leg1Filled++;
        this.openPositionCount++;  // Track for maxOpenPositions limit

        this.lastExecutionTime = Date.now();

        // Detailed execution logging
        const slippage = ((avgPrice - signal.currentPrice) / signal.currentPrice * 100);
        const execTimeMs = Date.now() - startTime;

        this.log(`✅ Leg1 FILLED: ${signal.dipSide} x${totalSharesFilled.toFixed(1)} @ ${avgPrice.toFixed(4)} (Open: ${this.openPositionCount})`);
        this.log(`   Expected: ${signal.currentPrice.toFixed(4)} | Actual: ${avgPrice.toFixed(4)} | Slippage: ${slippage >= 0 ? '+' : ''}${slippage.toFixed(2)}%`);
        this.log(`   Execution time: ${execTimeMs}ms | Orders: ${splitCount - failedOrders}/${splitCount}`);

        // ✅ FIX: Emit open position event for persistence (paper mode only)
        if (this.config.paperMode) {
          this.emit('openPositionCreated', {
            roundId: this.currentRound.roundId,
            marketConditionId: this.market.conditionId,
            marketName: this.market.name,
            marketUpTokenId: this.market.upTokenId,
            marketDownTokenId: this.market.downTokenId,
            roundStartTime: this.currentRound.startTime,
            roundEndTime: this.currentRound.endTime,
            priceToBeat: this.currentRound.priceToBeat,
            leg1Side: signal.dipSide,
            leg1Price: avgPrice,
            leg1Shares: totalSharesFilled,
            leg1Cost: totalAmountSpent,
            leg1Timestamp: Date.now(),
            leg1TokenId: signal.tokenId,
            config: {
              sumTarget: this.config.sumTarget,
              leg2TimeoutSeconds: this.getLeg2TimeoutSeconds(),
              shares: this.config.shares,
            },
          });
        }

        // Log orderbook after execution
        if (this.config.debug) {
          this.logOrderbookContext('Post-Leg1');
        }

        return {
          success: true,
          leg: 'leg1',
          roundId: signal.roundId,
          side: signal.dipSide,
          price: avgPrice,
          shares: totalSharesFilled,
          orderId: lastOrderId,
          executionTimeMs: execTimeMs,
        };
      } else {
        return {
          success: false,
          leg: 'leg1',
          roundId: signal.roundId,
          error: 'All orders failed',
          executionTimeMs: Date.now() - startTime,
        };
      }
    } catch (error) {
      return {
        success: false,
        leg: 'leg1',
        roundId: signal.roundId,
        error: error instanceof Error ? error.message : String(error),
        executionTimeMs: Date.now() - startTime,
      };
    } finally {
      this.isExecuting = false;
    }
  }

  /**
   * Simulate Leg1 fill for paper trading mode
   * Simulates order execution at signal price without sending real orders
   */
  private simulateLeg1Fill(signal: DipArbLeg1Signal, startTime: number): DipArbExecutionResult {
    if (!this.currentRound || !this.market) {
      this.isExecuting = false;
      return {
        success: false,
        leg: 'leg1',
        roundId: signal.roundId,
        error: 'No active round for paper trade',
        executionTimeMs: Date.now() - startTime,
      };
    }

    try {
      this.isExecuting = true;

      // Simulate fill at signal price with realistic slippage
      // FIXED: Use realistic slippage (0.5-2%+) instead of optimistic 0-0.5%
      const shares = this.config.shares;
      const slippageDecimal = calculateRealisticSlippage(shares, signal.currentPrice);
      const slippagePercent = slippageDecimal * 100;
      const simulatedPrice = signal.currentPrice * (1 + slippageDecimal);
      const cost = shares * simulatedPrice;

      // Simulate execution delay (50-150ms)
      const simulatedExecTime = 50 + Math.floor(Math.random() * 100);

      // Record leg1 fill
      this.currentRound.leg1 = {
        side: signal.dipSide,
        price: simulatedPrice,
        shares,
        timestamp: Date.now(),
        tokenId: signal.tokenId,
      };
      this.currentRound.phase = 'leg1_filled';
      this.stats.leg1Filled++;
      this.openPositionCount++;  // Track for maxOpenPositions limit

      this.lastExecutionTime = Date.now();

      this.log(`📝 [PAPER] Leg1 FILLED: ${signal.dipSide} x${shares.toFixed(1)} @ ${simulatedPrice.toFixed(4)} (Open: ${this.openPositionCount})`);
      this.log(`   Signal: ${signal.currentPrice.toFixed(4)} | Simulated: ${simulatedPrice.toFixed(4)} | Slippage: +${slippagePercent.toFixed(2)}%`);
      this.log(`   Cost: $${cost.toFixed(2)} | Exec time: ${simulatedExecTime}ms (simulated)`);

      // ✅ FIX: Emit open position event for persistence
      this.emit('openPositionCreated', {
        roundId: this.currentRound.roundId,
        marketConditionId: this.market.conditionId,
        marketName: this.market.name,
        marketUpTokenId: this.market.upTokenId,
        marketDownTokenId: this.market.downTokenId,
        roundStartTime: this.currentRound.startTime,
        roundEndTime: this.currentRound.endTime,
        priceToBeat: this.currentRound.priceToBeat,
        leg1Side: signal.dipSide,
        leg1Price: simulatedPrice,
        leg1Shares: shares,
        leg1Cost: cost,
        leg1Timestamp: Date.now(),
        leg1TokenId: signal.tokenId,
        config: {
          sumTarget: this.config.sumTarget,
          leg2TimeoutSeconds: this.getLeg2TimeoutSeconds(),
          shares: this.config.shares,
        },
      });

      // Emit paper trade event for frontend tracking
      this.emit('paperTrade', {
        type: 'leg1',
        side: signal.dipSide,
        shares,
        price: simulatedPrice,
        cost,
        expectedPrice: signal.currentPrice,          // Signal price before slippage
        slippagePercent: slippageDecimal,            // As decimal (e.g., 0.005 for 0.5%)
        marketName: this.market.name,
        conditionId: this.market.conditionId,
        timestamp: Date.now(),
      });

      return {
        success: true,
        leg: 'leg1',
        roundId: signal.roundId,
        side: signal.dipSide,
        price: simulatedPrice,
        shares,
        orderId: `paper_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
        executionTimeMs: simulatedExecTime,
      };
    } finally {
      this.isExecuting = false;
    }
  }

  /**
   * Execute Leg2 trade
   */
  async executeLeg2(signal: DipArbLeg2Signal): Promise<DipArbExecutionResult> {
    const startTime = Date.now();

    if (!this.market || !this.currentRound) {
      this.isExecuting = false;  // Reset in case handleSignal() set it
      return {
        success: false,
        leg: 'leg2',
        roundId: signal.roundId,
        error: 'No active market or round',
        executionTimeMs: Date.now() - startTime,
      };
    }

    // ===== Paper Trading Mode =====
    if (this.config.paperMode) {
      return this.simulateLeg2Fill(signal, startTime);
    }

    if (!this.tradingService) {
      this.isExecuting = false;
      return {
        success: false,
        leg: 'leg2',
        roundId: signal.roundId,
        error: 'Trading service not available',
        executionTimeMs: Date.now() - startTime,
      };
    }

    try {
      this.isExecuting = true;  // Also set here for manual mode (when not called from handleSignal)

      // 计算拆分订单参数
      const splitCount = Math.max(1, this.config.splitOrders);

      // 机制保证：确保满足 $1 最低限额
      const minSharesForMinAmount = Math.ceil(1 / signal.targetPrice);
      const adjustedShares = Math.max(signal.shares, minSharesForMinAmount);

      if (adjustedShares > signal.shares) {
        this.log(`📊 Leg2 Shares adjusted: ${signal.shares} → ${adjustedShares} (to meet $1 minimum at price ${signal.targetPrice.toFixed(4)})`);
      }

      const sharesPerOrder = adjustedShares / splitCount;
      const amountPerOrder = sharesPerOrder * signal.targetPrice;

      let totalSharesFilled = 0;
      let totalAmountSpent = 0;
      let lastOrderId: string | undefined;
      let failedOrders = 0;

      // 执行多笔订单
      for (let i = 0; i < splitCount; i++) {
        const orderParams: MarketOrderParams = {
          tokenId: signal.tokenId,
          side: 'BUY' as Side,
          amount: amountPerOrder,
        };

        if (this.config.debug && splitCount > 1) {
          this.log(`Leg2 order ${i + 1}/${splitCount}: ${sharesPerOrder.toFixed(2)} shares @ ${signal.targetPrice.toFixed(4)}`);
        }

        const result = await this.tradingService.createMarketOrder(orderParams);

        if (result.success) {
          totalSharesFilled += sharesPerOrder;
          totalAmountSpent += amountPerOrder;
          lastOrderId = result.orderId;
        } else {
          failedOrders++;
          this.log(`Leg2 order ${i + 1}/${splitCount} failed: ${result.errorMsg}`);
        }

        // 订单间隔
        if (i < splitCount - 1 && this.config.orderIntervalMs > 0) {
          await new Promise(resolve => setTimeout(resolve, this.config.orderIntervalMs));
        }
      }

      // 至少有一笔成功
      if (totalSharesFilled > 0) {
        const avgPrice = totalAmountSpent / totalSharesFilled;
        const leg1Price = this.currentRound.leg1?.price || 0;
        const actualTotalCost = leg1Price + avgPrice;

        // Record leg2 fill
        this.currentRound.leg2 = {
          side: signal.hedgeSide,
          price: avgPrice,
          shares: totalSharesFilled,
          timestamp: Date.now(),
          tokenId: signal.tokenId,
        };
        this.currentRound.phase = 'completed';
        this.currentRound.totalCost = actualTotalCost;

        // ✅ FIX: Calculate NET profit after fees (same as paper mode)
        const feeRate = this.config.takerFeeRate ?? DIP_ARB_CRYPTO_TAKER_FEE;
        const grossProfit = 1 - actualTotalCost;
        const totalFees = actualTotalCost * feeRate * 2;
        const netProfitPerShare = grossProfit - totalFees;
        const netProfit = netProfitPerShare * totalSharesFilled;

        this.currentRound.profit = netProfitPerShare; // Store NET profit per share

        this.stats.leg2Filled++;
        this.stats.roundsSuccessful++;
        this.stats.totalProfit += netProfit; // ✅ Use NET profit
        this.stats.totalSpent += actualTotalCost * totalSharesFilled;
        this.openPositionCount = Math.max(0, this.openPositionCount - 1);  // Position hedged

        this.lastExecutionTime = Date.now();

        // Detailed execution logging
        const slippage = ((avgPrice - signal.currentPrice) / signal.currentPrice * 100);
        const execTimeMs = Date.now() - startTime;
        const grossProfitTotal = grossProfit * totalSharesFilled;

        this.log(`✅ Leg2 FILLED: ${signal.hedgeSide} x${totalSharesFilled.toFixed(1)} @ ${avgPrice.toFixed(4)}`);
        this.log(`   Expected: ${signal.currentPrice.toFixed(4)} | Actual: ${avgPrice.toFixed(4)} | Slippage: ${slippage >= 0 ? '+' : ''}${slippage.toFixed(2)}%`);
        this.log(`   Leg1: ${leg1Price.toFixed(4)} + Leg2: ${avgPrice.toFixed(4)} = ${actualTotalCost.toFixed(4)}`);
        this.log(`   💰 Profit: $${netProfit.toFixed(2)} NET (gross: $${grossProfitTotal.toFixed(2)}, fees: $${(totalFees * totalSharesFilled).toFixed(2)})`);
        this.log(`   Execution time: ${execTimeMs}ms | Orders: ${splitCount - failedOrders}/${splitCount}`);

        // Log orderbook after execution
        if (this.config.debug) {
          this.logOrderbookContext('Post-Leg2');
        }

        // Calculate net profit rate for reporting
        const netProfitRate = calculateDipArbNetProfitRate(this.currentRound.totalCost, feeRate);

        const roundResult: DipArbRoundResult = {
          roundId: signal.roundId,
          status: 'completed',
          leg1: this.currentRound.leg1,
          leg2: this.currentRound.leg2,
          totalCost: this.currentRound.totalCost,
          profit: netProfit, // ✅ FIX: Use NET profit (was: this.currentRound.profit)
          profitRate: netProfitRate,
          merged: false,
        };

        this.emit('roundComplete', roundResult);

        // Emit position closed event for backend persistence
        this.emit('openPositionClosed', {
          roundId: signal.roundId,
          status: 'closed',
          closeType: 'leg2',
          netProfit: netProfit, // ✅ Already NET profit
        });

        // Auto merge if enabled
        if (this.config.autoMerge) {
          const mergeResult = await this.merge();
          roundResult.merged = mergeResult.success;
          roundResult.mergeTxHash = mergeResult.txHash;
        }

        return {
          success: true,
          leg: 'leg2',
          roundId: signal.roundId,
          side: signal.hedgeSide,
          price: avgPrice,
          shares: totalSharesFilled,
          orderId: lastOrderId,
          executionTimeMs: Date.now() - startTime,
        };
      } else {
        return {
          success: false,
          leg: 'leg2',
          roundId: signal.roundId,
          error: 'All orders failed',
          executionTimeMs: Date.now() - startTime,
        };
      }
    } catch (error) {
      return {
        success: false,
        leg: 'leg2',
        roundId: signal.roundId,
        error: error instanceof Error ? error.message : String(error),
        executionTimeMs: Date.now() - startTime,
      };
    } finally {
      this.isExecuting = false;
    }
  }

  /**
   * Simulate Leg2 fill for paper trading mode
   * Completes the arbitrage simulation and calculates paper profit
   */
  private simulateLeg2Fill(signal: DipArbLeg2Signal, startTime: number): DipArbExecutionResult {
    if (!this.currentRound || !this.market || !this.currentRound.leg1) {
      this.isExecuting = false;
      return {
        success: false,
        leg: 'leg2',
        roundId: signal.roundId,
        error: 'No active round or Leg1 not filled',
        executionTimeMs: Date.now() - startTime,
      };
    }

    try {
      this.isExecuting = true;

      const leg1 = this.currentRound.leg1;

      // Simulate fill at signal price with realistic slippage
      // FIXED: Use realistic slippage (0.5-2%+) instead of optimistic 0-0.5%
      const shares = leg1.shares; // Match Leg1 shares for balanced hedge
      const slippageDecimal = calculateRealisticSlippage(shares, signal.currentPrice);
      const slippagePercent = slippageDecimal * 100;
      const simulatedPrice = signal.currentPrice * (1 + slippageDecimal);
      const cost = shares * simulatedPrice;

      const actualTotalCost = leg1.price + simulatedPrice;

      // ✅ FIX: Calculate NET profit after fees (not gross profit)
      const grossProfit = 1 - actualTotalCost;
      const feeRate = this.config.takerFeeRate ?? 0.03;
      const totalFees = actualTotalCost * feeRate * 2; // 6% total (3% per leg)
      const netProfitPerShare = grossProfit - totalFees;
      const netProfitPerTrade = netProfitPerShare * shares; // NET profit in dollars
      const grossProfitPerTrade = grossProfit * shares; // For comparison logging

      // Simulate execution delay (50-150ms)
      const simulatedExecTime = 50 + Math.floor(Math.random() * 100);

      // Record leg2 fill
      this.currentRound.leg2 = {
        side: signal.hedgeSide,
        price: simulatedPrice,
        shares,
        timestamp: Date.now(),
        tokenId: signal.tokenId,
      };
      this.currentRound.phase = 'completed';
      this.currentRound.totalCost = actualTotalCost;
      this.currentRound.profit = netProfitPerShare; // Store NET profit per share

      this.stats.leg2Filled++;
      this.stats.roundsSuccessful++;
      this.stats.totalProfit += netProfitPerTrade; // ✅ Use NET profit
      this.stats.totalSpent += actualTotalCost * shares;
      this.openPositionCount = Math.max(0, this.openPositionCount - 1);  // Position hedged

      this.lastExecutionTime = Date.now();

      // Calculate profit rates for logging
      const grossProfitRate = (1 - actualTotalCost) / actualTotalCost;
      const netProfitRate = this.config.useFeeAdjustedProfit
        ? calculateDipArbNetProfitRate(actualTotalCost, feeRate)
        : grossProfitRate;

      this.log(`📝 [PAPER] Leg2 FILLED: ${signal.hedgeSide} x${shares.toFixed(1)} @ ${simulatedPrice.toFixed(4)}`);
      this.log(`   Leg1: ${leg1.price.toFixed(4)} + Leg2: ${simulatedPrice.toFixed(4)} = ${actualTotalCost.toFixed(4)}`);
      this.log(`   💰 [PAPER] Profit: $${netProfitPerTrade.toFixed(2)} NET (gross: $${grossProfitPerTrade.toFixed(2)}, fees: $${(totalFees * shares).toFixed(2)})`);
      this.log(`   📊 [PAPER] Rates: ${(grossProfitRate * 100).toFixed(2)}% gross, ${(netProfitRate * 100).toFixed(2)}% net`);

      // Emit paper trade event for frontend tracking
      this.emit('paperTrade', {
        type: 'leg2',
        side: signal.hedgeSide,
        shares,
        price: simulatedPrice,
        cost,
        totalCost: actualTotalCost,
        profit: netProfitPerTrade, // ✅ Now sends NET profit (after fees)
        profitRate: netProfitRate,
        expectedPrice: signal.currentPrice,          // Signal price before slippage
        slippagePercent: slippagePercent / 100,      // As decimal (e.g., 0.005 for 0.5%)
        marketName: this.market.name,
        conditionId: this.market.conditionId,
        timestamp: Date.now(),
      });

      // Emit round complete
      const roundResult: DipArbRoundResult = {
        roundId: signal.roundId,
        status: 'completed',
        leg1: this.currentRound.leg1,
        leg2: this.currentRound.leg2,
        totalCost: actualTotalCost,
        profit: netProfitPerTrade, // ✅ Use NET profit
        profitRate: netProfitRate,
        merged: true, // Paper trades are "merged" instantly
      };

      this.emit('roundComplete', roundResult);

      // ✅ FIX: Emit position closed event for backend persistence
      this.emit('openPositionClosed', {
        roundId: this.currentRound.roundId,
        status: 'closed',
        closeType: 'leg2',
        netProfit: netProfitPerTrade,
      });

      return {
        success: true,
        leg: 'leg2',
        roundId: signal.roundId,
        side: signal.hedgeSide,
        price: simulatedPrice,
        shares,
        orderId: `paper_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
        executionTimeMs: simulatedExecTime,
      };
    } finally {
      this.isExecuting = false;
    }
  }

  /**
   * Merge UP + DOWN tokens to USDC.e
   *
   * Uses mergeByTokenIds with Polymarket token IDs for correct CLOB market handling.
   * This locks in profit immediately after Leg2 completes.
   */
  async merge(): Promise<DipArbExecutionResult> {
    const startTime = Date.now();
    const roundId = this.currentRound?.roundId || 'unknown';

    if (!this.ctf || !this.market || !this.currentRound) {
      return {
        success: false,
        leg: 'merge',
        roundId,
        error: 'CTF client not available or no completed round',
        executionTimeMs: Date.now() - startTime,
      };
    }

    // Merge the minimum of Leg1 and Leg2 shares (should be equal after our fix)
    const shares = Math.min(
      this.currentRound.leg1?.shares || 0,
      this.currentRound.leg2?.shares || 0
    );

    if (shares <= 0) {
      return {
        success: false,
        leg: 'merge',
        roundId,
        error: 'No shares to merge',
        executionTimeMs: Date.now() - startTime,
      };
    }

    try {
      // Use mergeByTokenIds with Polymarket token IDs
      const tokenIds = {
        yesTokenId: this.market.upTokenId,
        noTokenId: this.market.downTokenId,
      };

      this.log(`🔄 Merging ${shares.toFixed(1)} UP + DOWN → USDC.e...`);

      const result = await this.ctf.mergeByTokenIds(
        this.market.conditionId,
        tokenIds,
        shares.toString()
      );

      if (result.success) {
        this.log(`✅ Merge successful: ${shares.toFixed(1)} pairs → $${result.usdcReceived || shares.toFixed(2)} USDC.e`);
        this.log(`   TxHash: ${result.txHash?.slice(0, 20)}...`);
      }

      return {
        success: result.success,
        leg: 'merge',
        roundId,
        shares,
        txHash: result.txHash,
        executionTimeMs: Date.now() - startTime,
      };
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : String(error);
      this.log(`❌ Merge failed: ${errorMsg}`);
      return {
        success: false,
        leg: 'merge',
        roundId,
        error: errorMsg,
        executionTimeMs: Date.now() - startTime,
      };
    }
  }

  // ===== Private: Event Handlers =====

  // Track last orderbook update time for debugging
  private lastOrderbookUpdateTime = 0;
  private orderbookUpdateCount = 0;

  private handleOrderbookUpdate(book: OrderbookSnapshot): void {
    if (!this.market) return;

    // Track update frequency
    const now = Date.now();
    this.orderbookUpdateCount++;

    // Log first orderbook update (always, not just in debug mode)
    if (this.orderbookUpdateCount === 1) {
      console.log(`[DipArb] 📡 First orderbook update received for ${this.market.underlying}`);
    }

    if (this.config.debug && now - this.lastOrderbookUpdateTime > 5000) {
      // Log update rate every 5 seconds
      const elapsed = now - this.lastOrderbookUpdateTime;
      const rate = (this.orderbookUpdateCount / elapsed) * 1000;
      this.log(`📡 Orderbook updates: ${this.orderbookUpdateCount} in ${(elapsed/1000).toFixed(1)}s (${rate.toFixed(1)}/sec)`);
      this.lastOrderbookUpdateTime = now;
      this.orderbookUpdateCount = 0;
    }

    // Determine which side this update is for
    const tokenId = book.tokenId;
    const isUpToken = tokenId === this.market.upTokenId;
    const isDownToken = tokenId === this.market.downTokenId;

    // OrderbookLevel has price and size as numbers
    if (isUpToken) {
      this.upAsks = book.asks.map(l => ({ price: l.price, size: l.size }));
      if (this.config.debug && this.upAsks.length === 0) {
        this.log('⚠️ UP orderbook is empty!');
      }
    } else if (isDownToken) {
      this.downAsks = book.asks.map(l => ({ price: l.price, size: l.size }));
      if (this.config.debug && this.downAsks.length === 0) {
        this.log('⚠️ DOWN orderbook is empty!');
      }
    }

    // Record price history for sliding window detection
    this.recordPriceHistory();

    // Emit orderbook update for frontend (every 2 seconds to avoid spam)
    if (!this.lastOrderbookEmitTime || now - this.lastOrderbookEmitTime > 2000) {
      const upAsk = this.upAsks[0]?.price ?? 0;
      const downAsk = this.downAsks[0]?.price ?? 0;
      const upSize = this.upAsks[0]?.size ?? 0;
      const downSize = this.downAsks[0]?.size ?? 0;
      
      // Emit orderbook (map UP→YES, DOWN→NO for frontend compatibility)
      this.emit('orderbook', {
        yesAsk: upAsk,
        yesBid: 0, // DipArb doesn't track bids
        noAsk: downAsk,
        noBid: 0,
        yesAskSize: upSize,
        noAskSize: downSize,
        longCost: upAsk + downAsk,  // Cost to buy both sides
        shortRevenue: 0,  // Not applicable for DipArb
        longArbProfit: upAsk + downAsk > 0 ? 1 - (upAsk + downAsk) : 0,  // Profit if sum < $1
        shortArbProfit: 0,  // Not applicable for DipArb
        timestamp: now,
      });
      this.lastOrderbookEmitTime = now;
    }

    // Check if we need to start a new round (async but fire-and-forget to not block orderbook updates)
    this.checkAndStartNewRound().catch(err => {
      this.emit('error', err instanceof Error ? err : new Error(String(err)));
    });

    // FIX #3: Always detect signals, even during execution
    // Previously blocked ALL detection during Leg1 execution (50-150ms window)
    // Now we detect signals but only block NEW Leg1 execution in handleSignal()
    // This allows Leg2 signals to be detected while Leg1 is executing

    // Detect signals
    const signal = this.detectSignal();
    if (signal) {
      this.handleSignal(signal);
    }
  }

  /**
   * Record current prices to history buffer for sliding window detection
   */
  private recordPriceHistory(): void {
    const upAsk = this.upAsks[0]?.price ?? 0;
    const downAsk = this.downAsks[0]?.price ?? 0;

    // Only record if we have valid prices
    if (upAsk <= 0 || downAsk <= 0) {
      if (this.config.debug) {
        this.log(`⚠️ Skipping price history: invalid prices (UP: ${upAsk}, DOWN: ${downAsk})`);
      }
      return;
    }

    this.priceHistory.push({
      timestamp: Date.now(),
      upAsk,
      downAsk,
    });

    // Trim history to max length
    if (this.priceHistory.length > this.MAX_HISTORY_LENGTH) {
      this.priceHistory = this.priceHistory.slice(-this.MAX_HISTORY_LENGTH);
    }

    // Debug log: Show price history status every 10 updates
    if (this.config.debug && this.priceHistory.length % 10 === 0) {
      this.log(`📊 Price history: ${this.priceHistory.length} entries, UP: ${upAsk.toFixed(4)}, DOWN: ${downAsk.toFixed(4)}, Sum: ${(upAsk + downAsk).toFixed(4)}`);
    }
  }

  /**
   * Get price from N milliseconds ago for sliding window detection
   *
   * @param side - 'UP' or 'DOWN'
   * @param msAgo - Milliseconds ago (e.g., 3000 for 3 seconds)
   * @returns Price from that time, or null if not available
   */
  private getPriceFromHistory(side: 'UP' | 'DOWN', msAgo: number): number | null {
    const targetTime = Date.now() - msAgo;

    // Find the closest price point at or before targetTime
    for (let i = this.priceHistory.length - 1; i >= 0; i--) {
      const entry = this.priceHistory[i];
      if (entry.timestamp <= targetTime) {
        return side === 'UP' ? entry.upAsk : entry.downAsk;
      }
    }

    return null;
  }

  private handleUnderlyingPriceUpdate(price: CryptoPrice): void {
    if (!this.market) return;

    // Only handle updates for our underlying (symbol format: eth/usd - lowercase)
    const expectedSymbol = `${this.market.underlying.toLowerCase()}/usd`;
    if (price.symbol !== expectedSymbol) return;

    if (this.config.debug) {
      this.log(`Underlying price update: ${price.symbol} = $${price.price.toFixed(2)}`);
    }

    // currentUnderlyingPrice = LIVE Chainlink price (for tracking/display)
    // This is NOT the same as priceToBeat (which is fixed at window start)
    this.currentUnderlyingPrice = price.price;

    // Fallback: If priceToBeat couldn't be fetched from API, use first Chainlink price
    if (this.currentRound && this.currentRound.priceToBeat === 0 && price.price > 0) {
      this.currentRound.priceToBeat = price.price;
      console.log(`[DipArb] ⚠️ Using fallback Price to Beat from Chainlink: $${price.price.toLocaleString()}`);
      this.log(`⚠️ Fallback: Price to Beat set from live Chainlink: $${price.price.toLocaleString()}`);

      // Emit newRound event with updated priceToBeat so frontend updates
      const event: DipArbNewRoundEvent = {
        roundId: this.currentRound.roundId,
        priceToBeat: price.price,
        upOpen: this.currentRound.openPrices.up,
        downOpen: this.currentRound.openPrices.down,
        startTime: this.currentRound.startTime,
        endTime: this.currentRound.endTime,
      };
      this.emit('newRound', event);
    }

    // Emit price update event
    if (this.currentRound) {
      const event: DipArbPriceUpdateEvent = {
        underlying: this.market.underlying,
        value: price.price,
        priceToBeat: this.currentRound.priceToBeat,
        changePercent: this.currentRound.priceToBeat > 0
          ? ((price.price - this.currentRound.priceToBeat) / this.currentRound.priceToBeat) * 100
          : 0,
      };
      this.emit('priceUpdate', event);
    }
  }

  // ===== Private: Round Management =====

  private async checkAndStartNewRound(): Promise<void> {
    if (!this.market) {
      if (this.config.debug) {
        this.log('⚠️ checkAndStartNewRound: No market configured');
      }
      return;
    }

    // If no current round or current round is completed/expired, start new round
    if (!this.currentRound || this.currentRound.phase === 'completed' || this.currentRound.phase === 'expired') {
      // Check if market is still active
      const now = new Date();
      const endTime = this.market.endTime instanceof Date
        ? this.market.endTime
        : new Date(this.market.endTime);

      if (now >= endTime) {
        // Always log market end (not just in debug mode)
        if (!this.currentRound) {
          console.log(`[DipArb] Market has ended before round could start (now=${now.toISOString()}, end=${endTime.toISOString()})`);
        }
        return;
      }

      // Price to Beat = Chainlink price at window START (fixed when market begins)
      // This comes from groupItemThreshold in the market data
      // currentUnderlyingPrice = LIVE Chainlink price (for tracking/display)
      const priceToBeat = this.market.priceToBeat || 0;

      // Log once when creating first round
      if (!this.currentRound) {
        console.log(`[DipArb] Creating first round for ${this.market.underlying} (endTime=${endTime.toISOString()}, priceToBeat=$${priceToBeat.toLocaleString()}, currentPrice=$${(this.currentUnderlyingPrice || 0).toLocaleString()})`);
      }

      // Get current prices
      const upPrice = this.upAsks[0]?.price ?? 0.5;
      const downPrice = this.downAsks[0]?.price ?? 0.5;

      if (this.config.debug) {
        this.log(`🆕 Creating new round: UP=${upPrice.toFixed(4)}, DOWN=${downPrice.toFixed(4)}, Sum=${(upPrice + downPrice).toFixed(4)}`);
      }

      // Warn if price to beat is missing (market data issue)
      if (priceToBeat === 0) {
        this.log(`⚠️ WARNING: Price to beat is $0 - groupItemThreshold not set in market data`);
        this.log(`   This may indicate the market hasn't started yet or data wasn't fetched correctly`);
      }

      // Create new round with ACTUAL market end time from Polymarket
      const roundId = `${this.market.slug}-${Date.now()}`;
      // Ensure endTime is a proper timestamp
      const marketEndTime = this.market.endTime instanceof Date
        ? this.market.endTime.getTime()
        : (typeof this.market.endTime === 'string'
          ? new Date(this.market.endTime).getTime()
          : this.market.endTime);
      this.currentRound = createDipArbRoundState(
        roundId,
        priceToBeat,
        upPrice,
        downPrice,
        marketEndTime,  // Use actual market end time, NOT calculated from duration
        this.market.durationMinutes  // Fallback only
      );

      // Immediate fallback: If priceToBeat is 0 but we already have a Chainlink price, use it now
      // This handles the case where Chainlink updates arrived BEFORE the round was created
      if (this.currentRound.priceToBeat === 0 && this.currentUnderlyingPrice && this.currentUnderlyingPrice > 0) {
        this.currentRound.priceToBeat = this.currentUnderlyingPrice;
        console.log(`[DipArb] ⚠️ Immediate fallback: Price to Beat set from existing Chainlink: $${this.currentUnderlyingPrice.toLocaleString()}`);
      }

      // Clear price history for new round - we only want to detect instant drops within this round
      const oldHistorySize = this.priceHistory.length;
      this.priceHistory = [];

      // Reset signal state for new round
      this.leg1SignalEmitted = false;

      this.stats.roundsMonitored++;

      // Use the potentially updated priceToBeat from fallback
      const finalPriceToBeat = this.currentRound.priceToBeat;

      const event: DipArbNewRoundEvent = {
        roundId,
        priceToBeat: finalPriceToBeat,
        upOpen: upPrice,
        downOpen: downPrice,
        startTime: this.currentRound.startTime,
        endTime: this.currentRound.endTime,
      };

      this.emit('newRound', event);
      console.log(`[DipArb] ✅ New round for ${this.market.underlying}: Target=$${finalPriceToBeat.toFixed(2)}, UP=${upPrice.toFixed(4)}, DOWN=${downPrice.toFixed(4)}`);
      this.log(`✅ New round #${this.stats.roundsMonitored}: ${roundId.slice(0, 30)}..., Price to Beat: $${finalPriceToBeat.toFixed(2)} (cleared ${oldHistorySize} history entries)`);
    } else if (this.config.debug) {
      this.log(`⏳ Round already active: ${this.currentRound.roundId.slice(0, 30)}..., phase: ${this.currentRound.phase}`);
    }

    // Check for round expiration - exit Leg1 if Leg2 times out
    if (this.currentRound && this.currentRound.phase === 'leg1_filled') {
      const elapsed = (Date.now() - (this.currentRound.leg1?.timestamp || this.currentRound.startTime)) / 1000;
      if (elapsed > this.getLeg2TimeoutSeconds()) {
        // ✅ Settlement Awareness: Get enhanced decision with full rule evaluations
        const enhancedDecision = this.getEnhancedSettlementDecision();
        const holdDecision = this.shouldHoldForSettlement();

        // Calculate metrics for settlement decision event
        const timeToEndMin = this.market
          ? ((this.market.endTime instanceof Date
              ? this.market.endTime.getTime()
              : (typeof this.market.endTime === 'number' ? this.market.endTime : new Date(this.market.endTime).getTime()))
            - Date.now()) / 60000
          : 0;

        // Emit legacy settlement decision event for backward compatibility
        this.emit('settlementDecision', {
          elapsed,
          baseTimeout: this.getLeg2TimeoutSeconds(),
          decision: holdDecision.hold ? 'HOLD' : 'EXIT',
          reason: holdDecision.reason,
          confidence: holdDecision.confidence,
          leg1Side: this.currentRound.leg1?.side,
          winProbability: this.getPositionWinProbability(this.currentRound.leg1?.side ?? 'UP'),
          timeToEndMin,
          roundId: this.currentRound.roundId,
          marketName: this.market?.name ?? '',
          underlying: this.market?.underlying,
        });

        // Emit enhanced settlement decision event with full rule evaluations for observability
        if (enhancedDecision) {
          this.emit('enhancedSettlementDecision', enhancedDecision);
          // Schedule settlement outcome check for BOTH hold and exit decisions
          // For HOLD: tracks actual settlement result
          // For EXIT: tracks counterfactual (what would have happened if we held)
          this.scheduleSettlementOutcomeCheck(enhancedDecision);
        }

        if (holdDecision.hold) {
          // Hold for settlement - don't exit
          this.log(`🔒 Holding for settlement: ${holdDecision.reason} (confidence: ${(holdDecision.confidence * 100).toFixed(0)}%, time to end: ${timeToEndMin.toFixed(1)}min)`);

          // Don't exit - continue monitoring until market end
          return;
        }

        // Proceed with timeout exit
        this.log(`⚠️ Leg2 timeout (${elapsed.toFixed(0)}s > ${this.getLeg2TimeoutSeconds()}s), exiting Leg1 position...`);

        // Try to sell Leg1 position
        const exitResult = await this.emergencyExitLeg1();

        this.currentRound.phase = 'expired';
        this.stats.roundsExpired++;
        this.stats.roundsCompleted++;

        const result: DipArbRoundResult = {
          roundId: this.currentRound.roundId,
          status: 'expired',
          leg1: this.currentRound.leg1,
          merged: false,
          exitResult,  // Include exit result for tracking
        };

        this.emit('roundComplete', result);
        this.log(`Round expired: ${this.currentRound.roundId} | Exit: ${exitResult?.success ? 'SUCCESS' : 'FAILED'}`);
      }
    }
  }

  /**
   * Emergency exit Leg1 position when Leg2 times out
   * Sells the Leg1 tokens at market price to avoid unhedged exposure
   */
  private async emergencyExitLeg1(): Promise<DipArbExecutionResult | null> {
    if (!this.tradingService || !this.market || !this.currentRound?.leg1) {
      this.log('Cannot exit Leg1: no trading service or position');
      return null;
    }

    const leg1 = this.currentRound.leg1;
    const startTime = Date.now();

    try {
      this.log(`Selling ${leg1.shares} ${leg1.side} tokens...`);

      // Get current price for the token
      const currentPrice = leg1.side === 'UP'
        ? (this.upAsks[0]?.price ?? 0.5)
        : (this.downAsks[0]?.price ?? 0.5);

      const exitAmount = leg1.shares * currentPrice;

      // 检查退出金额是否满足最低限额
      if (exitAmount < 1) {
        this.log(`⚠️ Exit amount ($${exitAmount.toFixed(2)}) below $1 minimum - position will be held to expiry`);
        return {
          success: false,
          leg: 'exit',
          roundId: this.currentRound.roundId,
          error: `Exit amount ($${exitAmount.toFixed(2)}) below Polymarket minimum ($1) - holding to expiry`,
          executionTimeMs: Date.now() - startTime,
        };
      }

      // In paper mode, simulate the exit instead of real trading
      if (this.config.paperMode) {
        const simulatedSlippage = 0.005 + Math.random() * 0.01; // 0.5-1.5% simulated slippage
        const soldPrice = currentPrice * (1 - simulatedSlippage);
        const loss = (leg1.price - soldPrice) * leg1.shares;
        
        this.log(`📝 [PAPER] Leg1 exit simulated: sold ${leg1.shares}x ${leg1.side} @ ~${soldPrice.toFixed(4)} | Loss: $${loss.toFixed(2)}`);
        
        // Emit paper trade event for the exit
        this.emit('paperTrade', {
          type: 'exit',
          side: leg1.side,
          shares: leg1.shares,
          price: soldPrice,
          cost: -exitAmount, // Negative cost = received money
          profit: -Math.abs(loss),
          expectedPrice: currentPrice,              // Price before slippage
          slippagePercent: simulatedSlippage,       // As decimal (e.g., 0.01 for 1%)
          marketName: this.market.name,
          conditionId: this.market.conditionId,
          timestamp: Date.now(),
        });

        // ✅ FIX: Emit position closed event for backend persistence
        this.emit('openPositionClosed', {
          roundId: this.currentRound.roundId,
          status: 'closed',
          closeType: 'exit',
          loss: Math.abs(loss),
        });

        // Update stats with the loss
        this.stats.totalProfit -= Math.abs(loss);
        this.openPositionCount = Math.max(0, this.openPositionCount - 1);  // Position closed

        return {
          success: true,
          leg: 'exit',
          roundId: this.currentRound.roundId,
          side: leg1.side,
          price: soldPrice,
          shares: leg1.shares,
          orderId: `paper_exit_${Date.now()}`,
          executionTimeMs: Date.now() - startTime,
        };
      }

      // Market sell the position (REAL trading only)
      const result = await this.tradingService.createMarketOrder({
        tokenId: leg1.tokenId,
        side: 'SELL' as Side,
        amount: exitAmount,
      });

      if (result.success) {
        const soldPrice = currentPrice;  // Approximate
        const loss = (leg1.price - soldPrice) * leg1.shares;

        this.log(`✅ Leg1 exit successful: sold ${leg1.shares}x ${leg1.side} @ ~${soldPrice.toFixed(4)} | Loss: $${loss.toFixed(2)}`);

        // ✅ FIX: Emit position closed event for backend persistence
        this.emit('openPositionClosed', {
          roundId: this.currentRound.roundId,
          status: 'closed',
          closeType: 'exit',
          loss: Math.abs(loss),
        });

        // Update stats with the loss
        this.stats.totalProfit -= Math.abs(loss);
        this.openPositionCount = Math.max(0, this.openPositionCount - 1);  // Position closed

        return {
          success: true,
          leg: 'exit',
          roundId: this.currentRound.roundId,
          side: leg1.side,
          price: soldPrice,
          shares: leg1.shares,
          orderId: result.orderId,
          executionTimeMs: Date.now() - startTime,
        };
      } else {
        this.log(`❌ Leg1 exit failed: ${result.errorMsg}`);
        return {
          success: false,
          leg: 'exit',
          roundId: this.currentRound.roundId,
          error: result.errorMsg,
          executionTimeMs: Date.now() - startTime,
        };
      }
    } catch (error) {
      this.log(`❌ Leg1 exit error: ${error instanceof Error ? error.message : String(error)}`);
      return {
        success: false,
        leg: 'exit',
        roundId: this.currentRound.roundId,
        error: error instanceof Error ? error.message : String(error),
        executionTimeMs: Date.now() - startTime,
      };
    }
  }

  // ===== Private: Signal Detection =====

  private detectSignal(): DipArbSignal | null {
    if (!this.currentRound || !this.market) return null;

    // FIX #5: Skip Leg1 detection during warm-up period
    // Price history needs slidingWindowMs worth of data before we can detect dips
    if (!this.isWarmedUp) {
      const warmupElapsed = Date.now() - this.warmupStartTime;
      if (warmupElapsed >= this.config.slidingWindowMs) {
        this.isWarmedUp = true;
        this.log(`✅ Warm-up complete (${this.config.slidingWindowMs}ms), dip detection enabled`);
      } else if (this.currentRound.phase === 'waiting') {
        // Only skip Leg1 detection during warm-up, Leg2 can proceed
        return null;
      }
    }

    // Check based on current phase
    if (this.currentRound.phase === 'waiting') {
      return this.detectLeg1Signal();
    } else if (this.currentRound.phase === 'leg1_filled') {
      return this.detectLeg2Signal();
    }

    return null;
  }

  private detectLeg1Signal(): DipArbLeg1Signal | null {
    if (!this.currentRound || !this.market) return null;

    // Check if within trading window (轮次开始后的交易窗口)
    const elapsed = (Date.now() - this.currentRound.startTime) / 60000;
    if (elapsed > this.config.windowMinutes) {
      return null;
    }

    const upPrice = this.upAsks[0]?.price ?? 1;
    const downPrice = this.downAsks[0]?.price ?? 1;
    const { openPrices } = this.currentRound;

    // Skip if no valid prices
    if (upPrice >= 1 || downPrice >= 1 || openPrices.up <= 0 || openPrices.down <= 0) {
      return null;
    }

    // ========================================
    // Pattern 1: Instant Dip Detection (核心策略)
    // ========================================
    // 检测 slidingWindowMs (默认 3 秒) 内的瞬时暴跌
    // 这是策略的核心！我们捕捉的是"情绪性暴跌"，不是趋势

    const upPriceAgo = this.getPriceFromHistory('UP', this.config.slidingWindowMs);
    const downPriceAgo = this.getPriceFromHistory('DOWN', this.config.slidingWindowMs);

    // UP instant dip: 3秒内暴跌 >= dipThreshold
    if (upPriceAgo !== null && upPriceAgo > 0) {
      const upInstantDrop = (upPriceAgo - upPrice) / upPriceAgo;
      if (upInstantDrop >= this.config.dipThreshold) {
        if (this.config.debug) {
          this.log(`⚡ Instant DIP detected! UP: ${upPriceAgo.toFixed(4)} → ${upPrice.toFixed(4)} = -${(upInstantDrop * 100).toFixed(1)}% in ${this.config.slidingWindowMs}ms`);
        }
        const signal = this.createLeg1Signal('UP', upPrice, downPrice, 'dip', upInstantDrop, upPriceAgo);
        if (signal && this.validateSignalProfitability(signal)) {
          return signal;
        }
      }
    }

    // DOWN instant dip: 3秒内暴跌 >= dipThreshold
    if (downPriceAgo !== null && downPriceAgo > 0) {
      const downInstantDrop = (downPriceAgo - downPrice) / downPriceAgo;
      if (downInstantDrop >= this.config.dipThreshold) {
        if (this.config.debug) {
          this.log(`⚡ Instant DIP detected! DOWN: ${downPriceAgo.toFixed(4)} → ${downPrice.toFixed(4)} = -${(downInstantDrop * 100).toFixed(1)}% in ${this.config.slidingWindowMs}ms`);
        }
        const signal = this.createLeg1Signal('DOWN', downPrice, upPrice, 'dip', downInstantDrop, downPriceAgo);
        if (signal && this.validateSignalProfitability(signal)) {
          return signal;
        }
      }
    }

    // ========================================
    // Pattern 2: Surge Detection (if enabled)
    // ========================================
    // 暴涨检测：当 token 价格暴涨时，买入对手 token
    if (this.config.enableSurge && upPriceAgo !== null && downPriceAgo !== null) {
      // UP surged in sliding window, buy DOWN
      if (upPriceAgo > 0) {
        const upSurge = (upPrice - upPriceAgo) / upPriceAgo;
        if (upSurge >= this.config.surgeThreshold) {
          if (this.config.debug) {
            this.log(`⚡ Instant SURGE detected! UP: ${upPriceAgo.toFixed(4)} → ${upPrice.toFixed(4)} = +${(upSurge * 100).toFixed(1)}% in ${this.config.slidingWindowMs}ms`);
          }
          // 买入 DOWN，参考价格是 DOWN 的历史价格
          const signal = this.createLeg1Signal('DOWN', downPrice, upPrice, 'surge', upSurge, downPriceAgo);
          if (signal && this.validateSignalProfitability(signal)) {
            return signal;
          }
        }
      }

      // DOWN surged in sliding window, buy UP
      if (downPriceAgo > 0) {
        const downSurge = (downPrice - downPriceAgo) / downPriceAgo;
        if (downSurge >= this.config.surgeThreshold) {
          if (this.config.debug) {
            this.log(`⚡ Instant SURGE detected! DOWN: ${downPriceAgo.toFixed(4)} → ${downPrice.toFixed(4)} = +${(downSurge * 100).toFixed(1)}% in ${this.config.slidingWindowMs}ms`);
          }
          // 买入 UP，参考价格是 UP 的历史价格
          const signal = this.createLeg1Signal('UP', upPrice, downPrice, 'surge', downSurge, upPriceAgo);
          if (signal && this.validateSignalProfitability(signal)) {
            return signal;
          }
        }
      }
    }

    // ========================================
    // Pattern 3: Mispricing Detection
    // ========================================
    // 定价偏差：基于底层资产价格估计胜率，检测错误定价
    if (this.currentRound.priceToBeat > 0 && this.currentUnderlyingPrice > 0) {
      const estimatedWinRate = estimateUpWinRate(this.currentUnderlyingPrice, this.currentRound.priceToBeat);
      const upMispricing = detectMispricing(upPrice, estimatedWinRate);
      const downMispricing = detectMispricing(downPrice, 1 - estimatedWinRate);

      // UP is underpriced
      if (upMispricing >= this.config.dipThreshold) {
        const signal = this.createLeg1Signal('UP', upPrice, downPrice, 'mispricing', upMispricing);
        if (signal && this.validateSignalProfitability(signal)) {
          return signal;
        }
      }

      // DOWN is underpriced
      if (downMispricing >= this.config.dipThreshold) {
        const signal = this.createLeg1Signal('DOWN', downPrice, upPrice, 'mispricing', downMispricing);
        if (signal && this.validateSignalProfitability(signal)) {
          return signal;
        }
      }
    }

    return null;
  }

  private createLeg1Signal(
    side: DipArbSide,
    price: number,
    oppositeAsk: number,
    source: 'dip' | 'surge' | 'mispricing',
    dropPercent: number,
    referencePrice?: number  // 用于 dip/surge: 滑动窗口前的价格
  ): DipArbLeg1Signal | null {
    if (!this.currentRound || !this.market) return null;

    const targetPrice = price * (1 + this.config.maxSlippage);
    const estimatedTotalCost = targetPrice + oppositeAsk;
    
    // Calculate fee-adjusted profit rate for accurate signal evaluation
    const grossProfitRate = calculateDipArbProfitRate(estimatedTotalCost);
    const feeRate = this.config.takerFeeRate ?? DIP_ARB_CRYPTO_TAKER_FEE;
    const estimatedProfitRate = this.config.useFeeAdjustedProfit
      ? calculateDipArbNetProfitRate(estimatedTotalCost, feeRate)
      : grossProfitRate;

    // openPrice: 对于 dip/surge 信号，使用滑动窗口参考价格；否则使用轮次开盘价
    const openPrice = referencePrice ??
      (side === 'UP' ? this.currentRound.openPrices.up : this.currentRound.openPrices.down);

    const signal: DipArbLeg1Signal = {
      type: 'leg1',
      roundId: this.currentRound.roundId,
      dipSide: side,
      currentPrice: price,
      openPrice,  // 参考价格（3秒前的价格或轮次开盘价）
      dropPercent,
      targetPrice,
      shares: this.config.shares,
      tokenId: side === 'UP' ? this.market.upTokenId : this.market.downTokenId,
      oppositeAsk,
      estimatedTotalCost,
      estimatedProfitRate,
      source,
    };

    // Add BTC info if available
    if (this.currentRound.priceToBeat > 0 && this.currentUnderlyingPrice > 0) {
      const btcChangePercent = ((this.currentUnderlyingPrice - this.currentRound.priceToBeat) / this.currentRound.priceToBeat) * 100;
      signal.btcInfo = {
        btcPrice: this.currentUnderlyingPrice,
        priceToBeat: this.currentRound.priceToBeat,
        btcChangePercent,
        estimatedWinRate: estimateUpWinRate(this.currentUnderlyingPrice, this.currentRound.priceToBeat),
      };
    }

    return signal;
  }

  private detectLeg2Signal(): DipArbLeg2Signal | null {
    if (!this.currentRound || !this.market || !this.currentRound.leg1) return null;

    const leg1 = this.currentRound.leg1;
    const hedgeSide: DipArbSide = leg1.side === 'UP' ? 'DOWN' : 'UP';
    const currentPrice = hedgeSide === 'UP' ? (this.upAsks[0]?.price ?? 1) : (this.downAsks[0]?.price ?? 1);

    if (currentPrice >= 1) return null;

    const targetPrice = currentPrice * (1 + this.config.maxSlippage);
    const totalCost = leg1.price + targetPrice;

    // Check if profitable - 只用 sumTarget 控制
    // FIX #8: Near timeout, accept degraded Leg2 to mitigate Leg1 loss
    const leg2TimeoutSeconds = this.getLeg2TimeoutSeconds();
    const timeSinceLeg1 = (Date.now() - leg1.timestamp) / 1000;
    const timeUntilTimeout = leg2TimeoutSeconds - timeSinceLeg1;
    const DEGRADED_WINDOW_SECONDS = 30; // Last 30 seconds before timeout
    const DEGRADED_TOLERANCE = 0.02;    // Accept 2% worse than sumTarget

    // Calculate effective sumTarget (may be degraded near timeout)
    let effectiveSumTarget = this.config.sumTarget;
    if (timeUntilTimeout < DEGRADED_WINDOW_SECONDS && timeUntilTimeout > 0) {
      // Linear degradation: worse trades accepted as timeout approaches
      const urgency = (DEGRADED_WINDOW_SECONDS - timeUntilTimeout) / DEGRADED_WINDOW_SECONDS;
      effectiveSumTarget = this.config.sumTarget * (1 + DEGRADED_TOLERANCE * urgency);
      if (this.config.debug && Date.now() % 5000 < 100) {
        this.log(`⚠️ Degraded mode: ${timeUntilTimeout.toFixed(0)}s until timeout, accepting up to ${effectiveSumTarget.toFixed(4)}`);
      }
    }

    if (totalCost > effectiveSumTarget) {
      // 每 5 秒输出一次等待日志，避免刷屏
      if (this.config.debug && Date.now() % 5000 < 100) {
        const grossProfitRate = calculateDipArbProfitRate(totalCost);
        const feeRate = this.config.takerFeeRate ?? DIP_ARB_CRYPTO_TAKER_FEE;
        const netProfitRate = calculateDipArbNetProfitRate(totalCost, feeRate);
        this.log(`⏳ Waiting Leg2: ${hedgeSide} @ ${currentPrice.toFixed(4)}, cost ${totalCost.toFixed(4)} > ${effectiveSumTarget.toFixed(4)}, gross ${(grossProfitRate * 100).toFixed(1)}%, net ${(netProfitRate * 100).toFixed(1)}%`);
      }
      return null;
    }

    // Calculate profit rates
    const grossProfitRate = calculateDipArbProfitRate(totalCost);
    const feeRate = this.config.takerFeeRate ?? DIP_ARB_CRYPTO_TAKER_FEE;
    const expectedProfitRate = this.config.useFeeAdjustedProfit
      ? calculateDipArbNetProfitRate(totalCost, feeRate)
      : grossProfitRate;

    // ⚡ CRITICAL FIX: Reject Leg2 signals with negative net profit
    // This prevents hedging at a guaranteed loss (the main cause of consecutive losses)
    const minProfitRate = this.config.minProfitRate ?? 0;
    if (expectedProfitRate < minProfitRate) {
      if (this.config.debug && Date.now() % 5000 < 100) {
        this.log(`❌ Leg2 rejected: net ${(expectedProfitRate * 100).toFixed(2)}% < min ${(minProfitRate * 100).toFixed(2)}%`);
      }
      return null;
    }

    if (this.config.debug) {
      this.log(`✅ Leg2 signal found! ${hedgeSide} @ ${currentPrice.toFixed(4)}, totalCost ${totalCost.toFixed(4)}, gross ${(grossProfitRate * 100).toFixed(2)}%, net ${(expectedProfitRate * 100).toFixed(2)}%`);
    }

    // ✅ FIX: Use leg1.shares instead of config.shares to ensure balanced hedge
    // This is critical - Leg2 must buy exactly the same shares as Leg1 to create a perfect hedge
    return {
      type: 'leg2',
      roundId: this.currentRound.roundId,
      hedgeSide,
      leg1,
      currentPrice,
      targetPrice,
      totalCost,
      expectedProfitRate,
      shares: leg1.shares,  // Must match Leg1 to ensure balanced hedge
      tokenId: hedgeSide === 'UP' ? this.market.upTokenId : this.market.downTokenId,
    };
  }

  private validateSignalProfitability(signal: DipArbLeg1Signal): boolean {
    // Leg1 验证：只检查跌幅是否足够大
    // 不在 Leg1 阶段检查 sumTarget，因为：
    // 1. Leg1 的目的是抄底，买入暴跌的一侧
    // 2. Leg2 会等待对侧价格下降后再买入
    // 3. sumTarget 应该在 Leg2 阶段检查

    // 只做基本验证：确保价格合理
    if (signal.currentPrice <= 0 || signal.currentPrice >= 1) {
      if (this.config.debug) {
        this.log(`❌ Signal rejected: invalid price ${signal.currentPrice.toFixed(4)}`);
      }
      return false;
    }

    // ========================================
    // CRITICAL: Market Asymmetry Check
    // ========================================
    // Prevent trading in nearly-resolved markets where one side dominates
    // When UP > 75% (or DOWN > 75%), the market has essentially decided
    // DipArb in these scenarios is pure gambling, not arbitrage
    const maxAsymmetry = this.config.maxMarketAsymmetry ?? 0.75;
    const upPrice = this.upAsks[0]?.price ?? 0.5;
    const downPrice = this.downAsks[0]?.price ?? 0.5;

    if (upPrice > maxAsymmetry || downPrice > maxAsymmetry) {
      this.log(`❌ Signal rejected: market too asymmetric (UP=${(upPrice * 100).toFixed(0)}%, DOWN=${(downPrice * 100).toFixed(0)}%) - wait for rotation`);
      return false;
    }

    // Minimum Dip Side Price Check
    // Prevent buying nearly-resolved tokens (e.g., $0.005) which are lottery tickets
    // When a token is at $0.005, the market has essentially resolved
    const minPrice = this.config.minDipSidePrice ?? 0.10;
    if (signal.currentPrice < minPrice) {
      if (this.config.debug) {
        this.log(`❌ Signal rejected: price ${signal.currentPrice.toFixed(4)} < min ${minPrice} (nearly resolved, lottery ticket)`);
      }
      return false;
    }

    // New Safety Check: Max Leg1 Price
    // Prevent buying dips on high-priced assets (falling knives) where upside is capped
    // Default cap is 0.75, meaning we won't buy a dip if the price is > 0.75
    const maxPrice = this.config.maxLeg1Price ?? 0.75;
    if (signal.currentPrice > maxPrice) {
      if (this.config.debug) {
        this.log(`❌ Signal rejected: price ${signal.currentPrice.toFixed(4)} > max allowed ${maxPrice} (risk/reward poor)`);
      }
      return false;
    }

    // 确保跌幅达到阈值（这个已经在 detectLeg1Signal 中检查过，这里再确认一下）
    if (signal.dropPercent < this.config.dipThreshold) {
      if (this.config.debug) {
        this.log(`❌ Signal rejected: drop ${(signal.dropPercent * 100).toFixed(1)}% < threshold ${(this.config.dipThreshold * 100).toFixed(1)}%`);
      }
      return false;
    }

    // ========================================
    // Opposite Side Liquidity Check (Leg2 Viability)
    // ========================================
    // Check that the opposite side has enough liquidity for Leg2
    // If opposite side is illiquid, Leg2 will be expensive or impossible
    const oppositeSideAsks = signal.dipSide === 'UP' ? this.downAsks : this.upAsks;

    // Check depth (total shares available)
    const minDepth = this.config.minOppositeSideDepth ?? 100;
    const totalOppDepth = oppositeSideAsks.reduce((sum, level) => sum + level.size, 0);
    if (totalOppDepth < minDepth) {
      if (this.config.debug) {
        this.log(`❌ Signal rejected: opposite side depth ${totalOppDepth.toFixed(0)} < min ${minDepth} (Leg2 would be difficult)`);
      }
      return false;
    }

    // Check spread (if we have multiple price levels)
    // Spread = (second best ask - best ask) / best ask
    // Wide spread indicates poor liquidity and potential for worse Leg2 execution
    if (oppositeSideAsks.length >= 2) {
      const bestAsk = oppositeSideAsks[0].price;
      const secondAsk = oppositeSideAsks[1].price;
      const spread = (secondAsk - bestAsk) / bestAsk;
      const maxSpread = this.config.maxOppositeSideSpread ?? 0.05;

      if (spread > maxSpread) {
        if (this.config.debug) {
          this.log(`❌ Signal rejected: opposite side spread ${(spread * 100).toFixed(1)}% > max ${(maxSpread * 100).toFixed(1)}% (Leg2 would be expensive)`);
        }
        // FIX #7: Emit signalRejected event
        this.emit('signalRejected', {
          reason: 'oppositeSideSpread',
          details: `spread ${(spread * 100).toFixed(1)}% > max ${(maxSpread * 100).toFixed(1)}%`,
          signal,
          timestamp: Date.now(),
        });
        return false;
      }
    }

    // ========================================
    // FIX #1: Leg2 Feasibility Check (Enhanced with maxRequiredLeg2Drop)
    // ========================================
    // Calculate how much the opposite side needs to drop for Leg2 to be profitable
    // This prevents entering positions in polarized markets where Leg2 is unlikely
    //
    // Example: If buying UP at $0.21, DOWN is at $0.79, sumTarget is $0.88:
    // - Max Leg2 price: $0.88 - $0.21 = $0.67
    // - Required drop: $0.79 - $0.67 = $0.12 (15.2% of $0.79)
    // - If maxRequiredLeg2Drop is 0.15 (15%), this entry is rejected
    const bestOppositePrice = oppositeSideAsks[0]?.price ?? 1;
    const maxLeg2Price = this.config.sumTarget - signal.targetPrice;
    const requiredDrop = bestOppositePrice - maxLeg2Price;
    const estimatedTotalCost = signal.targetPrice + bestOppositePrice;

    // If requiredDrop > 0, the opposite side needs to drop for Leg2 to work
    if (requiredDrop > 0) {
      const requiredDropPercent = requiredDrop / bestOppositePrice;
      const maxDropThreshold = this.config.maxRequiredLeg2Drop ?? 0.15;

      if (requiredDropPercent > maxDropThreshold) {
        // Spread is too wide - Leg2 is mathematically unlikely
        this.log(`❌ Signal rejected: leg2 needs ${(requiredDropPercent * 100).toFixed(1)}% drop (max ${(maxDropThreshold * 100).toFixed(0)}%) - spread too wide`);
        this.log(`   Leg1: ${signal.targetPrice.toFixed(4)}, Opposite: ${bestOppositePrice.toFixed(4)}, maxLeg2: ${maxLeg2Price.toFixed(4)}`);
        // FIX #7: Emit signalRejected event
        this.emit('signalRejected', {
          reason: 'leg2SpreadTooWide',
          details: `requires ${(requiredDropPercent * 100).toFixed(1)}% drop > max ${(maxDropThreshold * 100).toFixed(0)}%`,
          signal,
          timestamp: Date.now(),
        });
        return false;
      }

      // Drop required but within threshold - allow entry (market may move)
      if (this.config.debug) {
        this.log(`⚠️ Leg2 requires ${(requiredDropPercent * 100).toFixed(1)}% drop (within ${(maxDropThreshold * 100).toFixed(0)}% threshold) - proceeding`);
      }
    }

    // ========================================
    // FIX #2: minProfitRate Validation
    // ========================================
    // Verify signal meets minimum profit rate requirement
    const minProfitRate = this.config.minProfitRate ?? 0.02;
    if (signal.estimatedProfitRate < minProfitRate) {
      if (this.config.debug) {
        this.log(`❌ Signal rejected: profit ${(signal.estimatedProfitRate * 100).toFixed(2)}% < min ${(minProfitRate * 100).toFixed(0)}%`);
      }
      // FIX #7: Emit signalRejected event
      this.emit('signalRejected', {
        reason: 'lowProfitRate',
        details: `profit ${(signal.estimatedProfitRate * 100).toFixed(2)}% < min ${(minProfitRate * 100).toFixed(0)}%`,
        signal,
        timestamp: Date.now(),
      });
      return false;
    }

    if (this.config.debug) {
      this.log(`✅ Leg1 signal validated: ${signal.dipSide} @ ${signal.currentPrice.toFixed(4)}, drop ${(signal.dropPercent * 100).toFixed(1)}%`);
      this.log(`   Opposite side: depth=${totalOppDepth.toFixed(0)} shares, best=${bestOppositePrice.toFixed(4)}`);
      const dropInfo = requiredDrop > 0 ? `, leg2 needs ${(requiredDrop / bestOppositePrice * 100).toFixed(1)}% drop` : ', leg2 already profitable';
      this.log(`   Estimated total: ${estimatedTotalCost.toFixed(4)}, profit: ${(signal.estimatedProfitRate * 100).toFixed(2)}%${dropInfo}`);
    }

    return true;
  }

  // ===== Private: Signal Handling =====

  private async handleSignal(signal: DipArbSignal): Promise<void> {
    // Check if we can execute before emitting signal
    // This prevents logging signals that won't be executed
    if (!this.config.autoExecute) {
      // Manual mode: always emit signal for user to decide
      this.stats.signalsDetected++;
      this.emit('signal', signal);

      if (this.config.debug) {
        if (isDipArbLeg1Signal(signal)) {
          this.log(`Signal: Leg1 ${signal.dipSide} @ ${signal.currentPrice.toFixed(4)} (${signal.source})`);
        } else {
          this.log(`Signal: Leg2 ${signal.hedgeSide} @ ${signal.currentPrice.toFixed(4)}`);
        }
      }
      return;
    }

    // Auto-execute mode: only emit and log if we will actually execute
    // FIX #3: Only block NEW Leg1 signals during execution, allow Leg2 to proceed
    // This enables Leg2 to execute right after Leg1 completes (or even during in some cases)
    const isLeg1Signal = isDipArbLeg1Signal(signal);

    if (this.isExecuting && isLeg1Signal) {
      // Block new Leg1 signals while already executing (prevents duplicate positions)
      if (this.config.debug) {
        this.log(`Signal skipped (executing): Leg1 blocked during execution`);
      }
      return;
    }

    const now = Date.now();
    // FIX #4 part: Leg2 signals skip cooldown check for faster execution
    const shouldCheckCooldown = isLeg1Signal; // Only check cooldown for Leg1
    if (shouldCheckCooldown && now - this.lastExecutionTime < this.config.executionCooldown) {
      // Skip - within cooldown period (Leg1 only)
      if (this.config.debug) {
        const remaining = this.config.executionCooldown - (now - this.lastExecutionTime);
        this.log(`Signal skipped (cooldown ${remaining}ms): Leg1`);
      }
      return;
    }

    // CRITICAL: Set isExecuting immediately to prevent duplicate signals from being processed
    // This must happen before any async operations or emit() calls
    this.isExecuting = true;

    // Will execute - now emit signal and log
    this.stats.signalsDetected++;
    this.emit('signal', signal);

    if (this.config.debug) {
      const signalType = isDipArbLeg1Signal(signal) ? 'Leg1' : 'Leg2';

      // Log orderbook context before execution (last 5 seconds of data)
      this.logOrderbookContext(`${signalType} Signal`);

      if (isDipArbLeg1Signal(signal)) {
        this.log(`🎯 Signal: Leg1 ${signal.dipSide} @ ${signal.currentPrice.toFixed(4)} (${signal.source})`);
        this.log(`   Target: ${signal.targetPrice.toFixed(4)} | Opposite: ${signal.oppositeAsk.toFixed(4)} | Est.Cost: ${signal.estimatedTotalCost.toFixed(4)}`);
      } else {
        this.log(`🎯 Signal: Leg2 ${signal.hedgeSide} @ ${signal.currentPrice.toFixed(4)}`);
        this.log(`   Target: ${signal.targetPrice.toFixed(4)} | TotalCost: ${signal.totalCost.toFixed(4)} | Profit: ${(signal.expectedProfitRate * 100).toFixed(2)}%`);
      }
    }

    // Execute
    let result: DipArbExecutionResult;

    if (isDipArbLeg1Signal(signal)) {
      result = await this.executeLeg1(signal);
    } else {
      result = await this.executeLeg2(signal);
    }

    this.emit('execution', result);
  }

  // ===== Public API: Auto-Rotate =====

  /**
   * Configure and enable auto-rotate
   *
   * Auto-rotate 会自动：
   * 1. 监控当前市场到期时间
   * 2. 在市场结束前预加载下一个市场
   * 3. 市场结束时自动结算（redeem 或 sell）
   * 4. 无缝切换到下一个 15m 市场
   *
   * @example
   * ```typescript
   * sdk.dipArb.enableAutoRotate({
   *   underlyings: ['BTC', 'ETH'],
   *   duration: '15m',
   *   autoSettle: true,
   *   settleStrategy: 'redeem',
   * });
   * ```
   */
  enableAutoRotate(config: Partial<DipArbAutoRotateConfig> = {}): void {
    this.autoRotateConfig = {
      ...this.autoRotateConfig,
      ...config,
      enabled: true,
    };

    this.log(`Auto-rotate enabled: ${JSON.stringify(this.autoRotateConfig)}`);
    this.startRotateCheck();

    // Start background redemption check if using redeem strategy
    if (this.autoRotateConfig.settleStrategy === 'redeem') {
      this.startRedeemCheck();

      // ✅ FIX: Scan for existing redeemable positions at startup
      this.scanAndQueueRedeemablePositions().catch(err => {
        this.log(`Warning: Failed to scan redeemable positions: ${err instanceof Error ? err.message : String(err)}`);
      });
    }
  }

  /**
   * ✅ FIX: Scan for existing redeemable positions and add them to the queue
   *
   * This is called when auto-rotate is enabled to recover any positions
   * from previous sessions that can be redeemed.
   */
  private async scanAndQueueRedeemablePositions(): Promise<void> {
    if (!this.ctf) {
      this.log('Cannot scan redeemable positions: CTF client not available');
      return;
    }
    
    // Skip in paper mode - no real positions to scan
    if (this.config.paperMode) {
      this.log(`📝 [PAPER] Skipping redeemable position scan (paper mode)`);
      return;
    }

    try {
      // Scan for recently ended markets of the configured underlyings
      const now = Date.now();
      const markets = await this.scanUpcomingMarkets({
        coin: this.autoRotateConfig.underlyings.length === 1
          ? this.autoRotateConfig.underlyings[0]
          : 'all',
        duration: this.autoRotateConfig.duration,
        minMinutesUntilEnd: -60,  // Include markets that ended up to 60 minutes ago
        maxMinutesUntilEnd: 0,    // Only ended markets
        limit: 20,
      });

      this.log(`🔍 Scanning ${markets.length} recently ended markets for redeemable positions...`);

      let foundCount = 0;
      for (const market of markets) {
        // Skip current market
        if (market.conditionId === this.market?.conditionId) continue;

        // Check if we have any position in this market
        try {
          const tokenIds = {
            yesTokenId: market.upTokenId,
            noTokenId: market.downTokenId,
          };

          const balances = await this.ctf.getPositionBalanceByTokenIds(
            market.conditionId,
            tokenIds
          );

          const upBalance = parseFloat(balances.yesBalance);
          const downBalance = parseFloat(balances.noBalance);

          // If we have any tokens, check if market is resolved
          if (upBalance > 0.01 || downBalance > 0.01) {
            const resolution = await this.ctf.getMarketResolution(market.conditionId);

            if (resolution.isResolved) {
              // Check if we have winning tokens
              const winningBalance = resolution.winningOutcome === 'YES' ? upBalance : downBalance;

              if (winningBalance > 0.01) {
                // Add to pending redemption queue
                const pending: DipArbPendingRedemption = {
                  market,
                  round: {
                    roundId: `recovery-${market.slug}`,
                    priceToBeat: 0,
                    openPrices: { up: 0, down: 0 },
                    startTime: 0,
                    endTime: market.endTime.getTime(),
                    phase: 'completed',
                  },
                  marketEndTime: market.endTime.getTime(),
                  addedAt: now,
                  retryCount: 0,
                };

                this.pendingRedemptions.push(pending);
                foundCount++;
                this.log(`📌 Found redeemable: ${market.slug} | ${resolution.winningOutcome} won | Balance: ${winningBalance.toFixed(2)}`);
              }
            } else if (upBalance > 0.01 && downBalance > 0.01) {
              // Market not resolved but we have pairs - can merge
              const pairsToMerge = Math.min(upBalance, downBalance);
              this.log(`📌 Found mergeable pairs in ${market.slug}: ${pairsToMerge.toFixed(2)}`);

              // Try to merge immediately
              try {
                const result = await this.ctf.mergeByTokenIds(
                  market.conditionId,
                  tokenIds,
                  pairsToMerge.toString()
                );
                if (result.success) {
                  this.log(`✅ Merged ${pairsToMerge.toFixed(2)} pairs from ${market.slug}`);
                }
              } catch (mergeErr) {
                this.log(`⚠️ Failed to merge ${market.slug}: ${mergeErr instanceof Error ? mergeErr.message : String(mergeErr)}`);
              }
            }
          }
        } catch (err) {
          // Skip this market on error
        }
      }

      if (foundCount > 0) {
        this.log(`✅ Found ${foundCount} redeemable positions, added to queue`);
      } else {
        this.log('No redeemable positions found');
      }
    } catch (error) {
      this.log(`Error scanning redeemable positions: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Disable auto-rotate
   */
  disableAutoRotate(): void {
    this.autoRotateConfig.enabled = false;
    this.stopRotateCheck();
    this.stopRedeemCheck();
    this.log('Auto-rotate disabled');

    // Warn if there are pending redemptions
    if (this.pendingRedemptions.length > 0) {
      this.log(`Warning: ${this.pendingRedemptions.length} pending redemptions will not be processed`);
    }
  }

  /**
   * Get auto-rotate configuration
   */
  getAutoRotateConfig(): Required<DipArbAutoRotateConfig> {
    return { ...this.autoRotateConfig };
  }

  /**
   * Manually settle current position
   *
   * 结算策略：
   * - 'redeem': 等待市场结算后 redeem（需要等待结算完成）
   * - 'sell': 直接卖出 token（更快但可能有滑点）
   */
  async settle(strategy: 'redeem' | 'sell' = 'redeem'): Promise<DipArbSettleResult> {
    const startTime = Date.now();

    if (!this.market || !this.currentRound) {
      return {
        success: false,
        strategy,
        error: 'No active market or round',
        executionTimeMs: Date.now() - startTime,
      };
    }

    try {
      if (strategy === 'redeem') {
        return await this.settleByRedeem();
      } else {
        return await this.settleBySell();
      }
    } catch (error) {
      return {
        success: false,
        strategy,
        error: error instanceof Error ? error.message : String(error),
        executionTimeMs: Date.now() - startTime,
      };
    }
  }

  /**
   * Manually rotate to next market
   */
  async rotateToNextMarket(): Promise<DipArbMarketConfig | null> {
    if (!this.autoRotateConfig.enabled) {
      this.log('Auto-rotate not enabled');
      return null;
    }

    // Find next market
    const nextMarket = await this.findNextMarket();
    if (!nextMarket) {
      this.log('No suitable next market found');
      return null;
    }

    // Stop current monitoring
    await this.stop();

    // Start new market
    await this.start(nextMarket);

    const event: DipArbRotateEvent = {
      previousMarket: this.market?.conditionId,
      newMarket: nextMarket.conditionId,
      reason: 'manual',
      timestamp: Date.now(),
    };
    this.emit('rotate', event);

    return nextMarket;
  }

  // ===== Private: Auto-Rotate Implementation =====

  private startRotateCheck(): void {
    if (this.rotateCheckInterval) {
      clearInterval(this.rotateCheckInterval);
    }

    this.log('🔄 Starting rotation check interval (30s)');

    // Check every 30 seconds - with error handling to prevent silent failures
    this.rotateCheckInterval = setInterval(() => {
      this.checkRotation().catch((err) => {
        const errorMessage = err instanceof Error ? err.message : String(err);
        this.log(`❌ Rotation check failed: ${errorMessage}`);
        this.emit('error', { error: err, phase: 'rotation' });
      });
    }, 30000);

    // Also check immediately - with error handling
    this.checkRotation().catch((err) => {
      const errorMessage = err instanceof Error ? err.message : String(err);
      this.log(`❌ Initial rotation check failed: ${errorMessage}`);
      this.emit('error', { error: err, phase: 'rotation' });
    });
  }

  private stopRotateCheck(): void {
    if (this.rotateCheckInterval) {
      clearInterval(this.rotateCheckInterval);
      this.rotateCheckInterval = null;
    }
  }

  // ===== Private: Pending Redemption Processing =====

  private startRedeemCheck(): void {
    if (this.redeemCheckInterval) {
      clearInterval(this.redeemCheckInterval);
    }

    const intervalMs = (this.autoRotateConfig.redeemRetryIntervalSeconds || 30) * 1000;

    // Check every 30 seconds (configurable)
    this.redeemCheckInterval = setInterval(() => {
      this.processPendingRedemptions();
    }, intervalMs);

    this.log(`Redeem check started (interval: ${intervalMs / 1000}s)`);
  }

  private stopRedeemCheck(): void {
    if (this.redeemCheckInterval) {
      clearInterval(this.redeemCheckInterval);
      this.redeemCheckInterval = null;
    }
  }

  /**
   * Add a position to pending redemption queue
   */
  private addPendingRedemption(market: DipArbMarketConfig, round: DipArbRoundState): void {
    const pending: DipArbPendingRedemption = {
      market,
      round,
      marketEndTime: market.endTime.getTime(),
      addedAt: Date.now(),
      retryCount: 0,
    };
    this.pendingRedemptions.push(pending);
    this.log(`Added pending redemption: ${market.slug} (queue size: ${this.pendingRedemptions.length})`);
  }

  /**
   * Process all pending redemptions
   * Called periodically by redeemCheckInterval
   */
  private async processPendingRedemptions(): Promise<void> {
    if (this.pendingRedemptions.length === 0) {
      return;
    }

    const now = Date.now();
    const waitMs = (this.autoRotateConfig.redeemWaitMinutes || 5) * 60 * 1000;

    for (let i = this.pendingRedemptions.length - 1; i >= 0; i--) {
      const pending = this.pendingRedemptions[i];
      const timeSinceEnd = now - pending.marketEndTime;

      // Skip if not enough time has passed since market end
      if (timeSinceEnd < waitMs) {
        const waitLeft = Math.round((waitMs - timeSinceEnd) / 1000);
        if (this.config.debug) {
          this.log(`Pending redemption ${pending.market.slug}: waiting ${waitLeft}s more for resolution`);
        }
        continue;
      }

      // Try to redeem
      pending.retryCount++;
      pending.lastRetryAt = now;

      // In paper mode, simulate immediate redemption
      if (this.config.paperMode) {
        const totalShares = (pending.round.leg1?.shares ?? 0) + (pending.round.leg2?.shares ?? 0);
        const amountReceived = totalShares; // $1 per merged pair
        const totalCost = (pending.round.leg1?.price ?? 0) * (pending.round.leg1?.shares ?? 0) +
                          (pending.round.leg2?.price ?? 0) * (pending.round.leg2?.shares ?? 0);

        // ✅ FIX: Calculate NET profit after fees
        const grossProfit = amountReceived - totalCost;
        const feeRate = this.config.takerFeeRate ?? 0.03;
        const totalFees = totalCost * feeRate * 2; // 6% total (3% per leg)
        const netProfit = grossProfit - totalFees;

        this.log(`📝 [PAPER] Redemption simulated: ${pending.market.slug} | Amount: $${amountReceived.toFixed(2)}`);
        this.log(`   💰 [PAPER] NET Profit: $${netProfit.toFixed(2)} (gross: $${grossProfit.toFixed(2)}, fees: $${totalFees.toFixed(2)})`);

        // Emit paper trade event
        this.emit('paperTrade', {
          type: 'settle',
          side: pending.round.leg1?.side ?? 'UP',
          shares: totalShares,
          price: 1,
          cost: amountReceived,
          profit: netProfit, // ✅ Use NET profit
          profitRate: totalCost > 0 ? netProfit / (totalCost + totalFees) : 0,
          expectedPrice: 1,                          // Settlement at $1 (no price uncertainty)
          slippagePercent: 0,                        // No slippage on settlement
          marketName: pending.market.name,
          conditionId: pending.market.conditionId,
          timestamp: Date.now(),
        });

        // ✅ FIX: Emit position closed event for backend persistence
        this.emit('openPositionClosed', {
          roundId: pending.round.roundId,
          status: 'closed',
          closeType: 'settle',
          netProfit: netProfit,
        });

        // Remove from queue
        this.pendingRedemptions.splice(i, 1);

        const settleResult: DipArbSettleResult = {
          success: true,
          strategy: 'redeem',
          market: pending.market,
          txHash: `paper_redeem_${Date.now()}`,
          amountReceived,
          executionTimeMs: 0,
        };

        this.emit('settled', settleResult);
        this.stats.totalProfit += netProfit; // ✅ Use NET profit
        continue;
      }

      try {
        if (!this.ctf) {
          this.log(`Cannot redeem ${pending.market.slug}: CTF client not available`);
          continue;
        }

        // Check if market is resolved
        const resolution = await this.ctf.getMarketResolution(pending.market.conditionId);

        if (!resolution.isResolved) {
          this.log(`Pending redemption ${pending.market.slug}: market not yet resolved (retry ${pending.retryCount})`);

          // Give up after too many retries (10 minutes of trying)
          if (pending.retryCount > 20) {
            this.log(`Giving up on redemption ${pending.market.slug}: too many retries`);
            this.pendingRedemptions.splice(i, 1);
            this.emit('settled', {
              success: false,
              strategy: 'redeem',
              market: pending.market,
              error: 'Market not resolved after max retries',
              executionTimeMs: 0,
            } as DipArbSettleResult);
          }
          continue;
        }

        // Market is resolved, try to redeem using Polymarket token IDs
        this.log(`Redeeming ${pending.market.slug}...`);
        const tokenIds = {
          yesTokenId: pending.market.upTokenId,
          noTokenId: pending.market.downTokenId,
        };
        const result = await this.ctf.redeemByTokenIds(pending.market.conditionId, tokenIds);

        // Remove from queue
        this.pendingRedemptions.splice(i, 1);

        const settleResult: DipArbSettleResult = {
          success: result.success,
          strategy: 'redeem',
          market: pending.market,
          txHash: result.txHash,
          amountReceived: result.usdcReceived ? parseFloat(result.usdcReceived) : undefined,
          executionTimeMs: 0,
        };

        this.emit('settled', settleResult);
        this.log(`Redemption successful: ${pending.market.slug} | Amount: $${settleResult.amountReceived?.toFixed(2) || 'N/A'}`);

        // Update stats
        if (settleResult.amountReceived) {
          this.stats.totalProfit += settleResult.amountReceived;
        }
      } catch (error) {
        this.log(`Redemption error for ${pending.market.slug}: ${error instanceof Error ? error.message : String(error)}`);

        // Give up after too many retries
        if (pending.retryCount > 20) {
          this.log(`Giving up on redemption ${pending.market.slug}: error after max retries`);
          this.pendingRedemptions.splice(i, 1);
          this.emit('settled', {
            success: false,
            strategy: 'redeem',
            market: pending.market,
            error: error instanceof Error ? error.message : String(error),
            executionTimeMs: 0,
          } as DipArbSettleResult);
        }
      }
    }
  }

  /**
   * Get pending redemptions (for debugging/monitoring)
   */
  getPendingRedemptions(): DipArbPendingRedemption[] {
    return [...this.pendingRedemptions];
  }

  private async checkRotation(): Promise<void> {
    if (!this.autoRotateConfig.enabled || !this.market) {
      this.log(`🔄 checkRotation: skipped (enabled=${this.autoRotateConfig.enabled}, market=${!!this.market})`);
      return;
    }

    const now = Date.now();
    const endTime = this.market.endTime.getTime();
    const timeUntilEnd = endTime - now;
    const preloadMs = (this.autoRotateConfig.preloadMinutes || 2) * 60 * 1000;

    // Always log rotation status for debugging
    const timeLeftSec = Math.round(timeUntilEnd / 1000);
    const timeLeftMin = (timeLeftSec / 60).toFixed(1);
    this.log(`🔄 checkRotation: ${timeLeftMin}min until end, preload=${(preloadMs / 60000).toFixed(1)}min, nextMarket=${this.nextMarket?.slug || 'none'}`);

    // Preload next market when close to end - with error handling
    if (timeUntilEnd <= preloadMs && !this.nextMarket) {
      this.log('Preloading next market...');
      try {
        this.nextMarket = await this.findNextMarket();
        if (this.nextMarket) {
          this.log(`Next market ready: ${this.nextMarket.slug}`);
        } else {
          this.log('No next market found during preload');
        }
      } catch (preloadError) {
        const errorMessage = preloadError instanceof Error ? preloadError.message : String(preloadError);
        this.log(`⚠️ Preload failed: ${errorMessage}`);
        // Don't rethrow - we'll try again at rotation time
      }
    }

    // Market ended - settle and rotate
    if (timeUntilEnd <= 0) {
      this.log(`Market ended ${Math.round(-timeUntilEnd / 1000)}s ago, initiating rotation...`);

      // Settle if configured and has position - with error handling
      if (this.autoRotateConfig.autoSettle && this.currentRound?.leg1) {
        const strategy = this.autoRotateConfig.settleStrategy || 'redeem';

        if (strategy === 'redeem') {
          // For redeem strategy, add to pending queue (will be processed after 5 min wait)
          this.addPendingRedemption(this.market, this.currentRound);
          this.log(`Position added to pending redemption queue (will redeem after ${this.autoRotateConfig.redeemWaitMinutes || 5}min)`);
        } else {
          // For sell strategy, execute immediately
          try {
            const settleResult = await this.settle('sell');
            this.emit('settled', settleResult);
          } catch (settleError) {
            const errorMessage = settleError instanceof Error ? settleError.message : String(settleError);
            this.log(`⚠️ Settle failed during rotation: ${errorMessage}`);
            this.emit('error', { error: settleError, phase: 'settle' });
            // Continue with rotation despite settle failure
          }
        }
      }

      // Rotate to next market - with error handling
      const previousMarket = this.market;
      let newMarket = this.nextMarket;
      this.nextMarket = null;

      // If no preloaded market, try to find one now
      if (!newMarket) {
        this.log('No preloaded market, searching...');
        try {
          newMarket = await this.findNextMarket();
        } catch (findError) {
          const errorMessage = findError instanceof Error ? findError.message : String(findError);
          this.log(`❌ Failed to find next market: ${errorMessage}`);
          this.emit('error', { error: findError, phase: 'rotation' });
        }
      }

      if (newMarket) {
        // Stop current market - skip unsubscribe since market has ended
        try {
          await this.stop({ skipUnsubscribe: true, silent: true });
        } catch (stopError) {
          const errorMessage = stopError instanceof Error ? stopError.message : String(stopError);
          this.log(`⚠️ Stop during rotation failed: ${errorMessage}`);
          // Continue anyway - we need to start the new market
        }

        // Start new market
        try {
          await this.start(newMarket);

          // Restart the rotate check interval for the new market
          this.startRotateCheck();

          const event: DipArbRotateEvent = {
            previousMarket: previousMarket.conditionId,
            newMarket: newMarket.conditionId,
            reason: 'marketEnded',
            timestamp: Date.now(),
          };
          this.emit('rotate', event);
        } catch (startError) {
          const errorMessage = startError instanceof Error ? startError.message : String(startError);
          this.log(`❌ Start during rotation failed: ${errorMessage}`);
          this.emit('rotationFailed', {
            error: startError,
            previousMarket: previousMarket.conditionId,
            attemptedMarket: newMarket.conditionId,
          });
          // Don't leave in broken state - ensure we're stopped
          this.isRunning = false;
        }
      } else {
        this.log('No next market available, stopping...');
        // Market ended with no next market - skip unsubscribe but DO emit stopped
        try {
          await this.stop({ skipUnsubscribe: true });
        } catch (stopError) {
          const errorMessage = stopError instanceof Error ? stopError.message : String(stopError);
          this.log(`⚠️ Final stop failed: ${errorMessage}`);
          this.isRunning = false;
        }
      }
    }
  }

  private async findNextMarket(): Promise<DipArbMarketConfig | null> {
    // Use duration fallback chain if enabled
    const enableFallback = this.autoRotateConfig.enableFallback !== false;
    const durationPriority = this.autoRotateConfig.durationPriority || DURATION_FALLBACK_CHAIN;

    // Start from preferred duration (or current if fallback disabled)
    const startDuration = enableFallback ? this.preferredDuration : this.currentDuration;
    const startIndex = durationPriority.indexOf(startDuration);
    const durationsToTry = enableFallback
      ? durationPriority.slice(Math.max(0, startIndex))
      : [this.currentDuration];

    const triedDurations: DipArbDurationString[] = [];

    for (const duration of durationsToTry) {
      triedDurations.push(duration);
      const maxMinutes = this.getMaxMinutesForDuration(duration);

      const markets = await this.scanUpcomingMarkets({
        coin: this.autoRotateConfig.underlyings.length === 1
          ? this.autoRotateConfig.underlyings[0]
          : 'all',
        duration: duration,
        minMinutesUntilEnd: 3,
        maxMinutesUntilEnd: maxMinutes,
        limit: 10,
      });

      // Filter to configured underlyings
      const filtered = markets.filter(m =>
        this.autoRotateConfig.underlyings.includes(m.underlying as DipArbUnderlying)
      );

      // Exclude current market
      const available = filtered.filter(m =>
        m.conditionId !== this.market?.conditionId
      );

      if (available.length > 0) {
        // Use round-robin to prefer next coin in rotation sequence
        const nextMarket = this.selectNextMarketRoundRobin(available);
        const isFallback = duration !== this.preferredDuration;

        if (isFallback) {
          this.log(`⚠️ Rotating to ${duration} market (${this.preferredDuration} unavailable)`);
          this.currentDuration = duration;
          // Start upgrade check to look for higher-priority markets
          this.startUpgradeCheck();
        } else if (duration === this.preferredDuration && this.currentDuration !== this.preferredDuration) {
          // Upgrading back to preferred duration
          this.log(`🎯 Upgrading rotation back to ${duration} markets`);
          this.currentDuration = duration;
          this.stopUpgradeCheck();
        }

        return nextMarket;
      }

      this.log(`No ${duration} markets available, trying next duration...`);
    }

    // No markets found in any duration
    this.log(`❌ No markets found in any duration: ${triedDurations.join(', ')}`);
    this.emit('noMarketsAvailable', {
      triedDurations,
      willRetry: true,
      message: 'No markets available in any duration',
    });

    return null;
  }

  /**
   * Select next market using round-robin coin rotation
   * Prefers: current coin's next in sequence (BTC→ETH→SOL→XRP→BTC)
   * Falls back to first available if preferred coin unavailable
   */
  private selectNextMarketRoundRobin(markets: DipArbMarketConfig[]): DipArbMarketConfig {
    const rotationOrder: DipArbUnderlying[] = ['BTC', 'ETH', 'SOL', 'XRP'];
    const currentCoin = this.market?.underlying as DipArbUnderlying | undefined;

    // If no current market or only one option, return first
    if (!currentCoin || markets.length === 1) {
      return markets[0];
    }

    // Build rotation sequence starting from next coin after current
    const currentIndex = rotationOrder.indexOf(currentCoin);
    const nextCoins = [
      ...rotationOrder.slice(currentIndex + 1),  // Coins after current
      ...rotationOrder.slice(0, currentIndex + 1) // Wrap around including current
    ];

    // Find first available market matching next coin in sequence
    for (const coin of nextCoins) {
      const match = markets.find(m => m.underlying === coin);
      if (match) {
        if (match.underlying !== currentCoin) {
          this.log(`🔄 Round-robin: ${currentCoin} → ${coin}`);
        }
        this.lastRotatedCoin = coin;
        return match;
      }
    }

    // Fallback (shouldn't reach here if markets has items)
    return markets[0];
  }

  private async settleByRedeem(): Promise<DipArbSettleResult> {
    const startTime = Date.now();

    if (!this.ctf || !this.market) {
      return {
        success: false,
        strategy: 'redeem',
        error: 'CTF client or market not available',
        executionTimeMs: Date.now() - startTime,
      };
    }

    // In paper mode, simulate the redeem
    if (this.config.paperMode) {
      // Simulate successful redemption - assume market resolves in our favor
      // For paper trading, we assume UP+DOWN tokens merge to $1 each
      const totalShares = (this.currentRound?.leg1?.shares ?? 0) + (this.currentRound?.leg2?.shares ?? 0);
      const amountReceived = totalShares; // $1 per merged pair

      const totalCost = (this.currentRound?.leg1?.price ?? 0) * (this.currentRound?.leg1?.shares ?? 0) +
                        (this.currentRound?.leg2?.price ?? 0) * (this.currentRound?.leg2?.shares ?? 0);

      // ✅ FIX: Calculate NET profit after fees
      const grossProfit = amountReceived - totalCost;
      const feeRate = this.config.takerFeeRate ?? 0.03;
      const totalFees = totalCost * feeRate * 2; // 6% total (3% per leg)
      const netProfit = grossProfit - totalFees;

      this.log(`📝 [PAPER] Settle by redeem simulated: received ~$${amountReceived.toFixed(2)}`);
      this.log(`   💰 [PAPER] NET Profit: $${netProfit.toFixed(2)} (gross: $${grossProfit.toFixed(2)}, fees: $${totalFees.toFixed(2)})`);

      // Emit paper trade event
      this.emit('paperTrade', {
        type: 'settle',
        side: this.currentRound?.leg1?.side ?? 'UP',
        shares: totalShares,
        price: 1, // $1 redemption
        cost: amountReceived,
        profit: netProfit, // ✅ Use NET profit
        profitRate: totalCost > 0 ? netProfit / (totalCost + totalFees) : 0,
        expectedPrice: 1,                          // Settlement at $1 (no price uncertainty)
        slippagePercent: 0,                        // No slippage on settlement
        marketName: this.market.name,
        conditionId: this.market.conditionId,
        timestamp: Date.now(),
      });

      // ✅ FIX: Emit position closed event for backend persistence
      if (this.currentRound) {
        this.emit('openPositionClosed', {
          roundId: this.currentRound.roundId,
          status: 'closed',
          closeType: 'settle',
          netProfit: netProfit,
        });
      }

      return {
        success: true,
        strategy: 'redeem',
        txHash: `paper_redeem_${Date.now()}`,
        amountReceived,
        executionTimeMs: Date.now() - startTime,
      };
    }

    try {
      // Check market resolution first
      const resolution = await this.ctf.getMarketResolution(this.market.conditionId);

      if (!resolution.isResolved) {
        return {
          success: false,
          strategy: 'redeem',
          error: 'Market not yet resolved',
          executionTimeMs: Date.now() - startTime,
        };
      }

      // Redeem winning tokens using Polymarket token IDs
      const tokenIds = {
        yesTokenId: this.market.upTokenId,
        noTokenId: this.market.downTokenId,
      };

      const result = await this.ctf.redeemByTokenIds(this.market.conditionId, tokenIds);

      return {
        success: result.success,
        strategy: 'redeem',
        txHash: result.txHash,
        amountReceived: result.usdcReceived ? parseFloat(result.usdcReceived) : undefined,
        executionTimeMs: Date.now() - startTime,
      };
    } catch (error) {
      return {
        success: false,
        strategy: 'redeem',
        error: error instanceof Error ? error.message : String(error),
        executionTimeMs: Date.now() - startTime,
      };
    }
  }

  private async settleBySell(): Promise<DipArbSettleResult> {
    const startTime = Date.now();

    if (!this.tradingService || !this.market || !this.currentRound) {
      return {
        success: false,
        strategy: 'sell',
        error: 'Trading service or market not available',
        executionTimeMs: Date.now() - startTime,
      };
    }

    // In paper mode, simulate the sell
    if (this.config.paperMode) {
      let totalReceived = 0;
      
      if (this.currentRound.leg1) {
        const price = this.currentRound.leg1.side === 'UP'
          ? (this.upAsks[0]?.price ?? 0.5)
          : (this.downAsks[0]?.price ?? 0.5);
        totalReceived += this.currentRound.leg1.shares * price * 0.99; // 1% slippage
      }
      
      if (this.currentRound.leg2) {
        const price = this.currentRound.leg2.side === 'UP'
          ? (this.upAsks[0]?.price ?? 0.5)
          : (this.downAsks[0]?.price ?? 0.5);
        totalReceived += this.currentRound.leg2.shares * price * 0.99; // 1% slippage
      }
      
      // Calculate profit/loss from the settle
      const totalCost = (this.currentRound.leg1?.price ?? 0) * (this.currentRound.leg1?.shares ?? 0) +
                        (this.currentRound.leg2?.price ?? 0) * (this.currentRound.leg2?.shares ?? 0);

      // ✅ FIX: Calculate NET profit after fees
      const grossProfit = totalReceived - totalCost;
      const feeRate = this.config.takerFeeRate ?? 0.03;
      const totalFees = totalCost * feeRate * 2; // 6% total (3% per leg)
      const netProfit = grossProfit - totalFees;

      this.log(`📝 [PAPER] Settle by sell simulated: received ~$${totalReceived.toFixed(2)}`);
      this.log(`   💰 [PAPER] NET Profit: $${netProfit.toFixed(2)} (gross: $${grossProfit.toFixed(2)}, fees: $${totalFees.toFixed(2)})`);

      // Emit paper trade event for settle
      this.emit('paperTrade', {
        type: 'settle',
        side: this.currentRound.leg1?.side ?? 'UP',
        shares: (this.currentRound.leg1?.shares ?? 0) + (this.currentRound.leg2?.shares ?? 0),
        price: totalReceived / ((this.currentRound.leg1?.shares ?? 1) + (this.currentRound.leg2?.shares ?? 1)),
        cost: totalReceived,
        profit: netProfit, // ✅ Use NET profit
        profitRate: totalCost > 0 ? netProfit / (totalCost + totalFees) : 0,
        expectedPrice: 1,                          // Expected sell price before slippage
        slippagePercent: 0.01,                     // 1% simulated slippage on sell
        marketName: this.market.name,
        conditionId: this.market.conditionId,
        timestamp: Date.now(),
      });

      // ✅ FIX: Emit position closed event for backend persistence
      if (this.currentRound) {
        this.emit('openPositionClosed', {
          roundId: this.currentRound.roundId,
          status: 'closed',
          closeType: 'settle',
          netProfit: netProfit,
        });
      }

      return {
        success: true,
        strategy: 'sell',
        amountReceived: totalReceived,
        executionTimeMs: Date.now() - startTime,
      };
    }

    try {
      let totalReceived = 0;

      // Sell leg1 position if exists (REAL trading only)
      if (this.currentRound.leg1) {
        const leg1Shares = this.currentRound.leg1.shares;
        const result = await this.tradingService.createMarketOrder({
          tokenId: this.currentRound.leg1.tokenId,
          side: 'SELL' as Side,
          amount: leg1Shares,
        });

        if (result.success) {
          totalReceived += leg1Shares * (this.currentRound.leg1.side === 'UP'
            ? (this.upAsks[0]?.price ?? 0.5)
            : (this.downAsks[0]?.price ?? 0.5));
        }
      }

      // Sell leg2 position if exists (REAL trading only)
      if (this.currentRound.leg2) {
        const leg2Shares = this.currentRound.leg2.shares;
        const result = await this.tradingService.createMarketOrder({
          tokenId: this.currentRound.leg2.tokenId,
          side: 'SELL' as Side,
          amount: leg2Shares,
        });

        if (result.success) {
          totalReceived += leg2Shares * (this.currentRound.leg2.side === 'UP'
            ? (this.upAsks[0]?.price ?? 0.5)
            : (this.downAsks[0]?.price ?? 0.5));
        }
      }

      return {
        success: true,
        strategy: 'sell',
        amountReceived: totalReceived,
        executionTimeMs: Date.now() - startTime,
      };
    } catch (error) {
      return {
        success: false,
        strategy: 'sell',
        error: error instanceof Error ? error.message : String(error),
        executionTimeMs: Date.now() - startTime,
      };
    }
  }

  // ===== Private: Helpers =====

  /**
   * FIX #6: Dynamic Leg2 timeout based on market duration
   * 5m markets have very limited time, so use shorter timeout
   * @returns Timeout in seconds for Leg2 before emergency exit
   */
  private getLeg2TimeoutSeconds(): number {
    // Use config value as default
    const defaultTimeout = this.config.leg2TimeoutSeconds;

    // If no market, use default
    if (!this.market?.durationMinutes) {
      return defaultTimeout;
    }

    const durationMinutes = this.market.durationMinutes;

    // Dynamic timeout: shorter for shorter markets
    if (durationMinutes <= 5) {
      return Math.min(60, defaultTimeout);   // 1 minute max for 5m markets
    }
    if (durationMinutes <= 15) {
      return Math.min(120, defaultTimeout);  // 2 minutes max for 15m markets
    }
    if (durationMinutes <= 60) {
      return Math.min(180, defaultTimeout);  // 3 minutes max for 1h markets
    }

    return defaultTimeout; // Use config for longer markets
  }

  // ===== Private: Settlement Awareness Methods =====

  /**
   * Calculate win probability based on current token prices
   * Token price approximates market's implied probability
   *
   * @param side - The position side (UP or DOWN)
   * @returns Win probability as decimal (0-1)
   */
  private getPositionWinProbability(side: DipArbSide): number {
    const upAsk = this.upAsks[0]?.price ?? 0.5;
    const downAsk = this.downAsks[0]?.price ?? 0.5;

    // Normalize to probabilities (should sum to ~1)
    const total = upAsk + downAsk;
    if (total === 0) return 0.5;

    const upProb = upAsk / total;
    const downProb = downAsk / total;

    return side === 'UP' ? upProb : downProb;
  }

  /**
   * Calculate expected value of holding to settlement
   *
   * @param leg1 - The leg1 fill info
   * @returns Expected settlement value in USDC
   */
  private calculateSettlementExpectedValue(leg1: { side: DipArbSide; shares: number; price: number }): number {
    const winProb = this.getPositionWinProbability(leg1.side);
    // If we win: get $1 per share. If we lose: get $0
    return winProb * leg1.shares * 1.0;
  }

  /**
   * Get current market value if we exit now
   * Accounts for slippage and fees on exit
   *
   * @param leg1 - The leg1 fill info
   * @returns Current exit value in USDC (after fees)
   */
  private getCurrentExitValue(leg1: { side: DipArbSide; shares: number; price: number }): number {
    const currentPrice = leg1.side === 'UP'
      ? (this.upAsks[0]?.price ?? leg1.price)
      : (this.downAsks[0]?.price ?? leg1.price);
    // Account for slippage and fees on exit (~6% total cost)
    return leg1.shares * currentPrice * 0.94;
  }

  /**
   * Check if Chainlink price movement favors our position
   *
   * @param side - The position side (UP or DOWN)
   * @returns Object with alignment status and delta from price to beat
   */
  private checkChainlinkAlignment(side: DipArbSide): { aligned: boolean; delta: number } {
    if (!this.market?.priceToBeat || !this.currentUnderlyingPrice) {
      return { aligned: false, delta: 0 };
    }

    const delta = (this.currentUnderlyingPrice - this.market.priceToBeat) / this.market.priceToBeat;

    // UP wins if price >= priceToBeat (delta >= 0)
    // DOWN wins if price < priceToBeat (delta < 0)
    const aligned = (side === 'UP' && delta >= 0) || (side === 'DOWN' && delta < 0);

    return { aligned, delta };
  }

  /**
   * Check Binance momentum for settlement hold decision
   *
   * Note: This is a simplified sync version that uses Chainlink data as proxy.
   * The full async Binance momentum check is not used here because
   * shouldHoldForSettlement is called in a sync context.
   *
   * @param side - The position side (UP or DOWN)
   * @returns Object with favorable status and momentum strength
   */
  private checkSettlementMomentum(side: DipArbSide): { favorable: boolean; strength: number } {
    // Use Chainlink alignment as a proxy for momentum
    // This avoids async calls in the sync shouldHoldForSettlement method
    const chainlink = this.checkChainlinkAlignment(side);

    if (!chainlink.aligned) {
      return { favorable: false, strength: 0 };
    }

    // Convert delta to strength (cap at 1%)
    const strength = Math.min(Math.abs(chainlink.delta), 0.01) / 0.01;

    return { favorable: chainlink.aligned, strength };
  }

  /**
   * Core decision method: Should we hold for settlement instead of timeout exit?
   *
   * Evaluates multiple factors:
   * 1. Time to market end (always hold if very close)
   * 2. Win probability from token prices
   * 3. Expected value comparison (settlement vs exit)
   * 4. Chainlink price alignment
   * 5. Binance momentum (optional)
   *
   * @returns Decision object with hold boolean, reason, and confidence
   */
  private shouldHoldForSettlement(): { hold: boolean; reason: string; confidence: number } {
    // Check if settlement awareness is enabled
    if (!this.config.favorSettlement) {
      return { hold: false, reason: 'disabled', confidence: 0 };
    }

    // Check if we have a valid position
    if (!this.market || !this.currentRound?.leg1) {
      return { hold: false, reason: 'no_position', confidence: 0 };
    }

    const leg1 = this.currentRound.leg1;
    const now = Date.now();
    const endTime = this.market.endTime instanceof Date
      ? this.market.endTime.getTime()
      : (typeof this.market.endTime === 'number' ? this.market.endTime : new Date(this.market.endTime).getTime());
    const timeToEndMin = (endTime - now) / 60000;

    // RULE 1: Always hold if very close to market end
    const minTimeToEnd = this.config.minTimeToEndForHold ?? 3;
    if (timeToEndMin > 0 && timeToEndMin < minTimeToEnd) {
      return { hold: true, reason: 'near_settlement', confidence: 0.9 };
    }

    // Don't hold if market has already ended (let normal settlement logic handle)
    if (timeToEndMin <= 0) {
      return { hold: false, reason: 'market_ended', confidence: 0 };
    }

    // RULE 2: Check win probability
    const winProb = this.getPositionWinProbability(leg1.side);
    const threshold = this.config.settlementHoldThreshold ?? 0.50;

    if (winProb >= threshold) {
      return { hold: true, reason: 'high_probability', confidence: winProb };
    }

    // RULE 3: Expected value comparison
    const expectedSettleValue = this.calculateSettlementExpectedValue(leg1);
    const exitValue = this.getCurrentExitValue(leg1);

    // If expected settlement value > exit value, hold is better
    if (expectedSettleValue > exitValue && exitValue > 0) {
      const evRatio = expectedSettleValue / exitValue;
      return { hold: true, reason: 'positive_ev', confidence: Math.min(evRatio - 1, 0.5) };
    }

    // RULE 4: Chainlink alignment check
    const chainlink = this.checkChainlinkAlignment(leg1.side);
    if (chainlink.aligned && Math.abs(chainlink.delta) > 0.001) {
      return { hold: true, reason: 'chainlink_aligned', confidence: 0.6 };
    }

    // RULE 5: Momentum validation (if enabled)
    if (this.config.enableSettlementMomentum && this.binanceService) {
      const momentum = this.checkSettlementMomentum(leg1.side);
      const momentumThreshold = this.config.settlementMomentumThreshold ?? 0.3;
      if (momentum.favorable && momentum.strength > momentumThreshold) {
        return { hold: true, reason: 'momentum_favorable', confidence: momentum.strength };
      }
    }

    // No conditions met - don't hold
    return { hold: false, reason: 'no_edge', confidence: 0 };
  }

  /**
   * Enhanced version of shouldHoldForSettlement that returns full rule evaluations
   *
   * This method computes all rule evaluations upfront and includes them in the result,
   * enabling observability and auto-tuning of settlement parameters.
   *
   * @returns EnhancedSettlementDecision with full evaluation details
   */
  private getEnhancedSettlementDecision(): EnhancedSettlementDecision | null {
    // Need valid position for enhanced decision
    if (!this.market || !this.currentRound?.leg1) {
      return null;
    }

    const leg1 = this.currentRound.leg1;
    const now = Date.now();
    const endTime = this.market.endTime instanceof Date
      ? this.market.endTime.getTime()
      : (typeof this.market.endTime === 'number' ? this.market.endTime : new Date(this.market.endTime).getTime());
    const timeToEndMin = (endTime - now) / 60000;

    // Get current prices
    const upPrice = this.upAsks[0]?.price ?? 0.5;
    const downPrice = this.downAsks[0]?.price ?? 0.5;
    const currentChainlinkPrice = this.currentUnderlyingPrice ?? this.market.priceToBeat ?? 0;

    // Config thresholds
    const minTimeToEnd = this.config.minTimeToEndForHold ?? 3;
    const probThreshold = this.config.settlementHoldThreshold ?? 0.50;
    const momentumThreshold = this.config.settlementMomentumThreshold ?? 0.3;

    // Compute all rule evaluations
    const winProb = this.getPositionWinProbability(leg1.side);
    const expectedSettleValue = this.calculateSettlementExpectedValue(leg1);
    const exitValue = this.getCurrentExitValue(leg1);
    const evRatio = exitValue > 0 ? expectedSettleValue / exitValue : 0;
    const chainlink = this.checkChainlinkAlignment(leg1.side);
    const momentum = this.checkSettlementMomentum(leg1.side);

    // Build rule evaluations
    const ruleEvaluations: SettlementRuleEvaluations = {
      nearSettlement: {
        passed: timeToEndMin > 0 && timeToEndMin < minTimeToEnd,
        timeToEndMin,
        threshold: minTimeToEnd,
      },
      highProbability: {
        passed: winProb >= probThreshold,
        winProb,
        threshold: probThreshold,
      },
      positiveEV: {
        passed: expectedSettleValue > exitValue && exitValue > 0,
        settlementEV: expectedSettleValue,
        exitValue,
        evRatio,
      },
      chainlinkAligned: {
        passed: chainlink.aligned && Math.abs(chainlink.delta) > 0.001,
        delta: chainlink.delta,
        currentPrice: currentChainlinkPrice,
        priceToBeat: this.market.priceToBeat ?? 0,
        side: leg1.side,
      },
      momentumFavorable: {
        passed: this.config.enableSettlementMomentum && momentum.favorable && momentum.strength > momentumThreshold,
        strength: momentum.strength,
        threshold: momentumThreshold,
        enabled: this.config.enableSettlementMomentum ?? false,
      },
    };

    // Build entry context
    const entryContext: SettlementEntryContext = {
      leg1Side: leg1.side,
      leg1EntryPrice: leg1.price,
      leg1Shares: leg1.shares,
      leg1Cost: leg1.price * leg1.shares,
      enteredAt: leg1.timestamp,
    };

    // Build market snapshot
    const marketSnapshot: SettlementMarketSnapshot = {
      upPrice,
      downPrice,
      priceToBeat: this.market.priceToBeat ?? 0,
      currentChainlinkPrice,
      marketEndTime: endTime,
    };

    // Determine decision and reason based on rule priority
    let decision: 'HOLD' | 'EXIT' = 'EXIT';
    let reason: EnhancedSettlementDecision['reason'] = 'no_edge';
    let confidence = 0;

    // Check if feature is disabled
    if (!this.config.favorSettlement) {
      decision = 'EXIT';
      reason = 'disabled';
      confidence = 0;
    }
    // Check for market ended
    else if (timeToEndMin <= 0) {
      decision = 'EXIT';
      reason = 'market_ended';
      confidence = 0;
    }
    // Priority order for HOLD reasons
    else if (ruleEvaluations.nearSettlement.passed) {
      decision = 'HOLD';
      reason = 'near_settlement';
      confidence = 0.9;
    } else if (ruleEvaluations.highProbability.passed) {
      decision = 'HOLD';
      reason = 'high_probability';
      confidence = winProb;
    } else if (ruleEvaluations.positiveEV.passed) {
      decision = 'HOLD';
      reason = 'positive_ev';
      confidence = Math.min(evRatio - 1, 0.5);
    } else if (ruleEvaluations.chainlinkAligned.passed) {
      decision = 'HOLD';
      reason = 'chainlink_aligned';
      confidence = 0.6;
    } else if (ruleEvaluations.momentumFavorable.passed) {
      decision = 'HOLD';
      reason = 'momentum_favorable';
      confidence = momentum.strength;
    }

    return {
      decision,
      reason,
      confidence,
      ruleEvaluations,
      entryContext,
      marketSnapshot,
      roundId: this.currentRound.roundId,
      marketName: this.market.name,
      underlying: this.market.underlying,
      isPaper: this.config.paperMode ?? true,
      timestamp: now,
    };
  }

  // ===== Settlement Outcome Tracking =====

  /** Map of pending settlement decisions awaiting outcome reconciliation */
  private pendingSettlementDecisions: Map<string, EnhancedSettlementDecision> = new Map();

  /** Timeouts for settlement outcome checks */
  private settlementOutcomeTimeouts: Map<string, NodeJS.Timeout> = new Map();

  /**
   * Schedule a check for settlement outcome after market ends
   *
   * This method stores the decision and schedules a callback to check
   * the outcome after the market settles.
   *
   * @param decision - The enhanced settlement decision to track
   */
  private scheduleSettlementOutcomeCheck(decision: EnhancedSettlementDecision): void {
    const { roundId, marketSnapshot } = decision;

    // Store the decision for later reconciliation
    this.pendingSettlementDecisions.set(roundId, decision);

    // Calculate delay until we should check outcome
    const now = Date.now();
    const marketEndTime = marketSnapshot.marketEndTime;
    const bufferMs = (this.config.settlementWaitBuffer ?? 300) * 1000; // Default 5 min buffer

    // Schedule outcome check for after market end + buffer
    const checkDelay = Math.max(0, marketEndTime - now + bufferMs);

    this.log(`📋 Scheduled settlement outcome check for ${roundId.slice(0, 20)}... in ${Math.round(checkDelay / 1000)}s`);

    // Clear any existing timeout for this round
    const existingTimeout = this.settlementOutcomeTimeouts.get(roundId);
    if (existingTimeout) {
      clearTimeout(existingTimeout);
    }

    // Schedule the outcome check
    const timeout = setTimeout(() => {
      this.checkSettlementOutcome(roundId);
    }, checkDelay);

    this.settlementOutcomeTimeouts.set(roundId, timeout);
  }

  /**
   * Check the outcome of a settlement decision
   *
   * This is called after the market has settled to determine if the
   * HOLD decision was correct and calculate actual vs counterfactual profit.
   *
   * @param roundId - The round ID to check
   */
  private async checkSettlementOutcome(roundId: string): Promise<void> {
    const decision = this.pendingSettlementDecisions.get(roundId);
    if (!decision) {
      this.log(`⚠️ No pending decision found for ${roundId.slice(0, 20)}...`);
      return;
    }

    // Clean up
    this.pendingSettlementDecisions.delete(roundId);
    this.settlementOutcomeTimeouts.delete(roundId);

    try {
      // Determine settlement outcome
      const outcome = await this.calculateSettlementOutcome(decision);
      if (outcome) {
        this.emit('settlementOutcome', outcome);
        this.log(`📊 Settlement outcome for ${roundId.slice(0, 20)}...: ${outcome.outcome} ` +
          `(actual: $${outcome.actualProfit.toFixed(2)}, counterfactual: $${outcome.counterfactualProfit.toFixed(2)})`);
      }
    } catch (error) {
      this.log(`❌ Error checking settlement outcome for ${roundId}: ${error}`);
    }
  }

  /**
   * Calculate the settlement outcome for a decision
   *
   * Uses the current Chainlink price (which should be the settlement price)
   * to determine which side won and calculate actual/counterfactual profits.
   *
   * @param decision - The enhanced settlement decision
   * @returns SettlementOutcome or null if unable to calculate
   */
  private async calculateSettlementOutcome(decision: EnhancedSettlementDecision): Promise<SettlementOutcome | null> {
    const { entryContext, marketSnapshot, roundId, underlying, marketName, isPaper } = decision;

    // Get settlement price (current Chainlink price after market end)
    const settlementPrice = this.currentUnderlyingPrice ?? marketSnapshot.currentChainlinkPrice;

    // Determine which side won
    const settlementSide: DipArbSide = settlementPrice >= marketSnapshot.priceToBeat ? 'UP' : 'DOWN';

    // Calculate actual profit (for HOLD decision)
    let actualProfit: number;
    let outcome: 'WIN' | 'LOSS';

    if (decision.decision === 'HOLD') {
      // For HOLD: we kept the position and it settled
      const positionWon = entryContext.leg1Side === settlementSide;
      const feeRate = this.config.takerFeeRate ?? DIP_ARB_CRYPTO_TAKER_FEE;
      const entryFee = entryContext.leg1Cost * feeRate;

      if (positionWon) {
        // Won at settlement: receive $1 per share minus entry cost AND entry fee
        actualProfit = entryContext.leg1Shares - entryContext.leg1Cost - entryFee;
        outcome = 'WIN';
      } else {
        // Lost at settlement: lose entire entry cost (fee was already paid)
        actualProfit = -entryContext.leg1Cost - entryFee;
        outcome = 'LOSS';
      }
    } else {
      // For EXIT: we already exited at timeout, calculate what we got
      // This is approximate since we don't track the actual exit price in the decision
      const exitPrice = entryContext.leg1Side === 'UP' ? marketSnapshot.upPrice : marketSnapshot.downPrice;
      const feeRate = this.config.takerFeeRate ?? DIP_ARB_CRYPTO_TAKER_FEE;
      // Exit is a single SELL = single fee (3%, not 6%)
      // But we also paid entry fee on leg1
      const exitValue = exitPrice * entryContext.leg1Shares * (1 - feeRate);
      const entryFee = entryContext.leg1Cost * feeRate;
      actualProfit = exitValue - entryContext.leg1Cost - entryFee;
      // For EXIT, determine win/loss based on whether exiting was better than holding
      const wouldHaveWon = entryContext.leg1Side === settlementSide;
      const holdProfit = wouldHaveWon
        ? entryContext.leg1Shares - entryContext.leg1Cost - entryFee
        : -entryContext.leg1Cost - entryFee;
      outcome = actualProfit >= holdProfit ? 'WIN' : 'LOSS';
    }

    // Calculate counterfactual profit (what would have happened with opposite decision)
    let counterfactualProfit: number;
    const cfFeeRate = this.config.takerFeeRate ?? DIP_ARB_CRYPTO_TAKER_FEE;
    const cfEntryFee = entryContext.leg1Cost * cfFeeRate;

    if (decision.decision === 'HOLD') {
      // If we held, counterfactual is if we had exited
      const wouldHaveExitedAt = decision.entryContext.leg1Side === 'UP'
        ? marketSnapshot.upPrice
        : marketSnapshot.downPrice;
      const exitValue = wouldHaveExitedAt * entryContext.leg1Shares * (1 - cfFeeRate);
      counterfactualProfit = exitValue - entryContext.leg1Cost - cfEntryFee;
    } else {
      // If we exited, counterfactual is if we had held
      const wouldHaveWon = entryContext.leg1Side === settlementSide;
      if (wouldHaveWon) {
        counterfactualProfit = entryContext.leg1Shares - entryContext.leg1Cost - cfEntryFee;
      } else {
        counterfactualProfit = -entryContext.leg1Cost - cfEntryFee;
      }
    }

    return {
      roundId,
      decision: decision.decision,
      outcome,
      actualProfit,
      counterfactualProfit,
      settlementSide,
      settlementPrice,
      settledAt: Date.now(),
      isPaper,
      underlying,
      marketName,
    };
  }

  /**
   * Clean up pending settlement checks when service stops
   */
  private cleanupPendingSettlements(): void {
    for (const timeout of this.settlementOutcomeTimeouts.values()) {
      clearTimeout(timeout);
    }
    this.settlementOutcomeTimeouts.clear();
    this.pendingSettlementDecisions.clear();
  }

  /**
   * Update orderbook buffer for smart logging
   * Keeps last 5 seconds of orderbook data
   */
  private updateOrderbookBuffer(_book: OrderbookSnapshot): void {
    if (!this.market) return;

    const upAsk = this.upAsks[0]?.price ?? 0;
    const downAsk = this.downAsks[0]?.price ?? 0;

    this.orderbookBuffer.push({
      timestamp: Date.now(),
      upAsk,
      downAsk,
      upDepth: this.upAsks.length,
      downDepth: this.downAsks.length,
    });

    // Keep only last ORDERBOOK_BUFFER_SIZE entries
    if (this.orderbookBuffer.length > this.ORDERBOOK_BUFFER_SIZE) {
      this.orderbookBuffer = this.orderbookBuffer.slice(-this.ORDERBOOK_BUFFER_SIZE);
    }
  }

  /**
   * Log orderbook summary at intervals (every 10 seconds)
   * Reduces log noise from ~10 logs/sec to 1 log/10sec
   */
  private maybeLogOrderbookSummary(): void {
    const now = Date.now();

    // Only log every ORDERBOOK_LOG_INTERVAL_MS
    if (now - this.lastOrderbookLogTime < this.ORDERBOOK_LOG_INTERVAL_MS) {
      return;
    }

    this.lastOrderbookLogTime = now;

    const upAsk = this.upAsks[0]?.price ?? 0;
    const downAsk = this.downAsks[0]?.price ?? 0;
    const sum = upAsk + downAsk;

    this.log(`📊 Orderbook: UP=${upAsk.toFixed(3)} | DOWN=${downAsk.toFixed(3)} | Sum=${sum.toFixed(3)}`);
  }

  /**
   * Log orderbook buffer around a signal/trade
   * Called when signal is detected to capture market context
   */
  private logOrderbookContext(eventType: string): void {
    if (this.orderbookBuffer.length === 0) return;

    this.log(`📈 ${eventType} - Orderbook context (last ${this.orderbookBuffer.length} ticks):`);

    // Log first, middle, and last entries for context
    const first = this.orderbookBuffer[0];
    const mid = this.orderbookBuffer[Math.floor(this.orderbookBuffer.length / 2)];
    const last = this.orderbookBuffer[this.orderbookBuffer.length - 1];

    const formatTime = (ts: number) => new Date(ts).toISOString().slice(11, 23);

    this.log(`   ${formatTime(first.timestamp)}: UP=${first.upAsk.toFixed(4)} DOWN=${first.downAsk.toFixed(4)}`);
    if (this.orderbookBuffer.length > 2) {
      this.log(`   ${formatTime(mid.timestamp)}: UP=${mid.upAsk.toFixed(4)} DOWN=${mid.downAsk.toFixed(4)}`);
    }
    this.log(`   ${formatTime(last.timestamp)}: UP=${last.upAsk.toFixed(4)} DOWN=${last.downAsk.toFixed(4)}`);

    // Calculate price changes
    const upChange = ((last.upAsk - first.upAsk) / first.upAsk * 100).toFixed(2);
    const downChange = ((last.downAsk - first.downAsk) / first.downAsk * 100).toFixed(2);
    this.log(`   Change: UP ${upChange}% | DOWN ${downChange}%`);
  }

  private log(message: string): void {
    const shouldLog = this.config.debug || message.startsWith('Starting') || message.startsWith('Stopped');
    if (!shouldLog) return;

    const formatted = `[DipArb] ${message}`;

    // Use custom log handler if provided
    if (this.config.logHandler) {
      this.config.logHandler(formatted);
    } else {
      console.log(formatted);
    }
  }
}

// Re-export types
export * from './dip-arb-types.js';
