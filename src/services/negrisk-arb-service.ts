/**
 * NegRisk Arbitrage Service
 *
 * Handles multi-outcome arbitrage for NegRisk events (championships, tournaments, elections).
 *
 * ## Strategy
 *
 * NegRisk markets are "winner-take-all" events where exactly one outcome wins:
 * - All YES prices should sum to ~$1.00
 * - If sum < $1.00: Long Arb (buy all YES tokens, guaranteed profit at resolution)
 * - If sum > $1.00: Short Arb (split USDC, sell all YES tokens)
 *
 * ## NegRisk Contract Features
 *
 * The NegRisk adapter enables cross-outcome token conversion:
 * - A single NO token can be converted to YES tokens of all OTHER outcomes
 * - This creates capital efficiency and cross-market arbitrage opportunities
 *
 * ## Examples
 *
 * ### NBA Champion (6 teams):
 * ```
 * Lakers YES: $0.15  Celtics YES: $0.35  Nuggets YES: $0.25
 * Warriors YES: $0.18  Mavs YES: $0.05  Other YES: $0.02
 * Total: $1.00 (no arb)
 *
 * If total = $0.95: Buy all → Profit = $0.05 (5.3%)
 * If total = $1.08: Split $1 → Sell all → Profit = $0.08 (8%)
 * ```
 *
 * @see docs/arb/arbitrage.md Section 9 for detailed theory
 */

import { EventEmitter } from 'events';
import { GammaApiClient, type GammaMarket, type GammaEvent } from '../clients/gamma-api.js';
import { TradingService } from './trading-service.js';
import { CTFClient, type TokenIds } from '../clients/ctf-client.js';
import { RateLimiter } from '../core/rate-limiter.js';
import { createUnifiedCache, type UnifiedCache } from '../core/unified-cache.js';
import { MarketService } from './market-service.js';

// ===== Types =====

/**
 * Configuration for NegRisk arbitrage service
 */
export interface NegRiskArbServiceConfig {
  /** Private key for trading (optional for monitor-only mode) */
  privateKey?: string;
  /** RPC URL for CTF operations */
  rpcUrl?: string;
  /** Minimum profit threshold (default: 0.005 = 0.5%) */
  profitThreshold?: number;
  /** Minimum trade size in USDC (default: 10) */
  minTradeSize?: number;
  /** Maximum total trade size across all outcomes (default: 1000) */
  maxTotalTradeSize?: number;
  /** Auto-execute mode (default: false) */
  autoExecute?: boolean;
  /** Enable logging (default: true) */
  enableLogging?: boolean;
  /** Scan interval in ms (default: 30000 = 30 seconds) */
  scanIntervalMs?: number;
  /** Minimum liquidity per market (default: 100) */
  minLiquidityPerMarket?: number;
  /** Maximum number of outcomes to handle (default: 20) */
  maxOutcomes?: number;
}

/**
 * Outcome in a NegRisk event
 */
export interface NegRiskOutcome {
  /** Market condition ID */
  conditionId: string;
  /** Outcome name (e.g., "Lakers", "Trump") */
  name: string;
  /** YES token ID */
  yesTokenId: string;
  /** NO token ID */
  noTokenId: string;
  /** Current YES price (0-1) */
  yesPrice: number;
  /** Current NO price (0-1) */
  noPrice: number;
  /** Best ask for YES (for buying) */
  yesAsk?: number;
  /** Best bid for YES (for selling) */
  yesBid?: number;
  /** Available liquidity */
  liquidity: number;
  /** 24h volume */
  volume24h: number;
  /** Market slug */
  slug: string;
}

/**
 * NegRisk event with arbitrage analysis
 */
export interface NegRiskEvent {
  /** Event ID */
  eventId: string;
  /** Event title */
  title: string;
  /** Event slug */
  slug: string;
  /** All outcomes in this event */
  outcomes: NegRiskOutcome[];
  /** Sum of all YES prices */
  yesPriceSum: number;
  /** Sum of all YES asks (actual buy cost) */
  yesAskSum: number;
  /** Sum of all YES bids (actual sell revenue) */
  yesBidSum: number;
  /** Arbitrage type */
  arbType: 'long' | 'short' | 'none';
  /** Gross profit rate (before fees, which are 0% for most NegRisk) */
  grossProfitRate: number;
  /** Net profit rate (after any fees) */
  netProfitRate: number;
  /** Estimated profit in USDC for $100 trade */
  estimatedProfit100: number;
  /** Total liquidity across all outcomes */
  totalLiquidity: number;
  /** Maximum executable size based on lowest liquidity outcome */
  maxExecutableSize: number;
  /** Event end date */
  endDate?: Date;
  /** Last scan timestamp */
  lastScan: number;
}

/**
 * Execution result for a NegRisk arbitrage
 */
export interface NegRiskExecutionResult {
  /** Whether execution succeeded */
  success: boolean;
  /** Event ID */
  eventId: string;
  /** Arbitrage type executed */
  arbType: 'long' | 'short';
  /** Size per outcome (USDC) */
  sizePerOutcome: number;
  /** Total size across all outcomes */
  totalSize: number;
  /** Actual profit realized */
  profit: number;
  /** Number of successful orders */
  ordersSucceeded: number;
  /** Total number of orders */
  ordersTotal: number;
  /** Order results per outcome */
  orderResults: Array<{
    outcome: string;
    conditionId: string;
    success: boolean;
    price?: number;
    size?: number;
    error?: string;
  }>;
  /** Execution time in ms */
  executionTimeMs: number;
  /** Error message if failed */
  error?: string;
}

/**
 * Events emitted by NegRiskArbService
 */
export interface NegRiskArbServiceEvents {
  opportunity: (event: NegRiskEvent) => void;
  execution: (result: NegRiskExecutionResult) => void;
  scan: (events: NegRiskEvent[]) => void;
  error: (error: Error) => void;
  started: () => void;
  stopped: () => void;
}

// ===== Service Implementation =====

export class NegRiskArbService extends EventEmitter {
  private gammaApi: GammaApiClient;
  private marketService: MarketService;
  private tradingService: TradingService | null = null;
  private ctf: CTFClient | null = null;
  private rateLimiter: RateLimiter;
  private cache: UnifiedCache;

  private config: Required<Omit<NegRiskArbServiceConfig, 'privateKey' | 'rpcUrl'>> & {
    privateKey?: string;
    rpcUrl?: string;
  };

  private isRunning = false;
  private scanTimer: ReturnType<typeof setInterval> | null = null;
  private trackedEvents: Map<string, NegRiskEvent> = new Map();

  // Statistics
  private stats = {
    scansPerformed: 0,
    opportunitiesFound: 0,
    executionsAttempted: 0,
    executionsSucceeded: 0,
    totalProfit: 0,
    startTime: 0,
  };

  constructor(config: NegRiskArbServiceConfig = {}) {
    super();

    this.config = {
      privateKey: config.privateKey,
      rpcUrl: config.rpcUrl || 'https://polygon-rpc.com',
      profitThreshold: config.profitThreshold ?? 0.005,
      minTradeSize: config.minTradeSize ?? 10,
      maxTotalTradeSize: config.maxTotalTradeSize ?? 1000,
      autoExecute: config.autoExecute ?? false,
      enableLogging: config.enableLogging ?? true,
      scanIntervalMs: config.scanIntervalMs ?? 30000,
      minLiquidityPerMarket: config.minLiquidityPerMarket ?? 100,
      maxOutcomes: config.maxOutcomes ?? 20,
    };

    this.rateLimiter = new RateLimiter();
    this.cache = createUnifiedCache();
    this.gammaApi = new GammaApiClient(this.rateLimiter, this.cache);
    this.marketService = new MarketService(this.gammaApi, undefined, this.rateLimiter, this.cache);

    // Initialize trading if private key provided
    if (this.config.privateKey) {
      this.ctf = new CTFClient({
        privateKey: this.config.privateKey,
        rpcUrl: this.config.rpcUrl,
      });

      this.tradingService = new TradingService(this.rateLimiter, this.cache, {
        privateKey: this.config.privateKey,
        chainId: 137,
      });
    }
  }

  // ===== Public API =====

  /**
   * Start scanning for NegRisk arbitrage opportunities
   *
   * @example
   * ```typescript
   * const service = new NegRiskArbService({
   *   privateKey: '0x...',
   *   profitThreshold: 0.01,  // 1% minimum
   *   autoExecute: false,
   * });
   *
   * service.on('opportunity', (event) => {
   *   console.log(`${event.title}: ${event.arbType} +${(event.netProfitRate * 100).toFixed(2)}%`);
   * });
   *
   * await service.start();
   * ```
   */
  async start(): Promise<void> {
    if (this.isRunning) {
      throw new Error('NegRiskArbService is already running');
    }

    this.isRunning = true;
    this.stats.startTime = Date.now();

    this.log('Starting NegRisk Arbitrage Scanner...');
    this.log(`Profit Threshold: ${(this.config.profitThreshold * 100).toFixed(2)}%`);
    this.log(`Auto Execute: ${this.config.autoExecute ? 'YES' : 'NO'}`);

    // Initialize trading service if available
    if (this.tradingService) {
      await this.tradingService.initialize();
      this.log(`Wallet: ${this.ctf?.getAddress()}`);
    } else {
      this.log('No wallet configured - monitoring only');
    }

    // Perform initial scan
    await this.scan();

    // Start periodic scanning
    this.scanTimer = setInterval(() => this.scan(), this.config.scanIntervalMs);

    this.emit('started');
  }

  /**
   * Stop scanning
   */
  stop(): void {
    if (!this.isRunning) return;

    this.isRunning = false;

    if (this.scanTimer) {
      clearInterval(this.scanTimer);
      this.scanTimer = null;
    }

    this.log('Stopped');
    this.logStats();

    this.emit('stopped');
  }

  /**
   * Perform a single scan for NegRisk opportunities
   */
  async scan(): Promise<NegRiskEvent[]> {
    this.stats.scansPerformed++;

    try {
      // Fetch active events
      const events = await this.gammaApi.getEvents({
        active: true,
        limit: 100,
      });

      const negRiskEvents: NegRiskEvent[] = [];

      for (const event of events) {
        // Skip events without multiple markets
        if (!event.markets || event.markets.length < 2) {
          continue;
        }

        // Skip if too many outcomes
        if (event.markets.length > this.config.maxOutcomes) {
          continue;
        }

        // Parse and analyze event
        const analyzed = await this.analyzeEvent(event);
        if (!analyzed) continue;

        // Check liquidity requirements
        const meetsLiquidity = analyzed.outcomes.every(
          (o) => o.liquidity >= this.config.minLiquidityPerMarket
        );
        if (!meetsLiquidity) continue;

        // Track event
        this.trackedEvents.set(event.id, analyzed);

        // Check for arbitrage opportunity
        if (analyzed.arbType !== 'none' && analyzed.netProfitRate >= this.config.profitThreshold) {
          this.stats.opportunitiesFound++;
          negRiskEvents.push(analyzed);

          this.log(`\n${'!'.repeat(60)}`);
          this.log(`${analyzed.arbType.toUpperCase()} ARB: ${analyzed.title}`);
          this.log(`Outcomes: ${analyzed.outcomes.length}`);
          this.log(`YES Sum: ${analyzed.yesPriceSum.toFixed(4)} (should be 1.00)`);
          this.log(`Profit: ${(analyzed.netProfitRate * 100).toFixed(2)}%`);
          this.log(`Est. Profit on $100: $${analyzed.estimatedProfit100.toFixed(2)}`);
          this.log('!'.repeat(60));

          this.emit('opportunity', analyzed);

          // Auto-execute if enabled
          if (this.config.autoExecute && this.tradingService) {
            await this.execute(analyzed);
          }
        }
      }

      this.emit('scan', negRiskEvents);
      return negRiskEvents;
    } catch (error: any) {
      this.log(`Scan error: ${error.message}`);
      this.emit('error', error);
      return [];
    }
  }

  /**
   * Execute arbitrage on a NegRisk event
   *
   * @param event - NegRisk event with arbitrage opportunity
   * @param size - Total size in USDC (optional, defaults to calculated optimal)
   */
  async execute(event: NegRiskEvent, size?: number): Promise<NegRiskExecutionResult> {
    if (!this.tradingService || !this.ctf) {
      return {
        success: false,
        eventId: event.eventId,
        arbType: event.arbType === 'none' ? 'long' : event.arbType,
        sizePerOutcome: 0,
        totalSize: 0,
        profit: 0,
        ordersSucceeded: 0,
        ordersTotal: 0,
        orderResults: [],
        executionTimeMs: 0,
        error: 'Trading not configured (no private key)',
      };
    }

    if (event.arbType === 'none') {
      return {
        success: false,
        eventId: event.eventId,
        arbType: 'long',
        sizePerOutcome: 0,
        totalSize: 0,
        profit: 0,
        ordersSucceeded: 0,
        ordersTotal: 0,
        orderResults: [],
        executionTimeMs: 0,
        error: 'No arbitrage opportunity',
      };
    }

    const startTime = Date.now();
    this.stats.executionsAttempted++;

    // Calculate trade size
    const totalSize = Math.min(
      size || event.maxExecutableSize,
      this.config.maxTotalTradeSize
    );

    // For NegRisk, we buy the same dollar amount of each outcome
    // Size per outcome = total / number of outcomes (for long arb)
    const sizePerOutcome = totalSize / event.outcomes.length;

    if (sizePerOutcome < this.config.minTradeSize) {
      return {
        success: false,
        eventId: event.eventId,
        arbType: event.arbType,
        sizePerOutcome,
        totalSize,
        profit: 0,
        ordersSucceeded: 0,
        ordersTotal: event.outcomes.length,
        orderResults: [],
        executionTimeMs: Date.now() - startTime,
        error: `Size per outcome (${sizePerOutcome.toFixed(2)}) below minimum (${this.config.minTradeSize})`,
      };
    }

    this.log(`\nExecuting ${event.arbType.toUpperCase()} Arb on: ${event.title}`);
    this.log(`Total Size: $${totalSize.toFixed(2)}, Per Outcome: $${sizePerOutcome.toFixed(2)}`);

    const orderResults: NegRiskExecutionResult['orderResults'] = [];
    let ordersSucceeded = 0;

    if (event.arbType === 'long') {
      // Long Arb: Buy YES on all outcomes
      // Execute orders in parallel for speed
      const orderPromises = event.outcomes.map(async (outcome) => {
        try {
          const result = await this.tradingService!.createMarketOrder({
            tokenId: outcome.yesTokenId,
            side: 'BUY',
            amount: sizePerOutcome,
            orderType: 'FOK',
          });

          if (result.success) {
            ordersSucceeded++;
            return {
              outcome: outcome.name,
              conditionId: outcome.conditionId,
              success: true,
              price: outcome.yesAsk || outcome.yesPrice,
              size: sizePerOutcome,
            };
          } else {
            return {
              outcome: outcome.name,
              conditionId: outcome.conditionId,
              success: false,
              error: result.errorMsg || 'Order failed',
            };
          }
        } catch (error: any) {
          return {
            outcome: outcome.name,
            conditionId: outcome.conditionId,
            success: false,
            error: error.message,
          };
        }
      });

      const results = await Promise.all(orderPromises);
      orderResults.push(...results);
    } else {
      // Short Arb: Sell YES on all outcomes (requires holding tokens)
      // First check if we have tokens to sell
      let hasTokens = true;

      for (const outcome of event.outcomes) {
        try {
          const tokenIds: TokenIds = {
            yesTokenId: outcome.yesTokenId,
            noTokenId: outcome.noTokenId,
          };
          const balance = await this.ctf!.getPositionBalanceByTokenIds(
            outcome.conditionId,
            tokenIds
          );
          const yesBalance = parseFloat(balance.yesBalance);

          if (yesBalance < sizePerOutcome) {
            hasTokens = false;
            this.log(`Insufficient ${outcome.name} YES tokens: have ${yesBalance.toFixed(2)}, need ${sizePerOutcome.toFixed(2)}`);
          }
        } catch {
          hasTokens = false;
        }
      }

      if (!hasTokens) {
        return {
          success: false,
          eventId: event.eventId,
          arbType: 'short',
          sizePerOutcome,
          totalSize,
          profit: 0,
          ordersSucceeded: 0,
          ordersTotal: event.outcomes.length,
          orderResults: [],
          executionTimeMs: Date.now() - startTime,
          error: 'Insufficient token balance for short arb',
        };
      }

      // Execute sell orders in parallel
      const orderPromises = event.outcomes.map(async (outcome) => {
        try {
          const result = await this.tradingService!.createMarketOrder({
            tokenId: outcome.yesTokenId,
            side: 'SELL',
            amount: sizePerOutcome,
            orderType: 'FOK',
          });

          if (result.success) {
            ordersSucceeded++;
            return {
              outcome: outcome.name,
              conditionId: outcome.conditionId,
              success: true,
              price: outcome.yesBid || outcome.yesPrice,
              size: sizePerOutcome,
            };
          } else {
            return {
              outcome: outcome.name,
              conditionId: outcome.conditionId,
              success: false,
              error: result.errorMsg || 'Order failed',
            };
          }
        } catch (error: any) {
          return {
            outcome: outcome.name,
            conditionId: outcome.conditionId,
            success: false,
            error: error.message,
          };
        }
      });

      const results = await Promise.all(orderPromises);
      orderResults.push(...results);
    }

    const executionTimeMs = Date.now() - startTime;
    const allSucceeded = ordersSucceeded === event.outcomes.length;

    // Calculate actual profit
    const actualProfit = allSucceeded
      ? event.netProfitRate * totalSize
      : 0;

    if (allSucceeded) {
      this.stats.executionsSucceeded++;
      this.stats.totalProfit += actualProfit;
    }

    const result: NegRiskExecutionResult = {
      success: allSucceeded,
      eventId: event.eventId,
      arbType: event.arbType,
      sizePerOutcome,
      totalSize,
      profit: actualProfit,
      ordersSucceeded,
      ordersTotal: event.outcomes.length,
      orderResults,
      executionTimeMs,
      error: allSucceeded ? undefined : `Only ${ordersSucceeded}/${event.outcomes.length} orders succeeded`,
    };

    this.emit('execution', result);

    if (allSucceeded) {
      this.log(`✅ Execution complete! Profit: $${actualProfit.toFixed(2)}`);
    } else {
      this.log(`⚠️ Partial execution: ${ordersSucceeded}/${event.outcomes.length} orders succeeded`);
    }

    return result;
  }

  /**
   * Get all tracked events
   */
  getTrackedEvents(): NegRiskEvent[] {
    return Array.from(this.trackedEvents.values());
  }

  /**
   * Get events with arbitrage opportunities
   */
  getOpportunities(minProfitRate?: number): NegRiskEvent[] {
    const threshold = minProfitRate ?? this.config.profitThreshold;
    return this.getTrackedEvents().filter(
      (e) => e.arbType !== 'none' && e.netProfitRate >= threshold
    );
  }

  /**
   * Get statistics
   */
  getStats() {
    return {
      ...this.stats,
      runningTimeMs: this.isRunning ? Date.now() - this.stats.startTime : 0,
      eventsTracked: this.trackedEvents.size,
    };
  }

  // ===== Private Methods =====

  private async analyzeEvent(event: GammaEvent): Promise<NegRiskEvent | null> {
    const outcomes: NegRiskOutcome[] = [];

    for (const market of event.markets) {
      // Get CLOB market data for token IDs
      let clobMarket;
      try {
        clobMarket = await this.marketService.getClobMarket(market.conditionId);
        if (!clobMarket) continue;
      } catch {
        continue;
      }

      const yesToken = clobMarket.tokens[0];
      const noToken = clobMarket.tokens[1];
      if (!yesToken || !noToken) continue;

      // Get orderbook data if possible
      let yesAsk = market.outcomePrices?.[0] || 0.5;
      let yesBid = market.outcomePrices?.[0] || 0.5;

      try {
        const orderbook = await this.marketService.getProcessedOrderbook(market.conditionId);
        yesAsk = orderbook.yes.ask || yesAsk;
        yesBid = orderbook.yes.bid || yesBid;
      } catch {
        // Use price data as fallback
      }

      outcomes.push({
        conditionId: market.conditionId,
        name: market.outcomes?.[0] || market.question.slice(0, 30),
        yesTokenId: yesToken.tokenId,
        noTokenId: noToken.tokenId,
        yesPrice: market.outcomePrices?.[0] || 0.5,
        noPrice: market.outcomePrices?.[1] || 0.5,
        yesAsk,
        yesBid,
        liquidity: market.liquidity,
        volume24h: market.volume24hr || 0,
        slug: market.slug,
      });
    }

    if (outcomes.length < 2) {
      return null;
    }

    // Calculate sums
    const yesPriceSum = outcomes.reduce((sum, o) => sum + o.yesPrice, 0);
    const yesAskSum = outcomes.reduce((sum, o) => sum + (o.yesAsk || o.yesPrice), 0);
    const yesBidSum = outcomes.reduce((sum, o) => sum + (o.yesBid || o.yesPrice), 0);

    // Determine arbitrage type and profit
    let arbType: 'long' | 'short' | 'none' = 'none';
    let grossProfitRate = 0;

    if (yesAskSum < 0.995) {
      // Long arb: buy all YES for less than $1
      arbType = 'long';
      grossProfitRate = (1 - yesAskSum) / yesAskSum;
    } else if (yesBidSum > 1.005) {
      // Short arb: sell all YES for more than $1
      arbType = 'short';
      grossProfitRate = (yesBidSum - 1) / 1;
    }

    // Net profit = gross profit for NegRisk (0% taker fee on most)
    const netProfitRate = grossProfitRate;

    // Calculate max executable size (limited by lowest liquidity)
    const minLiquidity = Math.min(...outcomes.map((o) => o.liquidity));
    const maxExecutableSize = minLiquidity * outcomes.length * 0.5; // Use 50% of depth

    // Total liquidity
    const totalLiquidity = outcomes.reduce((sum, o) => sum + o.liquidity, 0);

    return {
      eventId: event.id,
      title: event.title,
      slug: event.slug,
      outcomes,
      yesPriceSum,
      yesAskSum,
      yesBidSum,
      arbType,
      grossProfitRate,
      netProfitRate,
      estimatedProfit100: netProfitRate * 100,
      totalLiquidity,
      maxExecutableSize,
      endDate: event.endDate,
      lastScan: Date.now(),
    };
  }

  private log(message: string): void {
    if (this.config.enableLogging) {
      console.log(`[NegRiskArbService] ${message}`);
    }
  }

  private logStats(): void {
    this.log(`\n${'='.repeat(50)}`);
    this.log('SESSION STATISTICS');
    this.log('='.repeat(50));
    this.log(`Scans Performed: ${this.stats.scansPerformed}`);
    this.log(`Opportunities Found: ${this.stats.opportunitiesFound}`);
    this.log(`Executions: ${this.stats.executionsSucceeded}/${this.stats.executionsAttempted}`);
    this.log(`Total Profit: $${this.stats.totalProfit.toFixed(2)}`);
    this.log(`Events Tracked: ${this.trackedEvents.size}`);
    this.log('='.repeat(50));
  }
}

// ===== Helper Functions =====

/**
 * Calculate NegRisk arbitrage profit
 *
 * @param yesPrices - Array of YES prices for all outcomes
 * @returns Arbitrage analysis
 */
export function calculateNegRiskArbitrage(yesPrices: number[]): {
  arbType: 'long' | 'short' | 'none';
  yesPriceSum: number;
  profitRate: number;
  profitPer100: number;
} {
  const yesPriceSum = yesPrices.reduce((sum, p) => sum + p, 0);

  if (yesPriceSum < 0.995) {
    const profitRate = (1 - yesPriceSum) / yesPriceSum;
    return {
      arbType: 'long',
      yesPriceSum,
      profitRate,
      profitPer100: profitRate * 100,
    };
  }

  if (yesPriceSum > 1.005) {
    const profitRate = (yesPriceSum - 1) / 1;
    return {
      arbType: 'short',
      yesPriceSum,
      profitRate,
      profitPer100: profitRate * 100,
    };
  }

  return {
    arbType: 'none',
    yesPriceSum,
    profitRate: 0,
    profitPer100: 0,
  };
}
