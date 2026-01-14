/**
 * Sports Arbitrage Orchestrator
 *
 * Unified service that coordinates sports arbitrage across:
 * - Binary sports markets (game outcomes, spreads, over/under)
 * - NegRisk multi-outcome events (championships, tournaments)
 *
 * Features:
 * - Continuous market scanning
 * - Automatic strategy routing (binary vs NegRisk)
 * - Priority ranking by profit potential
 * - Position management across multiple markets
 * - Real-time opportunity notifications
 *
 * @example
 * ```typescript
 * const orchestrator = new SportsArbOrchestrator({
 *   privateKey: '0x...',
 *   autoExecute: false,
 *   minProfitRate: 0.005,
 * });
 *
 * orchestrator.on('opportunity', (opp) => {
 *   console.log(`${opp.type}: ${opp.name} +${(opp.profitRate * 100).toFixed(2)}%`);
 * });
 *
 * await orchestrator.start();
 * ```
 */

import { EventEmitter } from 'events';
import {
  SportsMarketScanner,
  type SportMarket,
  type NegRiskSportsEvent,
  type SportsScanOptions,
  type SportsLeague,
} from './sports-scanner.js';
import {
  ArbitrageService,
  SPORTS_ARB_CONFIG,
  createSportsMarketConfig,
  type ArbitrageMarketConfig,
  type ArbitrageServiceConfig,
} from './arbitrage-service.js';
import {
  NegRiskArbService,
  type NegRiskArbServiceConfig,
  type NegRiskEvent,
  type NegRiskExecutionResult,
} from './negrisk-arb-service.js';
import { MarketService } from './market-service.js';
import { GammaApiClient } from '../clients/gamma-api.js';
import { RateLimiter } from '../core/rate-limiter.js';
import { createUnifiedCache, type UnifiedCache } from '../core/unified-cache.js';

// ===== Types =====

/**
 * Orchestrator configuration
 */
export interface SportsArbOrchestratorConfig {
  /** Private key for trading */
  privateKey?: string;
  /** RPC URL for CTF operations */
  rpcUrl?: string;
  /** Minimum profit rate for opportunities (default: 0.005 = 0.5%) */
  minProfitRate?: number;
  /** Auto-execute trades when opportunities are found (default: false) */
  autoExecute?: boolean;
  /** Scan interval in milliseconds (default: 30000 = 30 seconds) */
  scanIntervalMs?: number;
  /** Leagues to monitor (default: all) */
  leagues?: SportsLeague[];
  /** Enable binary market arbitrage (default: true) */
  enableBinaryArb?: boolean;
  /** Enable NegRisk multi-outcome arbitrage (default: true) */
  enableNegRiskArb?: boolean;
  /** Minimum 24h volume for markets (default: 1000) */
  minVolume24h?: number;
  /** Minimum liquidity (default: 100) */
  minLiquidity?: number;
  /** Maximum concurrent binary arbitrage monitors (default: 5) */
  maxConcurrentBinaryMonitors?: number;
  /** Enable logging (default: true) */
  enableLogging?: boolean;
}

/**
 * Unified opportunity type
 */
export interface SportsArbOpportunity {
  /** Unique opportunity ID */
  id: string;
  /** Opportunity type */
  type: 'binary' | 'negrisk';
  /** Market/event name */
  name: string;
  /** Profit rate (0.01 = 1%) */
  profitRate: number;
  /** Profit percentage */
  profitPercent: number;
  /** Estimated profit on $100 */
  estimatedProfit100: number;
  /** Available size (limited by liquidity) */
  maxSize: number;
  /** League/category */
  league: SportsLeague;
  /** Arbitrage direction */
  arbType: 'long' | 'short';
  /** Timestamp */
  timestamp: number;
  /** Original binary market (if type === 'binary') */
  binaryMarket?: SportMarket;
  /** Original NegRisk event (if type === 'negrisk') */
  negRiskEvent?: NegRiskEvent;
}

/**
 * Orchestrator status
 */
export interface OrchestratorStatus {
  /** Whether orchestrator is running */
  isRunning: boolean;
  /** Start time */
  startTime: number;
  /** Running time in ms */
  runningTimeMs: number;
  /** Number of scans performed */
  scansPerformed: number;
  /** Total opportunities found */
  opportunitiesFound: number;
  /** Executions attempted */
  executionsAttempted: number;
  /** Successful executions */
  executionsSucceeded: number;
  /** Total profit */
  totalProfit: number;
  /** Currently monitored binary markets */
  activeBinaryMonitors: number;
  /** Tracked NegRisk events */
  trackedNegRiskEvents: number;
  /** Current opportunities */
  currentOpportunities: SportsArbOpportunity[];
}

/**
 * Orchestrator events
 */
export interface SportsArbOrchestratorEvents {
  started: () => void;
  stopped: () => void;
  scan: (opportunities: SportsArbOpportunity[]) => void;
  opportunity: (opportunity: SportsArbOpportunity) => void;
  execution: (result: {
    opportunity: SportsArbOpportunity;
    success: boolean;
    profit: number;
    error?: string;
  }) => void;
  error: (error: Error) => void;
}

// ===== Orchestrator Implementation =====

export class SportsArbOrchestrator extends EventEmitter {
  private config: Required<Omit<SportsArbOrchestratorConfig, 'privateKey' | 'rpcUrl' | 'leagues'>> & {
    privateKey?: string;
    rpcUrl?: string;
    leagues?: SportsLeague[];
  };

  private sportsScanner: SportsMarketScanner;
  private negRiskService: NegRiskArbService | null = null;
  private activeBinaryServices: Map<string, ArbitrageService> = new Map();
  private marketService: MarketService;
  private rateLimiter: RateLimiter;
  private cache: UnifiedCache;

  private isRunning = false;
  private scanTimer: ReturnType<typeof setInterval> | null = null;
  private currentOpportunities: SportsArbOpportunity[] = [];

  private stats = {
    startTime: 0,
    scansPerformed: 0,
    opportunitiesFound: 0,
    executionsAttempted: 0,
    executionsSucceeded: 0,
    totalProfit: 0,
  };

  constructor(config: SportsArbOrchestratorConfig = {}) {
    super();

    this.config = {
      privateKey: config.privateKey,
      rpcUrl: config.rpcUrl || 'https://polygon-rpc.com',
      minProfitRate: config.minProfitRate ?? 0.005,
      autoExecute: config.autoExecute ?? false,
      scanIntervalMs: config.scanIntervalMs ?? 30000,
      leagues: config.leagues,
      enableBinaryArb: config.enableBinaryArb ?? true,
      enableNegRiskArb: config.enableNegRiskArb ?? true,
      minVolume24h: config.minVolume24h ?? 1000,
      minLiquidity: config.minLiquidity ?? 100,
      maxConcurrentBinaryMonitors: config.maxConcurrentBinaryMonitors ?? 5,
      enableLogging: config.enableLogging ?? true,
    };

    // Initialize shared infrastructure
    this.rateLimiter = new RateLimiter();
    this.cache = createUnifiedCache();

    const gammaApi = new GammaApiClient(this.rateLimiter, this.cache);
    this.sportsScanner = new SportsMarketScanner(gammaApi, this.rateLimiter, this.cache);
    this.marketService = new MarketService(gammaApi, undefined, this.rateLimiter, this.cache);

    // Initialize NegRisk service if enabled
    if (this.config.enableNegRiskArb) {
      this.negRiskService = new NegRiskArbService({
        privateKey: this.config.privateKey,
        rpcUrl: this.config.rpcUrl,
        profitThreshold: this.config.minProfitRate,
        autoExecute: false, // We'll handle execution ourselves
        enableLogging: false, // We'll do our own logging
        minLiquidityPerMarket: this.config.minLiquidity,
      });

      // Forward NegRisk opportunities
      this.negRiskService.on('opportunity', (event: NegRiskEvent) => {
        const opportunity = this.convertNegRiskToOpportunity(event);
        this.handleOpportunity(opportunity);
      });
    }
  }

  // ===== Public API =====

  /**
   * Start the orchestrator
   */
  async start(): Promise<void> {
    if (this.isRunning) {
      throw new Error('SportsArbOrchestrator is already running');
    }

    this.isRunning = true;
    this.stats.startTime = Date.now();

    this.log('Starting Sports Arbitrage Orchestrator...');
    this.log(`Profit Threshold: ${(this.config.minProfitRate * 100).toFixed(2)}%`);
    this.log(`Auto Execute: ${this.config.autoExecute ? 'YES' : 'NO'}`);
    this.log(`Binary Arb: ${this.config.enableBinaryArb ? 'ON' : 'OFF'}`);
    this.log(`NegRisk Arb: ${this.config.enableNegRiskArb ? 'ON' : 'OFF'}`);

    if (this.config.leagues) {
      this.log(`Leagues: ${this.config.leagues.join(', ')}`);
    } else {
      this.log('Leagues: ALL');
    }

    // Start NegRisk service
    if (this.negRiskService && this.config.enableNegRiskArb) {
      await this.negRiskService.start();
    }

    // Perform initial scan
    await this.scan();

    // Start periodic scanning
    this.scanTimer = setInterval(() => this.scan(), this.config.scanIntervalMs);

    this.emit('started');
  }

  /**
   * Stop the orchestrator
   */
  async stop(): Promise<void> {
    if (!this.isRunning) return;

    this.isRunning = false;

    // Stop scan timer
    if (this.scanTimer) {
      clearInterval(this.scanTimer);
      this.scanTimer = null;
    }

    // Stop NegRisk service
    if (this.negRiskService) {
      this.negRiskService.stop();
    }

    // Stop all binary services
    for (const [id, service] of this.activeBinaryServices) {
      await service.stop();
      this.activeBinaryServices.delete(id);
    }

    this.log('Stopped');
    this.logStats();

    this.emit('stopped');
  }

  /**
   * Perform a manual scan
   */
  async scan(): Promise<SportsArbOpportunity[]> {
    this.stats.scansPerformed++;
    const opportunities: SportsArbOpportunity[] = [];

    try {
      // Scan sports markets
      const scanOptions: SportsScanOptions = {
        leagues: this.config.leagues,
        minVolume24h: this.config.minVolume24h,
        minLiquidity: this.config.minLiquidity,
        activeOnly: true,
        includeNegRisk: false, // NegRisk handled separately
      };

      if (this.config.enableBinaryArb) {
        const result = await this.sportsScanner.scan(scanOptions);

        // Find binary markets with potential arbitrage
        const binaryArbs = await this.sportsScanner.findBinaryArbitrageOpportunities(scanOptions);

        for (const market of binaryArbs) {
          const opportunity = await this.convertBinaryToOpportunity(market);
          if (opportunity && opportunity.profitRate >= this.config.minProfitRate) {
            opportunities.push(opportunity);
          }
        }
      }

      // NegRisk opportunities come through event listener
      // But we can also get current opportunities from the service
      if (this.negRiskService && this.config.enableNegRiskArb) {
        const negRiskOpps = this.negRiskService.getOpportunities(this.config.minProfitRate);
        for (const event of negRiskOpps) {
          opportunities.push(this.convertNegRiskToOpportunity(event));
        }
      }

      // Sort by profit rate (descending)
      opportunities.sort((a, b) => b.profitRate - a.profitRate);

      // Update current opportunities
      this.currentOpportunities = opportunities;

      // Emit scan event
      this.emit('scan', opportunities);

      // Handle new opportunities
      for (const opp of opportunities) {
        this.handleOpportunity(opp);
      }

      return opportunities;
    } catch (error: any) {
      this.log(`Scan error: ${error.message}`);
      this.emit('error', error);
      return [];
    }
  }

  /**
   * Execute an opportunity
   */
  async execute(opportunity: SportsArbOpportunity): Promise<{
    success: boolean;
    profit: number;
    error?: string;
  }> {
    this.stats.executionsAttempted++;

    try {
      if (opportunity.type === 'binary' && opportunity.binaryMarket) {
        return await this.executeBinaryArb(opportunity);
      } else if (opportunity.type === 'negrisk' && opportunity.negRiskEvent) {
        return await this.executeNegRiskArb(opportunity);
      } else {
        return { success: false, profit: 0, error: 'Invalid opportunity type' };
      }
    } catch (error: any) {
      return { success: false, profit: 0, error: error.message };
    }
  }

  /**
   * Get current status
   */
  getStatus(): OrchestratorStatus {
    return {
      isRunning: this.isRunning,
      startTime: this.stats.startTime,
      runningTimeMs: this.isRunning ? Date.now() - this.stats.startTime : 0,
      scansPerformed: this.stats.scansPerformed,
      opportunitiesFound: this.stats.opportunitiesFound,
      executionsAttempted: this.stats.executionsAttempted,
      executionsSucceeded: this.stats.executionsSucceeded,
      totalProfit: this.stats.totalProfit,
      activeBinaryMonitors: this.activeBinaryServices.size,
      trackedNegRiskEvents: this.negRiskService?.getTrackedEvents().length || 0,
      currentOpportunities: this.currentOpportunities,
    };
  }

  /**
   * Get current opportunities
   */
  getOpportunities(): SportsArbOpportunity[] {
    return [...this.currentOpportunities];
  }

  /**
   * Get top opportunities by profit rate
   */
  getTopOpportunities(limit = 5): SportsArbOpportunity[] {
    return this.currentOpportunities.slice(0, limit);
  }

  // ===== Private Methods =====

  private async convertBinaryToOpportunity(market: SportMarket): Promise<SportsArbOpportunity | null> {
    try {
      // Get CLOB market data for token IDs
      const clobMarket = await this.marketService.getClobMarket(market.raw.conditionId);
      if (!clobMarket) return null;

      // Get orderbook for accurate prices
      const orderbook = await this.marketService.getProcessedOrderbook(market.raw.conditionId);

      const { longArbProfit, shortArbProfit } = orderbook.summary;
      const profitRate = Math.max(longArbProfit, shortArbProfit);
      const arbType: 'long' | 'short' = longArbProfit >= shortArbProfit ? 'long' : 'short';

      if (profitRate <= 0) return null;

      // Calculate max size based on orderbook depth
      const minAskSize = Math.min(orderbook.yes.askSize, orderbook.no.askSize);
      const minBidSize = Math.min(orderbook.yes.bidSize, orderbook.no.bidSize);
      const maxSize = arbType === 'long' ? minAskSize : minBidSize;

      return {
        id: `binary-${market.raw.conditionId}`,
        type: 'binary',
        name: market.raw.question,
        profitRate,
        profitPercent: profitRate * 100,
        estimatedProfit100: profitRate * 100,
        maxSize: maxSize * 100, // Approximate USDC value
        league: market.league,
        arbType,
        timestamp: Date.now(),
        binaryMarket: market,
      };
    } catch {
      return null;
    }
  }

  private convertNegRiskToOpportunity(event: NegRiskEvent): SportsArbOpportunity {
    // Detect league from event title
    const title = event.title.toLowerCase();
    let league: SportsLeague = 'OTHER';

    if (title.includes('nba') || title.includes('basketball')) {
      league = 'NBA';
    } else if (title.includes('nfl') || title.includes('super bowl')) {
      league = 'NFL';
    } else if (title.includes('nhl') || title.includes('stanley cup')) {
      league = 'NHL';
    } else if (title.includes('mlb') || title.includes('world series')) {
      league = 'MLB';
    } else if (title.includes('ufc') || title.includes('mma')) {
      league = 'UFC';
    } else if (title.includes('soccer') || title.includes('premier league')) {
      league = 'SOCCER';
    }

    return {
      id: `negrisk-${event.eventId}`,
      type: 'negrisk',
      name: event.title,
      profitRate: event.netProfitRate,
      profitPercent: event.netProfitRate * 100,
      estimatedProfit100: event.estimatedProfit100,
      maxSize: event.maxExecutableSize,
      league,
      arbType: event.arbType === 'none' ? 'long' : event.arbType,
      timestamp: Date.now(),
      negRiskEvent: event,
    };
  }

  private handleOpportunity(opportunity: SportsArbOpportunity): void {
    this.stats.opportunitiesFound++;

    this.log(`\n${'!'.repeat(60)}`);
    this.log(`${opportunity.type.toUpperCase()} ARB: ${opportunity.name}`);
    this.log(`Type: ${opportunity.arbType.toUpperCase()}`);
    this.log(`League: ${opportunity.league}`);
    this.log(`Profit: ${opportunity.profitPercent.toFixed(2)}%`);
    this.log(`Est. Profit on $100: $${opportunity.estimatedProfit100.toFixed(2)}`);
    this.log(`Max Size: $${opportunity.maxSize.toFixed(2)}`);
    this.log('!'.repeat(60));

    this.emit('opportunity', opportunity);

    // Auto-execute if enabled
    if (this.config.autoExecute && this.config.privateKey) {
      this.execute(opportunity).then((result) => {
        this.emit('execution', { opportunity, ...result });
      });
    }
  }

  private async executeBinaryArb(opportunity: SportsArbOpportunity): Promise<{
    success: boolean;
    profit: number;
    error?: string;
  }> {
    if (!opportunity.binaryMarket || !this.config.privateKey) {
      return { success: false, profit: 0, error: 'No market or private key' };
    }

    const market = opportunity.binaryMarket;

    try {
      // Get token IDs
      const clobMarket = await this.marketService.getClobMarket(market.raw.conditionId);
      if (!clobMarket) {
        return { success: false, profit: 0, error: 'Could not get CLOB market' };
      }

      // Create a temporary ArbitrageService for execution
      const arbService = new ArbitrageService({
        privateKey: this.config.privateKey,
        rpcUrl: this.config.rpcUrl,
        ...SPORTS_ARB_CONFIG,
        autoExecute: false,
        enableLogging: false,
      });

      const marketConfig: ArbitrageMarketConfig = createSportsMarketConfig(market, {
        yesTokenId: clobMarket.tokens[0].tokenId,
        noTokenId: clobMarket.tokens[1].tokenId,
      });

      // Start the service to initialize trading
      await arbService.start(marketConfig);

      // Check for opportunity and execute
      const opp = arbService.checkOpportunity();
      if (opp) {
        const result = await arbService.execute(opp);
        await arbService.stop();

        if (result.success) {
          this.stats.executionsSucceeded++;
          this.stats.totalProfit += result.profit;
          return { success: true, profit: result.profit };
        } else {
          return { success: false, profit: 0, error: result.error };
        }
      } else {
        await arbService.stop();
        return { success: false, profit: 0, error: 'Opportunity disappeared' };
      }
    } catch (error: any) {
      return { success: false, profit: 0, error: error.message };
    }
  }

  private async executeNegRiskArb(opportunity: SportsArbOpportunity): Promise<{
    success: boolean;
    profit: number;
    error?: string;
  }> {
    if (!opportunity.negRiskEvent || !this.negRiskService) {
      return { success: false, profit: 0, error: 'No event or NegRisk service' };
    }

    const result = await this.negRiskService.execute(opportunity.negRiskEvent);

    if (result.success) {
      this.stats.executionsSucceeded++;
      this.stats.totalProfit += result.profit;
      return { success: true, profit: result.profit };
    } else {
      return { success: false, profit: 0, error: result.error };
    }
  }

  private log(message: string): void {
    if (this.config.enableLogging) {
      console.log(`[SportsArbOrchestrator] ${message}`);
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
    this.log('='.repeat(50));
  }
}
