/**
 * P1.2: Market Performance Scoring System
 *
 * Tracks per-market performance metrics and provides intelligent market selection
 * based on historical profitability and reliability.
 *
 * Used by DipArbService to prioritize high-performing markets over low-performing ones.
 */

import type { DipArbUnderlying, DipArbDurationString, DipArbSide } from './dip-arb-types.js';

// ============= Types =============

/**
 * Trade result for recording market performance
 */
export interface TradeResult {
  /** Underlying asset */
  underlying: DipArbUnderlying;
  /** Market duration */
  duration: DipArbDurationString;
  /** Whether Leg1 was filled */
  leg1Filled: boolean;
  /** Whether Leg2 was filled (completed trade) */
  leg2Filled: boolean;
  /** Net profit (after fees) - only set if leg2Filled */
  netProfit?: number;
  /** Leg2 wait time in seconds - only set if leg2Filled */
  leg2WaitTimeSec?: number;
  /** Whether position was held to settlement */
  heldToSettlement?: boolean;
  /** Settlement outcome if held */
  settlementWon?: boolean;
  /** Timestamp of the trade */
  timestamp: number;
}

/**
 * Market score with performance metrics
 */
export interface MarketScore {
  /** Underlying asset */
  underlying: DipArbUnderlying;
  /** Market duration */
  duration: DipArbDurationString;
  /** Total trades attempted (Leg1 filled) */
  tradesAttempted: number;
  /** Leg2 completion rate (0-1) */
  leg2CompletionRate: number;
  /** Average net profit per completed trade */
  avgNetProfit: number;
  /** Average Leg2 wait time in seconds */
  avgLeg2WaitTimeSec: number;
  /** Volatility score (0-1): higher = more volatile = more opportunities */
  volatilityScore: number;
  /** Last time score was updated */
  lastUpdated: number;
  /** Composite score (0-1) for ranking */
  compositeScore: number;
  /** Number of completed trades (for statistical significance) */
  completedTrades: number;
}

/**
 * Internal storage for raw trade data
 */
interface MarketTradeData {
  underlying: DipArbUnderlying;
  duration: DipArbDurationString;
  trades: TradeResult[];
  lastUpdated: number;
}

// ============= Constants =============

/** Scoring weights for composite score calculation */
const SCORING_WEIGHTS = {
  leg2CompletionRate: 0.35,  // Most important - successful hedges
  normalizedProfit: 0.30,    // Profitability
  volatilityScore: 0.20,     // Opportunity frequency
  recencyBonus: 0.15,        // Recent performance matters more
};

/** Maximum trades to keep in history per market */
const MAX_TRADES_PER_MARKET = 50;

/** Recency decay - trades older than this (hours) get reduced weight */
const RECENCY_DECAY_HOURS = 24;

/** Minimum trades required for statistical significance */
const MIN_TRADES_FOR_CONFIDENCE = 5;

// ============= Market Scorer Class =============

/**
 * MarketScorer tracks performance metrics for DipArb markets
 * and provides intelligent market selection based on historical data.
 */
export class MarketScorer {
  private marketData: Map<string, MarketTradeData> = new Map();

  constructor() {
    // Initialize empty - scores build up over time
  }

  /**
   * Get unique key for market identification
   */
  private getMarketKey(underlying: DipArbUnderlying, duration: DipArbDurationString): string {
    return `${underlying}-${duration}`;
  }

  /**
   * Record a trade result for scoring
   */
  recordTrade(result: TradeResult): void {
    const key = this.getMarketKey(result.underlying, result.duration);

    let data = this.marketData.get(key);
    if (!data) {
      data = {
        underlying: result.underlying,
        duration: result.duration,
        trades: [],
        lastUpdated: Date.now(),
      };
      this.marketData.set(key, data);
    }

    // Add trade to history
    data.trades.push(result);
    data.lastUpdated = Date.now();

    // Trim to max history
    if (data.trades.length > MAX_TRADES_PER_MARKET) {
      data.trades = data.trades.slice(-MAX_TRADES_PER_MARKET);
    }
  }

  /**
   * Get score for a specific market
   */
  getScore(underlying: DipArbUnderlying, duration: DipArbDurationString): MarketScore | null {
    const key = this.getMarketKey(underlying, duration);
    const data = this.marketData.get(key);

    if (!data || data.trades.length === 0) {
      return null;
    }

    return this.calculateScore(data);
  }

  /**
   * Get the best market from available options
   * Falls back to round-robin order if no historical data
   */
  getBestMarket(
    available: Array<{ underlying: DipArbUnderlying; duration: DipArbDurationString }>
  ): { underlying: DipArbUnderlying; duration: DipArbDurationString } | null {
    if (available.length === 0) return null;
    if (available.length === 1) return available[0];

    // Get scores for all available markets
    const scored = available.map(market => ({
      market,
      score: this.getScore(market.underlying, market.duration),
    }));

    // Separate markets with sufficient data from those without
    const withData = scored.filter(
      s => s.score !== null && s.score.tradesAttempted >= MIN_TRADES_FOR_CONFIDENCE
    );
    const withoutData = scored.filter(
      s => s.score === null || s.score.tradesAttempted < MIN_TRADES_FOR_CONFIDENCE
    );

    // If we have markets with data, prefer the highest scoring one
    if (withData.length > 0) {
      withData.sort((a, b) => (b.score?.compositeScore ?? 0) - (a.score?.compositeScore ?? 0));
      return withData[0].market;
    }

    // Otherwise, return first available (round-robin fallback)
    return available[0];
  }

  /**
   * Export all scores for dashboard display
   */
  exportScores(): MarketScore[] {
    const scores: MarketScore[] = [];

    for (const data of this.marketData.values()) {
      if (data.trades.length > 0) {
        scores.push(this.calculateScore(data));
      }
    }

    // Sort by composite score descending
    scores.sort((a, b) => b.compositeScore - a.compositeScore);

    return scores;
  }

  /**
   * Clear all scoring data (useful for testing or reset)
   */
  clear(): void {
    this.marketData.clear();
  }

  /**
   * Load scores from serialized data (for persistence)
   */
  loadFromData(data: MarketTradeData[]): void {
    this.marketData.clear();
    for (const item of data) {
      const key = this.getMarketKey(item.underlying, item.duration);
      this.marketData.set(key, item);
    }
  }

  /**
   * Export raw data for persistence
   */
  exportRawData(): MarketTradeData[] {
    return Array.from(this.marketData.values());
  }

  // ============= Private Methods =============

  /**
   * Calculate score for a market from its trade data
   */
  private calculateScore(data: MarketTradeData): MarketScore {
    const trades = data.trades;
    const now = Date.now();

    // Basic counts
    const tradesAttempted = trades.filter(t => t.leg1Filled).length;
    const completedTrades = trades.filter(t => t.leg2Filled).length;

    // Leg2 completion rate
    const leg2CompletionRate = tradesAttempted > 0
      ? completedTrades / tradesAttempted
      : 0;

    // Average net profit (only from completed trades)
    const completedWithProfit = trades.filter(t => t.leg2Filled && t.netProfit !== undefined);
    const avgNetProfit = completedWithProfit.length > 0
      ? completedWithProfit.reduce((sum, t) => sum + (t.netProfit ?? 0), 0) / completedWithProfit.length
      : 0;

    // Average Leg2 wait time
    const completedWithWait = trades.filter(t => t.leg2Filled && t.leg2WaitTimeSec !== undefined);
    const avgLeg2WaitTimeSec = completedWithWait.length > 0
      ? completedWithWait.reduce((sum, t) => sum + (t.leg2WaitTimeSec ?? 0), 0) / completedWithWait.length
      : 0;

    // Volatility score: based on trade frequency and signal quality
    // More trades in recent time = higher volatility/opportunity
    const recentTrades = trades.filter(t => (now - t.timestamp) < RECENCY_DECAY_HOURS * 3600 * 1000);
    const volatilityScore = Math.min(recentTrades.length / 10, 1); // 10 trades in 24h = max score

    // Recency bonus: weight towards markets with recent activity
    const mostRecentTrade = trades.reduce((max, t) => Math.max(max, t.timestamp), 0);
    const hoursSinceLastTrade = (now - mostRecentTrade) / (1000 * 3600);
    const recencyBonus = Math.max(0, 1 - (hoursSinceLastTrade / RECENCY_DECAY_HOURS));

    // Normalize profit for scoring (map to 0-1 range)
    // Assume 5% net profit is "excellent", 0% is baseline
    const normalizedProfit = Math.max(0, Math.min(1, avgNetProfit / 0.05));

    // Composite score
    const compositeScore =
      (leg2CompletionRate * SCORING_WEIGHTS.leg2CompletionRate) +
      (normalizedProfit * SCORING_WEIGHTS.normalizedProfit) +
      (volatilityScore * SCORING_WEIGHTS.volatilityScore) +
      (recencyBonus * SCORING_WEIGHTS.recencyBonus);

    return {
      underlying: data.underlying,
      duration: data.duration,
      tradesAttempted,
      leg2CompletionRate,
      avgNetProfit,
      avgLeg2WaitTimeSec,
      volatilityScore,
      lastUpdated: data.lastUpdated,
      compositeScore,
      completedTrades,
    };
  }
}

// ============= Default Export =============

export default MarketScorer;
