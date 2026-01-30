/**
 * Dip Arbitrage Service Types
 *
 * 暴跌套利服务类型定义
 *
 * 策略原理：
 * 在 Polymarket 的 BTC/ETH/SOL/XRP UP/DOWN 短期市场中：
 *
 * 1. 每个市场有一个 "price to beat"（开盘时的 Chainlink 价格）
 * 2. 结算规则：
 *    - UP 赢：结束时价格 >= price to beat
 *    - DOWN 赢：结束时价格 < price to beat
 *
 * 3. 套利流程：
 *    - Leg1：检测暴跌 → 买入暴跌侧
 *    - Leg2：等待对冲条件 → 买入另一侧
 *    - 利润：总成本 < $1 时获得无风险利润
 */

// ============= Configuration =============

/**
 * DipArbService 配置
 */
export interface DipArbServiceConfig {
  /**
   * 每次交易的份额数量
   * @default 20
   */
  shares?: number;

  /**
   * 对冲价格阈值 (sumTarget)
   * 只有当 leg1Price + leg2Price <= sumTarget 时才执行对冲
   * @default 0.95
   */
  sumTarget?: number;

  /**
   * 暴跌触发阈值
   * 价格相对开盘价下跌超过此比例时触发 Leg1
   * 0.15 = 15%
   * @default 0.15
   */
  dipThreshold?: number;

  /**
   * 交易窗口（分钟）
   * 每轮开始后，只在此时间窗口内触发 Leg1
   * @default 2
   */
  windowMinutes?: number;

  /**
   * 滑动窗口时长（毫秒）
   * 用于检测瞬时暴跌：比较当前价格与 N 毫秒前的价格
   *
   * 重要：这是策略的核心参数！
   * - 3000ms (3秒) 跌 15% = 异常事件 ✅ 触发
   * - 5分钟跌 15% = 趋势下行 ❌ 不触发
   *
   * @default 3000
   */
  slidingWindowMs?: number;

  /**
   * 最大滑点
   * 下单价格 = 市场价 * (1 + maxSlippage)
   * @default 0.02
   */
  maxSlippage?: number;

  /**
   * 最小利润率
   * 只有预期利润率 > minProfitRate 时才生成信号
   * @default 0.03
   */
  minProfitRate?: number;

  /**
   * Leg1 成交后等待 Leg2 的最大时间（秒）
   * 超时后放弃当前轮次
   * @default 300
   */
  leg2TimeoutSeconds?: number;

  /**
   * 启用暴涨检测
   * 当 token 价格暴涨时，买入对手 token（预期均值回归）
   * @default true
   */
  enableSurge?: boolean;

  /**
   * 暴涨触发阈值
   * 价格相对开盘价上涨超过此比例时触发
   * @default 0.15
   */
  surgeThreshold?: number;

  /**
   * 完成双腿后自动合并回 USDC
   * YES + NO tokens → USDC
   * @default true
   */
  autoMerge?: boolean;

  /**
   * Maximum price allowed for Leg1 entry
   * Prevents buying dips on high-priced assets where risk/reward is poor
   * e.g., buying a dip from 0.99 to 0.98 risks 0.98 for 0.02 upside
   * @default 0.75
   */
  maxLeg1Price?: number;

  /**
   * Maximum market asymmetry allowed for trading
   * When one side exceeds this threshold (e.g., UP > 80%), the market is too resolved
   * DipArb in asymmetric markets is gambling, not arbitrage - wait for rotation
   * @default 0.80 (don't trade when either side > 80%)
   */
  maxMarketAsymmetry?: number;

  /**
   * Minimum depth (shares available) on opposite side for Leg1 entry
   * Prevents entering when opposite side has poor liquidity for Leg2
   * If opposite side doesn't have enough depth, Leg2 will be expensive/impossible
   * @default 100 (shares)
   */
  minOppositeSideDepth?: number;

  /**
   * Maximum allowed bid-ask spread on opposite side (as ratio of price)
   * Calculated as: (secondBestAsk - bestAsk) / bestAsk
   * Higher spreads mean worse execution for Leg2
   * @default 0.05 (5%)
   */
  maxOppositeSideSpread?: number;

  /**
   * Maximum required Leg2 price drop percentage for entry
   *
   * When the opposite side price is too high relative to sumTarget,
   * it would need to drop significantly for Leg2 to be profitable.
   * This prevents entering positions where Leg2 is mathematically unlikely.
   *
   * Example: If buying UP at $0.21, DOWN is at $0.79, and sumTarget is $0.88:
   * - Max Leg2 price: $0.88 - $0.21 = $0.67
   * - Required drop: $0.79 - $0.67 = $0.12 (15.2% of $0.79)
   * - If maxRequiredLeg2Drop is 0.15 (15%), this entry is rejected
   *
   * Lower values = stricter filtering (fewer entries, higher quality)
   * Higher values = more permissive (more entries, some may timeout)
   *
   * @default 0.15 (15% - requires opposite side to drop max 15%)
   */
  maxRequiredLeg2Drop?: number;

  /**
   * Minimum price allowed for dip side entry
   * Prevents buying nearly-resolved tokens (e.g., $0.005) which are lottery tickets
   * When a token is at $0.005, the market has essentially resolved
   * @default 0.10 (don't buy tokens under $0.10)
   */
  minDipSidePrice?: number;

  /**
   * Maximum concurrent leg1 positions allowed
   * Prevents capital lockup in too many pending (unhedged) positions
   * When this limit is reached, new leg1 signals are skipped
   * @default 10
   */
  maxOpenPositions?: number;

  /**
   * 自动执行交易

   * 检测到信号后自动下单
   * @default false
   */
  autoExecute?: boolean;

  /**
   * 执行冷却时间（毫秒）
   * 两次交易之间的最小间隔
   * @default 3000
   */
  executionCooldown?: number;

  /**
   * 拆分订单数量
   * 将 shares 拆分成多笔订单执行
   * 例如: shares=30, splitOrders=3 → 每笔 10 shares
   * @default 1 (不拆分)
   */
  splitOrders?: number;

  /**
   * 拆分订单间隔（毫秒）
   * 多笔订单之间的间隔时间
   * @default 500
   */
  orderIntervalMs?: number;

  /**
   * 启用调试日志
   * @default false
   */
  debug?: boolean;

  /**
   * 自定义日志处理函数
   * 如果设置，所有日志将通过此函数输出
   * @example
   * logHandler: (msg) => {
   *   console.log(`[${Date.now()}] ${msg}`);
   *   logs.push(msg);
   * }
   */
  logHandler?: (message: string) => void;

  // ============= Fee Configuration =============

  /**
   * Taker fee rate for the market
   * For crypto UP/DOWN markets, this is 3% (0.03)
   * Fee is applied to both legs, so total overhead is ~6%
   * @default 0.03
   */
  takerFeeRate?: number;

  /**
   * Use fee-adjusted profit calculations
   * When true, sumTarget and minProfitRate account for fees
   * @default true
   */
  useFeeAdjustedProfit?: boolean;

  // ============= Binance Momentum Configuration =============

  /**
   * Paper trading mode - simulate trades without sending real orders
   * When enabled, orders are simulated at signal prices instead of sent to CLOB
   * Useful for testing strategies without risking real funds
   * @default true
   */
  paperMode?: boolean;

  /**
   * Enable Binance momentum check before Leg1 execution
   * When enabled, validates that external exchange price movement
   * supports the expected direction before entering a position
   * @default true
   */
  enableBinanceMomentum?: boolean;

  /**
   * Minimum Binance price change percentage to confirm momentum
   * A dip signal on Polymarket should correlate with Binance movement
   * e.g., if DOWN dip detected, Binance should be dropping too
   * @default 0.5 (0.5% price movement)
   */
  binanceMomentumThreshold?: number;

  /**
   * Time window for Binance momentum check (in ms)
   * How far back to look for price movement
   * @default 60000 (1 minute)
   */
  binanceMomentumWindowMs?: number;

  /**
   * Require Binance momentum confirmation for Leg1
   * If false, momentum check is advisory only (logged but not blocking)
   * If true, Leg1 execution is blocked without momentum confirmation
   * @default false
   */
  requireBinanceMomentum?: boolean;

  // ============= Settlement Awareness Configuration =============

  /**
   * Enable smart settlement logic
   * When enabled, positions trending favorably will hold for settlement
   * rather than timing out at a loss
   * @default true
   */
  favorSettlement?: boolean;

  /**
   * Minimum win probability threshold to hold for settlement
   * Only hold if estimated win probability >= this threshold
   * @default 0.50 (50% - conservative)
   */
  settlementHoldThreshold?: number;

  /**
   * Minutes before market end to always hold (regardless of probability)
   * If time to market end is less than this, always hold for settlement
   * @default 3
   */
  minTimeToEndForHold?: number;

  /**
   * Extra seconds to wait after market end for settlement processing
   * @default 300 (5 minutes)
   */
  settlementWaitBuffer?: number;

  /**
   * Enable Binance momentum validation for settlement decisions
   * When enabled, uses real-time Binance data to validate hold decisions
   * @default false
   */
  enableSettlementMomentum?: boolean;

  /**
   * Minimum momentum strength for settlement hold decisions
   * Only factors into hold decision if momentum strength exceeds this
   * @default 0.3
   */
  settlementMomentumThreshold?: number;

  // ============= P1.1: Chainlink Momentum Configuration =============

  /**
   * P1.1: Enable Chainlink momentum validation for Leg1 signals
   * When enabled, validates that underlying price movement supports the dip direction
   * Uses historical Chainlink prices instead of Binance (which may be geofenced)
   * @default true
   */
  enableChainlinkMomentum?: boolean;

  /**
   * P1.1: Time window for Chainlink momentum check (in seconds)
   * Compares current price vs price N seconds ago
   * @default 30
   */
  chainlinkMomentumWindowSec?: number;

  /**
   * P1.1: Minimum Chainlink price change to confirm momentum
   * Percentage change required to validate dip direction
   * @default 0.001 (0.1%)
   */
  chainlinkMomentumThreshold?: number;

  /**
   * P1.1: Require Chainlink momentum confirmation for Leg1
   * If true, Leg1 signals are rejected without momentum confirmation
   * If false, momentum check is advisory only (logged but not blocking)
   * @default false
   */
  requireChainlinkMomentum?: boolean;

  // ============= P1.3: Two-Tier Early Entry Configuration =============

  /**
   * P1.3: Enable two-tier early entry system
   * When enabled, enters 50% position at tier1 threshold with momentum confirmation,
   * then adds remaining 50% at full threshold (tier2)
   * @default false
   */
  enableTieredEntry?: boolean;

  /**
   * P1.3: Tier 1 dip threshold (early entry)
   * Enter with partial position when dip reaches this threshold
   * Must be lower than main dipThreshold
   * @default 0.015 (1.5%)
   */
  tier1DipThreshold?: number;

  /**
   * P1.3: Share ratio for Tier 1 entry
   * Portion of total shares to enter at Tier 1
   * Remaining shares enter at Tier 2 (full dipThreshold)
   * @default 0.5 (50%)
   */
  tier1ShareRatio?: number;

  /**
   * P1.3: Require momentum confirmation for Tier 1 entry
   * If true, Tier 1 entry only triggers if momentum aligns
   * Tier 2 does not require momentum (uses existing dipThreshold logic)
   * @default true
   */
  requireMomentumForTier1?: boolean;

  // ============= P2.1: Order Flow Analysis Configuration =============

  /**
   * P2.1: Enable order flow analysis for predictive entry
   * When enabled, monitors orderbook microstructure to detect sell pressure
   * before price actually moves, generating predictive signals
   * @default false
   */
  enableOrderFlowPrediction?: boolean;

  /**
   * P2.1: Imbalance ratio threshold to trigger predictive signal
   * imbalanceRatio = (bidPressure - askPressure) / (bidPressure + askPressure)
   * Negative values indicate sell pressure (more asks than bids)
   * @default -0.33 (ask pressure > bid pressure by 50%)
   */
  orderFlowImbalanceThreshold?: number;

  /**
   * P2.1: Share ratio for predictive (lower confidence) signals
   * Predictive signals use reduced position size due to lower certainty
   * @default 0.3 (30% of normal shares)
   */
  predictiveShareRatio?: number;

  /**
   * P2.1: Minimum depth on top levels to consider for pressure calculation
   * Sum of sizes on top N levels of each side
   * @default 3 (top 3 levels)
   */
  orderFlowDepthLevels?: number;

  /**
   * P2.1: Time window to detect spread widening (ms)
   * If spread increased within this window, may indicate impending move
   * @default 500
   */
  spreadWideningWindowMs?: number;

  /**
   * P2.1: Minimum value for large cancellation detection
   * Cancelled bids above this size (in USD) are significant
   * @default 500
   */
  largeCancellationThreshold?: number;

  // ============= P2.2: Dynamic Fee Optimization Configuration =============

  /**
   * P2.2: Enable maker orders for Leg2 to reduce fees
   * When enabled, attempts to use limit orders before falling back to market
   * @default false
   */
  enableMakerOrders?: boolean;

  /**
   * P2.2: Minimum time (seconds) until timeout to attempt maker order
   * Only attempts limit order if enough time remains
   * @default 60
   */
  minTimeForMakerOrder?: number;

  /**
   * P2.2: Price improvement for maker order (inside spread)
   * Places limit order at bestAsk * (1 - makerPriceImprovement)
   * @default 0.001 (0.1% inside spread)
   */
  makerPriceImprovement?: number;

  /**
   * P2.2: Maximum wait time (seconds) for maker order to fill
   * If not filled within this time, converts to market order
   * @default 30
   */
  makerOrderTimeout?: number;

  // ============= P2.3: High-Probability Timeout Extension Configuration =============

  /**
   * P2.3: Enable holding positions for settlement when probability is high
   * When enabled, extends timeout for high-probability positions near settlement
   * @default false
   */
  enableSettlementHoldExtension?: boolean;

  /**
   * P2.3: Minimum win probability to hold for settlement
   * Only holds if estimated win probability exceeds this threshold
   * @default 0.70 (70%)
   */
  settlementExtensionWinProbThreshold?: number;

  /**
   * P2.3: Maximum time to settlement (minutes) to consider holding
   * Only holds if market ends within this time window
   * @default 5
   */
  settlementExtensionMaxMinutes?: number;
}

/**
 * 内部配置类型（不包含 logHandler，因为它是纯可选的回调函数）
 */
export type DipArbConfigInternal = Required<Omit<DipArbServiceConfig, 'logHandler'>> & {
  logHandler?: (message: string) => void;
};

/**
 * Default taker fee for crypto UP/DOWN markets (3%)
 * This fee applies to EACH leg of the trade, so roundtrip cost is ~6%
 */
export const DIP_ARB_CRYPTO_TAKER_FEE = 0.03;

/**
 * 默认配置
 * 
 * IMPORTANT: With 3% taker fee per leg (~6% total), you need:
 * - sumTarget <= 0.90 for 10%+ gross profit to clear ~4%+ net profit
 * - minProfitRate >= 0.08 to ensure profitability after fees
 */
/**
 * ⚡ OPTIMIZED CONFIG - Updated 2026-01-12 for More Opportunities
 * - Lower thresholds to catch more dips
 * - Debug mode enabled for troubleshooting
 * - Full window monitoring (15 minutes)
 */
export const DEFAULT_DIP_ARB_CONFIG: DipArbConfigInternal = {
  shares: 50,             // ⚡ Increased from 20 for more capital deployment
  sumTarget: 0.86,        // P0.2: Tightened from 0.88 → 0.86 (~14% gross = ~7.5% net after 6% fees)
  dipThreshold: 0.025,    // ⚡ AGGRESSIVE: 2.5% dip threshold catches momentum shifts
  windowMinutes: 15,      // ⚡ Full market duration (was 10)
  slidingWindowMs: 1000,  // ⚡ AGGRESSIVE: 1 second for fastest detection (was 2000)
  maxSlippage: 0.02,
  minProfitRate: 0.02,    // ⚡ FIXED: Require 2% net profit minimum (was 0 = no check)
  leg2TimeoutSeconds: 180, // ⚡ AGGRESSIVE: 3min timeout reduces bleed from timeouts
  enableSurge: true,
  surgeThreshold: 0.025,  // ⚡ Match dipThreshold (2.5%)
  autoMerge: true,
  autoExecute: true,      // ✅ MUST be true to actually trade!
  executionCooldown: 200, // ⚡ AGGRESSIVE: 200ms cooldown for maximum speed (was 500ms)
  splitOrders: 1,         // Don't split orders (avoids share errors)
  orderIntervalMs: 500,
  debug: false,           // ⚡ Disabled to reduce log noise (enable for troubleshooting)
  // Paper trading - simulate trades without real orders
  paperMode: true,        // ✅ Default to paper mode - safe testing without real funds
  // Fee configuration
  takerFeeRate: DIP_ARB_CRYPTO_TAKER_FEE,
  useFeeAdjustedProfit: true,
  // Binance momentum configuration
  enableBinanceMomentum: false,         // ❌ Disabled by default - causes issues with geofencing/region locks
  binanceMomentumThreshold: 0.5,        // 0.5% minimum price movement on Binance
  binanceMomentumWindowMs: 60000,       // 1 minute lookback
  requireBinanceMomentum: false,        // Advisory by default, not blocking
  maxLeg1Price: 0.95,                   // ⚡ RAISED: 0.95 - allow nearly all prices
  maxMarketAsymmetry: 0.75,             // ⚠️ CRITICAL: Don't trade when UP or DOWN > 75%
  // Spread/depth checks for opposite side (Leg2 viability)
  minOppositeSideDepth: 100,            // ✅ Require at least 100 shares on opposite side
  maxOppositeSideSpread: 0.05,          // ✅ Max 5% spread on opposite side
  minDipSidePrice: 0.03,                // ⚡ LOWERED: 0.03 to include more markets
  maxOpenPositions: 25,                 // ⚡ INCREASED: 25 concurrent positions (was 10)
  // Leg2 feasibility check - prevents entering polarized markets
  maxRequiredLeg2Drop: 0.15,            // ⚡ NEW: Max 15% drop required for Leg2 profitability
  // Settlement awareness configuration
  favorSettlement: true,                // ✅ Enable smart settlement logic
  settlementHoldThreshold: 0.50,        // 50% win probability threshold (conservative)
  minTimeToEndForHold: 3,               // Always hold if <3 min to market end
  settlementWaitBuffer: 300,            // 5 min buffer after market end
  enableSettlementMomentum: false,      // Momentum validation off by default
  settlementMomentumThreshold: 0.3,     // 30% momentum strength threshold
  // P1.1: Chainlink momentum validation (replaces Binance momentum)
  enableChainlinkMomentum: true,        // ✅ Enable Chainlink momentum check for Leg1
  chainlinkMomentumWindowSec: 30,       // 30 second lookback
  chainlinkMomentumThreshold: 0.001,    // 0.1% minimum price change
  requireChainlinkMomentum: false,      // Advisory by default (logged but not blocking)
  // P1.3: Two-tier early entry system
  enableTieredEntry: false,             // Disabled by default - enable for early entry
  tier1DipThreshold: 0.015,             // 1.5% dip threshold for early entry
  tier1ShareRatio: 0.5,                 // 50% of shares at Tier 1
  requireMomentumForTier1: true,        // Require momentum for Tier 1 entry
  // P2.1: Order flow analysis for predictive entry
  enableOrderFlowPrediction: false,     // Disabled by default - experimental
  orderFlowImbalanceThreshold: -0.33,   // Ask pressure > bid by 50% triggers signal
  predictiveShareRatio: 0.3,            // 30% of normal shares for predictive signals
  orderFlowDepthLevels: 3,              // Sum top 3 levels for pressure calculation
  spreadWideningWindowMs: 500,          // 500ms window for spread widening detection
  largeCancellationThreshold: 500,      // $500 minimum for significant cancellation
  // P2.2: Dynamic fee optimization (maker orders)
  enableMakerOrders: false,             // Disabled by default - requires more testing
  minTimeForMakerOrder: 60,             // Need 60s+ remaining to attempt maker
  makerPriceImprovement: 0.001,         // 0.1% inside spread
  makerOrderTimeout: 30,                // 30s wait before converting to market
  // P2.3: High-probability timeout extension
  enableSettlementHoldExtension: false, // Disabled by default
  settlementExtensionWinProbThreshold: 0.70, // 70% win prob threshold
  settlementExtensionMaxMinutes: 5,     // Only hold if <5min to settlement
};

// ============= Market Configuration =============

/** 支持的底层资产 */
export type DipArbUnderlying = 'BTC' | 'ETH' | 'SOL' | 'XRP';

/** 市场时长 (分钟) - 5m, 15m, 1hr, 4hr, daily */
export type DipArbDuration = 5 | 15 | 60 | 240 | 1440;

/** 市场时长字符串格式 */
export type DipArbDurationString = '5m' | '15m' | '1h' | '4h' | 'daily';

/** Duration priority (lower = higher priority) */
export const DURATION_PRIORITY: Record<DipArbDurationString, number> = {
  '5m': 0,
  '15m': 1,
  '1h': 2,
  '4h': 3,
  'daily': 4,
};

/** Duration to minutes mapping */
export const DURATION_MINUTES: Record<DipArbDurationString, DipArbDuration> = {
  '5m': 5,
  '15m': 15,
  '1h': 60,
  '4h': 240,
  'daily': 1440,
};

/** Minutes to duration string mapping */
export const MINUTES_TO_DURATION: Record<DipArbDuration, DipArbDurationString> = {
  5: '5m',
  15: '15m',
  60: '1h',
  240: '4h',
  1440: 'daily',
};

/** Default fallback chain - priority order for duration fallback */
export const DURATION_FALLBACK_CHAIN: DipArbDurationString[] = ['15m', '1h', '4h', 'daily'];

/** Coin short name to full name mapping (for hourly/daily slug generation) */
export const COIN_TO_FULL_NAME: Record<DipArbUnderlying, string> = {
  BTC: 'bitcoin',
  ETH: 'ethereum',
  SOL: 'solana',
  XRP: 'xrp',
};

/** Full name to coin short name mapping */
export const FULL_NAME_TO_COIN: Record<string, DipArbUnderlying> = {
  bitcoin: 'BTC',
  ethereum: 'ETH',
  solana: 'SOL',
  xrp: 'XRP',
};

/**
 * 市场配置
 */
export interface DipArbMarketConfig {
  /** 市场名称（用于日志） */
  name: string;
  /** 市场 slug (e.g., 'btc-updown-15m-1767165300') */
  slug: string;
  /** Condition ID */
  conditionId: string;
  /** UP token ID */
  upTokenId: string;
  /** DOWN token ID */
  downTokenId: string;
  /** 底层资产 */
  underlying: DipArbUnderlying;
  /** 市场时长（分钟） */
  durationMinutes: DipArbDuration;
  /** 市场结束时间 */
  endTime: Date;
  /**
   * Price to beat (threshold) from Polymarket's groupItemThreshold
   * This is the starting price that determines UP vs DOWN resolution
   * e.g., 95000 for BTC at $95,000
   */
  priceToBeat?: number;
}

// ============= Round State =============

/** 轮次阶段 */
export type DipArbPhase = 'waiting' | 'leg1_filled' | 'completed' | 'expired';

/** 交易侧 */
export type DipArbSide = 'UP' | 'DOWN';

/**
 * Leg 信息
 */
export interface DipArbLegInfo {
  /** 买入侧 */
  side: DipArbSide;
  /** 成交价格 */
  price: number;
  /** 份额数量 */
  shares: number;
  /** 成交时间 */
  timestamp: number;
  /** Token ID */
  tokenId: string;
}

/**
 * 轮次状态
 */
export interface DipArbRoundState {
  /** 轮次 ID */
  roundId: string;
  /** 轮次开始时间 (Unix ms) */
  startTime: number;
  /** 轮次结束时间 (Unix ms) */
  endTime: number;
  /** Price to Beat - 开盘时的底层资产价格（Chainlink） */
  priceToBeat: number;
  /** 开盘时的 token 价格 */
  openPrices: {
    up: number;
    down: number;
  };
  /** 当前阶段 */
  phase: DipArbPhase;
  /** Leg1 信息（如果已成交） */
  leg1?: DipArbLegInfo;
  /** Leg2 信息（如果已成交） */
  leg2?: DipArbLegInfo;
  /** 总成本 */
  totalCost?: number;
  /** 实际利润 */
  profit?: number;
}

// ============= P1.1: Chainlink Momentum =============

/**
 * P1.1: Result of Chainlink momentum check
 * Used to validate dip direction against underlying price movement
 */
export interface ChainlinkMomentumResult {
  /** Whether momentum confirms the signal direction */
  confirmed: boolean;
  /** Direction of underlying price movement */
  direction: 'bullish' | 'bearish' | 'neutral';
  /** Price change percentage over the window */
  changePercent: number;
  /** Current underlying price */
  currentPrice: number;
  /** Historical price from momentum window */
  historicalPrice: number;
  /** Reason for confirmation/rejection */
  reason: string;
}

// ============= P2.1: Order Flow Analysis =============

/**
 * P2.1: Order flow metrics for predictive entry detection
 * Tracks orderbook microstructure to predict price movements
 */
export interface OrderFlowMetrics {
  /** Sum of bid sizes on top N levels */
  bidPressure: number;
  /** Sum of ask sizes on top N levels */
  askPressure: number;
  /** Imbalance ratio: (bid - ask) / (bid + ask), -1 to 1 */
  imbalanceRatio: number;
  /** Whether spread widened in the last window */
  spreadWidening: boolean;
  /** Whether a large bid was cancelled in the last 200ms */
  largeCancellation: boolean;
  /** Current spread (ask - bid) */
  currentSpread: number;
  /** Historical spread for comparison */
  previousSpread: number;
  /** Timestamp of metrics */
  timestamp: number;
}

/**
 * P2.1: Order flow signal for predictive entry
 * Generated when orderbook microstructure suggests impending price move
 */
export interface OrderFlowSignal {
  /** Side that is likely to experience price drop */
  predictedDropSide: DipArbSide;
  /** Confidence level (0-1, lower than standard signals) */
  confidence: number;
  /** Order flow metrics that triggered the signal */
  metrics: OrderFlowMetrics;
  /** Reason for signal generation */
  reason: string;
  /** Timestamp of signal */
  timestamp: number;
}

/**
 * P2.1: Orderbook delta tracking for cancellation detection
 */
export interface OrderbookDelta {
  /** Token side (UP or DOWN) */
  side: DipArbSide;
  /** Bid level changes */
  bidChanges: Array<{ price: number; sizeDelta: number }>;
  /** Ask level changes */
  askChanges: Array<{ price: number; sizeDelta: number }>;
  /** Total value of cancelled bids */
  cancelledBidValue: number;
  /** Timestamp */
  timestamp: number;
}

// ============= Signal Confidence (P0.3) =============

/**
 * P0.3: Signal confidence scoring for position sizing
 * Higher confidence = larger position size
 */
export interface SignalConfidence {
  /** Overall confidence score (0.0 - 1.0) */
  score: number;
  /** Individual factor scores */
  factors: {
    /** Drop magnitude factor (0-1): larger dips = higher confidence */
    dropMagnitude: number;
    /** Opposite side liquidity factor (0-1): better depth = higher confidence */
    oppositeLiquidity: number;
    /** Spread quality factor (0-1): tighter spread = higher confidence */
    spreadQuality: number;
    /** Time remaining factor (0-1): more time = higher confidence */
    timeRemaining: number;
  };
}

/**
 * P0.3: Confidence-weighted position sizing config
 */
export interface ConfidencePositionConfig {
  /** Enable confidence-weighted sizing */
  enabled: boolean;
  /** Minimum shares (at 0 confidence) - default 20 */
  minShares: number;
  /** Maximum shares (at 1.0 confidence) - default 80 */
  maxShares: number;
  /** Minimum confidence required to trade - default 0.2 */
  minConfidence: number;
}

// ============= Signals =============

/**
 * Leg1 信号
 */
export interface DipArbLeg1Signal {
  type: 'leg1';
  /** 轮次 ID */
  roundId: string;
  /** 买入侧 */
  dipSide: DipArbSide;
  /** 当前价格 */
  currentPrice: number;
  /** 开盘价格 */
  openPrice: number;
  /** 下跌/上涨幅度 */
  dropPercent: number;
  /** 目标价格（包含滑点） */
  targetPrice: number;
  /** 份额数量 */
  shares: number;
  /** Token ID */
  tokenId: string;
  /** 对手侧当前 ask 价格 */
  oppositeAsk: number;
  /** 预估总成本 */
  estimatedTotalCost: number;
  /** 预估利润率 */
  estimatedProfitRate: number;
  /** 信号来源 */
  source: 'dip' | 'surge' | 'mispricing';
  /** BTC 信息（用于定价偏差检测） */
  btcInfo?: {
    btcPrice: number;
    priceToBeat: number;
    btcChangePercent: number;
    estimatedWinRate: number;
  };
  /** P0.3: Signal confidence for position sizing */
  confidence?: SignalConfidence;
}

/**
 * Leg2 信号
 */
export interface DipArbLeg2Signal {
  type: 'leg2';
  /** 轮次 ID */
  roundId: string;
  /** 对冲侧 */
  hedgeSide: DipArbSide;
  /** Leg1 信息 */
  leg1: DipArbLegInfo;
  /** 当前价格 */
  currentPrice: number;
  /** 目标价格（包含滑点） */
  targetPrice: number;
  /** 总成本 (leg1 + leg2) */
  totalCost: number;
  /** 预期利润率 */
  expectedProfitRate: number;
  /** 份额数量 */
  shares: number;
  /** Token ID */
  tokenId: string;
}

/** 信号类型 */
export type DipArbSignal = DipArbLeg1Signal | DipArbLeg2Signal;

// ============= Execution Results =============

/**
 * 执行结果
 */
export interface DipArbExecutionResult {
  /** 是否成功 */
  success: boolean;
  /** 执行的 leg */
  leg: 'leg1' | 'leg2' | 'merge' | 'exit';
  /** 轮次 ID */
  roundId: string;
  /** 交易侧 */
  side?: DipArbSide;
  /** 成交价格 */
  price?: number;
  /** 成交份额 */
  shares?: number;
  /** 订单 ID */
  orderId?: string;
  /** 交易哈希（merge 操作） */
  txHash?: string;
  /** 错误信息 */
  error?: string;
  /** 执行时间（毫秒） */
  executionTimeMs: number;
}

/**
 * 轮次完成结果
 */
export interface DipArbRoundResult {
  /** 轮次 ID */
  roundId: string;
  /** 状态 */
  status: 'completed' | 'expired' | 'partial';
  /** Leg1 信息 */
  leg1?: DipArbLegInfo;
  /** Leg2 信息 */
  leg2?: DipArbLegInfo;
  /** 总成本 */
  totalCost?: number;
  /** 实际利润 */
  profit?: number;
  /** 利润率 */
  profitRate?: number;
  /** 是否已合并 */
  merged: boolean;
  /** 合并交易哈希 */
  mergeTxHash?: string;
  /** Leg1 退出结果（Leg2 超时时） */
  exitResult?: DipArbExecutionResult | null;
}

// ============= Statistics =============

/**
 * 服务统计
 */
export interface DipArbStats {
  /** 开始时间 */
  startTime: number;
  /** 运行时长（毫秒） */
  runningTimeMs: number;
  /** 监控的轮次数 */
  roundsMonitored: number;
  /** 完成的轮次数 */
  roundsCompleted: number;
  /** 成功的轮次数（双腿完成） */
  roundsSuccessful: number;
  /** 过期的轮次数 */
  roundsExpired: number;
  /** 检测到的信号数 */
  signalsDetected: number;
  /** Leg1 成交次数 */
  leg1Filled: number;
  /** Leg2 成交次数 */
  leg2Filled: number;
  /** 总花费 (USDC) */
  totalSpent: number;
  /** 总收益 (USDC) */
  totalProfit: number;
  /** 平均利润率 */
  avgProfitRate: number;
  /** 当前轮次信息 */
  currentRound?: {
    roundId: string;
    phase: DipArbPhase;
    priceToBeat: number;
    leg1?: { side: DipArbSide; price: number };
  };
}

// ============= Events =============

/**
 * 新轮次事件数据
 */
export interface DipArbNewRoundEvent {
  roundId: string;
  priceToBeat: number;
  upOpen: number;
  downOpen: number;
  startTime: number;
  endTime: number;
}

/**
 * 价格更新事件数据
 */
export interface DipArbPriceUpdateEvent {
  underlying: DipArbUnderlying;
  value: number;
  priceToBeat: number;
  changePercent: number;
}

/**
 * 服务事件
 */
export interface DipArbServiceEvents {
  started: (market: DipArbMarketConfig) => void;
  stopped: () => void;
  newRound: (event: DipArbNewRoundEvent) => void;
  signal: (signal: DipArbSignal) => void;
  execution: (result: DipArbExecutionResult) => void;
  roundComplete: (result: DipArbRoundResult) => void;
  priceUpdate: (event: DipArbPriceUpdateEvent) => void;
  error: (error: Error) => void;
}

// ============= Settlement Awareness Types =============

/**
 * Rule evaluation result for a single settlement rule
 */
export interface SettlementRuleEvaluation {
  passed: boolean;
}

/**
 * Near settlement rule evaluation
 */
export interface NearSettlementEvaluation extends SettlementRuleEvaluation {
  timeToEndMin: number;
  threshold: number;
}

/**
 * High probability rule evaluation
 */
export interface HighProbabilityEvaluation extends SettlementRuleEvaluation {
  winProb: number;
  threshold: number;
}

/**
 * Positive EV rule evaluation
 */
export interface PositiveEVEvaluation extends SettlementRuleEvaluation {
  settlementEV: number;
  exitValue: number;
  evRatio: number;
}

/**
 * Chainlink alignment rule evaluation
 */
export interface ChainlinkAlignedEvaluation extends SettlementRuleEvaluation {
  delta: number;
  currentPrice: number;
  priceToBeat: number;
  side: DipArbSide;
}

/**
 * Momentum rule evaluation
 */
export interface MomentumEvaluation extends SettlementRuleEvaluation {
  strength: number;
  threshold: number;
  enabled: boolean;
}

/**
 * All rule evaluations for a settlement decision
 */
export interface SettlementRuleEvaluations {
  nearSettlement: NearSettlementEvaluation;
  highProbability: HighProbabilityEvaluation;
  positiveEV: PositiveEVEvaluation;
  chainlinkAligned: ChainlinkAlignedEvaluation;
  momentumFavorable: MomentumEvaluation;
}

/**
 * Entry context for a settlement decision
 */
export interface SettlementEntryContext {
  leg1Side: DipArbSide;
  leg1EntryPrice: number;
  leg1Shares: number;
  leg1Cost: number;
  enteredAt: number;
}

/**
 * Market snapshot at time of settlement decision
 */
export interface SettlementMarketSnapshot {
  upPrice: number;
  downPrice: number;
  priceToBeat: number;
  currentChainlinkPrice: number;
  marketEndTime: number;
}

/**
 * Enhanced settlement decision with full rule evaluations
 *
 * This interface captures all details about why a HOLD/EXIT decision was made,
 * enabling observability and future auto-tuning of settlement parameters.
 */
export interface EnhancedSettlementDecision {
  /** Decision: HOLD for settlement or EXIT with timeout */
  decision: 'HOLD' | 'EXIT';
  /** Primary reason for the decision */
  reason:
    | 'disabled'
    | 'no_position'
    | 'near_settlement'
    | 'market_ended'
    | 'high_probability'
    | 'positive_ev'
    | 'chainlink_aligned'
    | 'momentum_favorable'
    | 'no_edge';
  /** Confidence level (0-1) */
  confidence: number;

  /** All rule evaluations with their inputs/outputs */
  ruleEvaluations: SettlementRuleEvaluations;

  /** Entry context (leg1 position details) */
  entryContext: SettlementEntryContext;

  /** Market snapshot at decision time */
  marketSnapshot: SettlementMarketSnapshot;

  /** Round ID */
  roundId: string;
  /** Market name */
  marketName: string;
  /** Underlying asset */
  underlying: DipArbUnderlying;
  /** Whether this is a paper trade */
  isPaper: boolean;
  /** Decision timestamp */
  timestamp: number;
}

/**
 * Settlement outcome tracking
 *
 * After a market settles, this records whether the decision was correct
 * and calculates both actual and counterfactual profits.
 */
export interface SettlementOutcome {
  /** Round ID to link back to decision */
  roundId: string;
  /** The decision that was made */
  decision: 'HOLD' | 'EXIT';
  /** Actual outcome */
  outcome: 'WIN' | 'LOSS';
  /** Actual profit/loss from the decision */
  actualProfit: number;
  /** What the opposite decision would have yielded */
  counterfactualProfit: number;
  /** Which side won at settlement */
  settlementSide: DipArbSide;
  /** Final Chainlink price at settlement */
  settlementPrice: number;
  /** Settlement timestamp */
  settledAt: number;
  /** Whether this is a paper trade */
  isPaper: boolean;
  /** Underlying asset */
  underlying: DipArbUnderlying;
  /** Market name */
  marketName: string;
}

// ============= Scan Options =============

/**
 * 市场扫描选项
 */
export interface DipArbScanOptions {
  /** 筛选底层资产 */
  coin?: DipArbUnderlying | 'all';
  /** 筛选时长 */
  duration?: DipArbDurationString | 'all';
  /** 距离结束的最小分钟数 */
  minMinutesUntilEnd?: number;
  /** 距离结束的最大分钟数 */
  maxMinutesUntilEnd?: number;
  /** 返回数量限制 */
  limit?: number;
}

/**
 * 自动启动选项
 */
export interface DipArbFindAndStartOptions {
  /** 偏好的底层资产 */
  coin?: DipArbUnderlying;
  /** 偏好的时长 */
  preferDuration?: DipArbDurationString;
}

/**
 * 自动轮换配置
 */
export interface DipArbAutoRotateConfig {
  /** 是否启用自动轮换 */
  enabled: boolean;
  /** 监控的底层资产列表 */
  underlyings: DipArbUnderlying[];
  /** 偏好的时长 */
  duration: DipArbDurationString;
  /** 市场结束前多少分钟开始寻找下一个市场 */
  preloadMinutes?: number;
  /** 市场结束后自动结算 */
  autoSettle?: boolean;
  /** 结算策略: 'redeem' 赎回 (等结算) 或 'sell' 立即卖出 */
  settleStrategy?: 'redeem' | 'sell';
  /** Redeem 等待时间（分钟）- 市场结束后等待 Oracle 结算的时间，默认 5 分钟 */
  redeemWaitMinutes?: number;
  /** Redeem 重试间隔（秒）- 每次检查 resolution 的间隔，默认 30 秒 */
  redeemRetryIntervalSeconds?: number;
  /**
   * Enable duration fallback when preferred duration unavailable
   * @default true
   */
  enableFallback?: boolean;
  /**
   * Duration priority order for fallback (first available wins)
   * @default ['15m', '1h', '4h', 'daily']
   */
  durationPriority?: DipArbDurationString[];
  /**
   * Polling interval for checking higher-priority markets (ms)
   * Only used when trading at a fallback duration
   * @default 60000 (1 minute)
   */
  upgradeCheckIntervalMs?: number;
  /**
   * Whether to immediately switch to higher-priority market when available
   * or wait for current market to end
   * @default false (wait for current market)
   */
  immediateUpgrade?: boolean;
}

/**
 * 默认自动轮换配置
 */
export const DEFAULT_AUTO_ROTATE_CONFIG: Required<DipArbAutoRotateConfig> = {
  enabled: false,
  underlyings: ['BTC', 'ETH', 'SOL', 'XRP'],
  duration: '15m',
  preloadMinutes: 2,
  autoSettle: true,
  settleStrategy: 'redeem',
  redeemWaitMinutes: 5,
  redeemRetryIntervalSeconds: 30,
  enableFallback: true,
  durationPriority: ['15m', '1h', '4h', 'daily'],
  upgradeCheckIntervalMs: 60000,
  immediateUpgrade: false,
};

/**
 * 结算结果
 */
export interface DipArbSettleResult {
  /** 是否成功 */
  success: boolean;
  /** 结算策略 */
  strategy: 'redeem' | 'sell';
  /** 市场信息 */
  market?: DipArbMarketConfig;
  /** UP token 数量 */
  upBalance?: number;
  /** DOWN token 数量 */
  downBalance?: number;
  /** 收到的金额 (USDC) */
  amountReceived?: number;
  /** 交易哈希 */
  txHash?: string;
  /** 错误信息 */
  error?: string;
  /** 执行时间（毫秒） */
  executionTimeMs: number;
}

/**
 * 待赎回的仓位
 * 用于跟踪市场结束后需要赎回的仓位
 */
export interface DipArbPendingRedemption {
  /** 市场配置 */
  market: DipArbMarketConfig;
  /** 轮次状态（包含持仓信息） */
  round: DipArbRoundState;
  /** 市场结束时间 */
  marketEndTime: number;
  /** 添加到队列的时间 */
  addedAt: number;
  /** 重试次数 */
  retryCount: number;
  /** 最后一次尝试时间 */
  lastRetryAt?: number;
}

/**
 * 市场轮换事件
 */
export interface DipArbRotateEvent {
  /** 旧市场 condition ID */
  previousMarket?: string;
  /** 新市场 condition ID */
  newMarket: string;
  /** 轮换原因 */
  reason: 'marketEnded' | 'manual' | 'error' | 'upgrade';
  /** 时间戳 */
  timestamp: number;
  /** 结算结果（如果有） */
  settleResult?: DipArbSettleResult;
}

// ============= Helper Functions =============

/**
 * 创建初始统计
 */
export function createDipArbInitialStats(): DipArbStats {
  return {
    startTime: Date.now(),
    runningTimeMs: 0,
    roundsMonitored: 0,
    roundsCompleted: 0,
    roundsSuccessful: 0,
    roundsExpired: 0,
    signalsDetected: 0,
    leg1Filled: 0,
    leg2Filled: 0,
    totalSpent: 0,
    totalProfit: 0,
    avgProfitRate: 0,
  };
}

/**
 * 创建新轮次状态
 */
/**
 * Create a new DipArb round state
 *
 * @param roundId - Unique round identifier
 * @param priceToBeat - Chainlink price at round start (for UP/DOWN resolution)
 * @param upPrice - Current UP token price
 * @param downPrice - Current DOWN token price
 * @param marketEndTime - ACTUAL market end time from Polymarket (use this!)
 * @param durationMinutes - Fallback duration if marketEndTime not provided (legacy)
 */
export function createDipArbRoundState(
  roundId: string,
  priceToBeat: number,
  upPrice: number,
  downPrice: number,
  marketEndTime?: number | Date,
  durationMinutes: number = 15
): DipArbRoundState {
  const now = Date.now();

  // Use actual market end time if provided, otherwise calculate from duration
  let endTime: number;
  let startTime: number;

  if (marketEndTime) {
    endTime = typeof marketEndTime === 'number' ? marketEndTime : marketEndTime.getTime();
    // Calculate startTime from endTime - this is the ACTUAL market window start
    // Not when we started monitoring (which could be mid-window after rotation)
    startTime = endTime - (durationMinutes * 60 * 1000);
  } else {
    // Fallback: calculate from duration (legacy behavior - not accurate!)
    startTime = now;
    endTime = now + durationMinutes * 60 * 1000;
  }

  return {
    roundId,
    startTime,
    endTime,
    priceToBeat,
    openPrices: {
      up: upPrice,
      down: downPrice,
    },
    phase: 'waiting',
  };
}

/**
 * 计算利润率 (GROSS - before fees)
 * @deprecated Use calculateDipArbNetProfitRate for fee-adjusted calculations
 */
export function calculateDipArbProfitRate(totalCost: number): number {
  if (totalCost >= 1 || totalCost <= 0) return 0;
  return (1 - totalCost) / totalCost;
}

/**
 * Calculate NET profit rate after trading fees
 * 
 * For DipArb with 3% taker fee per leg:
 * - Gross profit = 1 - totalCost
 * - Fee overhead = totalCost * feeRate * 2 (fees on both legs)
 * - Net profit = gross profit - fees
 * 
 * @param totalCost - Sum of leg1Price + leg2Price
 * @param feeRate - Taker fee rate (default: 0.03 = 3%)
 * @returns Net profit rate after fees (can be negative)
 * 
 * @example
 * // Cost = 0.92 (8% gross profit)
 * // Fees = 0.92 * 0.03 * 2 = 0.0552 (5.52%)
 * // Net = 0.08 - 0.0552 = 0.0248 (2.48%)
 * calculateDipArbNetProfitRate(0.92, 0.03) // ~0.0248
 */
export function calculateDipArbNetProfitRate(
  totalCost: number,
  feeRate: number = DIP_ARB_CRYPTO_TAKER_FEE
): number {
  if (totalCost >= 1 || totalCost <= 0) return 0;
  
  const grossProfit = 1 - totalCost;
  const totalFees = totalCost * feeRate * 2; // Fees on both legs
  const netProfit = grossProfit - totalFees;
  
  // Return as a rate relative to effective cost (including fees)
  const effectiveCost = totalCost + totalFees;
  return netProfit / effectiveCost;
}

/**
 * Calculate the maximum sumTarget that ensures a minimum net profit rate
 * 
 * @param minNetProfitRate - Minimum desired net profit (e.g., 0.02 = 2%)
 * @param feeRate - Taker fee rate (default: 0.03 = 3%)
 * @returns Maximum sumTarget value
 * 
 * @example
 * // For 2% net profit with 3% fees:
 * getMaxSumTargetForNetProfit(0.02, 0.03) // ~0.896
 */
export function getMaxSumTargetForNetProfit(
  minNetProfitRate: number,
  feeRate: number = DIP_ARB_CRYPTO_TAKER_FEE
): number {
  // Solve for totalCost where netProfitRate = minNetProfitRate
  // netProfit = (1 - cost) - cost * feeRate * 2
  // netProfit = 1 - cost * (1 + 2 * feeRate)
  // netProfitRate = netProfit / (cost + cost * 2 * feeRate)
  // netProfitRate = (1 - cost * (1 + 2*f)) / (cost * (1 + 2*f))
  // Let k = 1 + 2*f
  // r = (1 - cost*k) / (cost*k)
  // r * cost * k = 1 - cost*k
  // cost*k * (r + 1) = 1
  // cost = 1 / (k * (r + 1))
  const k = 1 + 2 * feeRate;
  return 1 / (k * (1 + minNetProfitRate));
}

/**
 * Calculate NET profit for a completed DipArb leg2 trade
 *
 * Formula: netProfit = (1 - totalCost) - (totalCost * feeRate * 2)
 *
 * @param leg1Price - Fill price for leg1
 * @param leg2Price - Fill price for leg2
 * @param shares - Number of shares traded
 * @param feeRate - Taker fee rate (default: DIP_ARB_CRYPTO_TAKER_FEE)
 * @returns NET profit in dollars (after fees)
 */
export function calculateDipArbLeg2NetProfit(
  leg1Price: number,
  leg2Price: number,
  shares: number,
  feeRate: number = DIP_ARB_CRYPTO_TAKER_FEE
): number {
  const totalCost = leg1Price + leg2Price;
  const grossProfit = 1 - totalCost;
  const totalFees = totalCost * feeRate * 2; // Fees on both legs
  const netProfitPerShare = grossProfit - totalFees;
  return netProfitPerShare * shares;
}

/**
 * Calculate NET profit for a settlement win (holding leg1 to expiry)
 *
 * At settlement, you receive $1 per share if you win.
 * You paid: leg1Cost + (leg1Cost * feeRate) when entering.
 *
 * @param leg1Cost - Cost paid for leg1 (shares * price, not including fee)
 * @param shares - Number of shares
 * @param feeRate - Taker fee rate (default: DIP_ARB_CRYPTO_TAKER_FEE)
 * @returns NET profit in dollars
 */
export function calculateDipArbSettlementWinProfit(
  leg1Cost: number,
  shares: number,
  feeRate: number = DIP_ARB_CRYPTO_TAKER_FEE
): number {
  const received = shares * 1; // $1 per winning share
  const entryFee = leg1Cost * feeRate;
  return received - leg1Cost - entryFee;
}

/**
 * Calculate exit value after selling position
 *
 * Exit is a single SELL transaction, so only ONE fee (not two).
 * Note: Entry fee was already paid, this is just the exit fee.
 *
 * @param exitPrice - Price received per share
 * @param shares - Number of shares
 * @param feeRate - Taker fee rate (default: DIP_ARB_CRYPTO_TAKER_FEE)
 * @returns Value received after exit fee
 */
export function calculateDipArbExitValue(
  exitPrice: number,
  shares: number,
  feeRate: number = DIP_ARB_CRYPTO_TAKER_FEE
): number {
  return exitPrice * shares * (1 - feeRate); // Single fee
}

/**
 * 计算基于底层资产价格变化的"真实"胜率
 *
 * @param currentPrice - 当前价格
 * @param priceToBeat - 开盘价格
 * @returns UP 的真实胜率估计 (0-1)
 */
export function estimateUpWinRate(currentPrice: number, priceToBeat: number): number {
  if (priceToBeat <= 0) return 0.5;

  const priceChange = (currentPrice - priceToBeat) / priceToBeat;

  // 简单模型：价格变化 1% 对应胜率变化约 10%
  const sensitivity = 10;
  const winRateShift = priceChange * sensitivity;

  // 限制在 [0.05, 0.95] 范围内
  return Math.max(0.05, Math.min(0.95, 0.5 + winRateShift));
}

/**
 * 检测定价偏差
 *
 * @param tokenPrice - token 当前价格（隐含胜率）
 * @param estimatedWinRate - 基于价格估计的真实胜率
 * @returns 偏差程度（正数 = 被低估，负数 = 被高估）
 */
export function detectMispricing(tokenPrice: number, estimatedWinRate: number): number {
  return estimatedWinRate - tokenPrice;
}

/**
 * 从 slug 解析底层资产
 *
 * Slug patterns:
 * - Short-term: btc-updown-15m-{timestamp}, eth-updown-4h-{timestamp}
 * - Hourly: bitcoin-up-or-down-january-13-6pm-et
 * - Daily: bitcoin-up-or-down-on-january-14
 */
export function parseUnderlyingFromSlug(slug: string): DipArbUnderlying {
  const lower = slug.toLowerCase();

  // Short-form slugs (btc-, eth-, sol-, xrp-)
  if (lower.startsWith('btc-')) return 'BTC';
  if (lower.startsWith('eth-')) return 'ETH';
  if (lower.startsWith('sol-')) return 'SOL';
  if (lower.startsWith('xrp-')) return 'XRP';

  // Human-readable slugs (bitcoin-, ethereum-, solana-, xrp-)
  if (lower.startsWith('bitcoin')) return 'BTC';
  if (lower.startsWith('ethereum')) return 'ETH';
  if (lower.startsWith('solana')) return 'SOL';
  if (lower.startsWith('xrp')) return 'XRP';

  return 'BTC'; // default
}

/**
 * 从 slug 解析时长
 *
 * Slug patterns:
 * - 5m/15m: btc-updown-{5m|15m}-{timestamp}
 * - 4h: btc-updown-4h-{timestamp}
 * - Hourly: bitcoin-up-or-down-january-13-6pm-et
 * - Daily: bitcoin-up-or-down-on-january-14
 */
export function parseDurationFromSlug(slug: string): DipArbDuration {
  const lower = slug.toLowerCase();

  // Epoch-based short-term slugs
  if (lower.includes('-5m-')) return 5;
  if (lower.includes('-15m-')) return 15;
  if (lower.includes('-4h-')) return 240;

  // Human-readable hourly: bitcoin-up-or-down-january-13-6pm-et
  if (/-up-or-down-\w+-\d+-\d+[ap]m-et$/.test(lower)) return 60;

  // Human-readable daily: bitcoin-up-or-down-on-january-14
  if (/-up-or-down-on-\w+-\d+$/.test(lower)) return 1440;

  return 15; // default
}

/**
 * 从 slug 解析时长字符串格式
 */
export function parseDurationStringFromSlug(slug: string): DipArbDurationString {
  const minutes = parseDurationFromSlug(slug);
  return MINUTES_TO_DURATION[minutes] || '15m';
}

/**
 * 类型守卫：检查是否为 Leg1 信号
 */
export function isDipArbLeg1Signal(signal: DipArbSignal): signal is DipArbLeg1Signal {
  return signal.type === 'leg1';
}

/**
 * 类型守卫：检查是否为 Leg2 信号
 */
export function isDipArbLeg2Signal(signal: DipArbSignal): signal is DipArbLeg2Signal {
  return signal.type === 'leg2';
}
