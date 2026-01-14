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
  sumTarget: 0.92,        // ⚡ FIXED: ~8% gross = ~2% net after 6% fees (was 1.05 = guaranteed loss!)
  dipThreshold: 0.01,     // ⚡ LOWERED: 1% dip threshold catches more opportunities
  windowMinutes: 15,      // ⚡ Full market duration (was 10)
  slidingWindowMs: 2000,  // ⚡ LOWERED: 2 seconds for faster detection (was 3000)
  maxSlippage: 0.02,
  minProfitRate: 0.02,    // ⚡ FIXED: Require 2% net profit minimum (was 0 = no check)
  leg2TimeoutSeconds: 60, // ⚡ LOWERED: 60s timeout for faster rotation
  enableSurge: true,
  surgeThreshold: 0.01,   // ⚡ Match dipThreshold (1%)
  autoMerge: true,
  autoExecute: true,      // ✅ MUST be true to actually trade!
  executionCooldown: 500, // ⚡ FIX #4: Reduced from 2000ms for faster Leg2 execution
  splitOrders: 1,         // Don't split orders (avoids share errors)
  orderIntervalMs: 500,
  debug: true,            // ⚡ ENABLED for troubleshooting (includes rotation logs)
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
  if (marketEndTime) {
    endTime = typeof marketEndTime === 'number' ? marketEndTime : marketEndTime.getTime();
  } else {
    // Fallback: calculate from duration (legacy behavior - not accurate!)
    endTime = now + durationMinutes * 60 * 1000;
  }

  return {
    roundId,
    startTime: now,
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
