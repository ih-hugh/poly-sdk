/**
 * SmartMoneyService
 *
 * 聪明钱监控和自动跟单服务
 *
 * 核心功能：
 * 1. 监听指定地址的交易 - subscribeSmartMoneyTrades()
 * 2. 自动跟单 - startAutoCopyTrading()
 * 3. 聪明钱信息获取 - getSmartMoneyList(), getSmartMoneyInfo()
 *
 * ============================================================================
 * 设计决策
 * ============================================================================
 *
 * ## 监控方式
 * 使用 Activity WebSocket，延迟 < 100ms，实测验证有效。
 *
 * ## 下单方式
 * | 方式 | 使用场景 | 特点 |
 * |------|---------|------|
 * | FOK | 小额跟单 | 全部成交或取消 |
 * | FAK | 大额跟单 | 部分成交也接受 |
 *
 * ## 重要限制
 * ⚠️ Activity WebSocket 不会广播用户自己的交易！
 * 验证跟单结果请使用 TradingService.getTrades()
 */

import type { WalletService, TimePeriod, PeriodLeaderboardEntry } from './wallet-service.js';
import type { RealtimeServiceV2, ActivityTrade } from './realtime-service-v2.js';
import type { TradingService, OrderResult } from './trading-service.js';
import type { Position } from '../clients/data-api.js';

// ============================================================================
// Types
// ============================================================================

/**
 * Smart Money wallet information
 */
export interface SmartMoneyWallet {
  address: string;
  name?: string;
  pnl: number;
  volume: number;
  score: number;
  rank?: number;
}

/**
 * Smart Money trade from Activity WebSocket
 */
export interface SmartMoneyTrade {
  traderAddress: string;
  traderName?: string;
  conditionId?: string;
  marketSlug?: string;
  side: 'BUY' | 'SELL';
  size: number;
  price: number;
  tokenId?: string;
  outcome?: string;
  txHash?: string;
  timestamp: number;
  isSmartMoney: boolean;
  smartMoneyInfo?: SmartMoneyWallet;
}

/**
 * Auto copy trading options
 */
export interface AutoCopyTradingOptions {
  /** Specific wallet addresses to follow */
  targetAddresses?: string[];
  /** Follow top N from leaderboard */
  topN?: number;

  /** Scale factor for size (0.1 = 10%) */
  sizeScale?: number;
  /** Maximum USDC per trade */
  maxSizePerTrade?: number;
  /** Maximum slippage (e.g., 0.03 = 3%) */
  maxSlippage?: number;
  /** Order type: FOK or FAK */
  orderType?: 'FOK' | 'FAK';
  /** Delay before executing (ms) */
  delay?: number;

  /** Minimum trade value to copy (USDC) */
  minTradeSize?: number;
  /** Only copy BUY or SELL trades */
  sideFilter?: 'BUY' | 'SELL';

  /** Dry run mode */
  dryRun?: boolean;

  /** Callbacks */
  onTrade?: (trade: SmartMoneyTrade, result: OrderResult) => void;
  onError?: (error: Error) => void;
}

/**
 * Auto copy trading statistics
 */
export interface AutoCopyTradingStats {
  startTime: number;
  tradesDetected: number;
  tradesExecuted: number;
  tradesSkipped: number;
  tradesFailed: number;
  totalUsdcSpent: number;
}

/**
 * Auto copy trading subscription
 */
export interface AutoCopyTradingSubscription {
  id: string;
  targetAddresses: string[];
  startTime: number;
  isActive: boolean;
  stats: AutoCopyTradingStats;
  stop: () => void;
  getStats: () => AutoCopyTradingStats;
}

/**
 * Service configuration
 */
export interface SmartMoneyServiceConfig {
  /** Minimum PnL to be considered Smart Money (default: $1000) */
  minPnl?: number;
  /** Cache TTL (default: 300000 = 5 min) */
  cacheTtl?: number;
}

// ============================================================================
// Leaderboard & Report Types
// ============================================================================

/**
 * Leaderboard query options
 */
export interface LeaderboardOptions {
  /** Time period: 'day' | 'week' | 'month' | 'all' */
  period?: TimePeriod;
  /** Maximum entries (default: 50, max: 500) */
  limit?: number;
  /** Sort by: 'pnl' | 'volume' */
  sortBy?: 'pnl' | 'volume';
  /** Pagination offset (default: 0, max: 10000) */
  offset?: number;
}

/**
 * Smart Money Leaderboard entry (simplified from PeriodLeaderboardEntry)
 */
export interface SmartMoneyLeaderboardEntry {
  address: string;
  rank: number;
  pnl: number;
  volume: number;
  tradeCount: number;
  userName?: string;
  profileImage?: string;
}

/**
 * Period ranking info
 */
export interface PeriodRanking {
  rank: number;
  pnl: number;
  volume: number;
}

/**
 * Wallet report - comprehensive wallet analysis
 */
export interface WalletReport {
  address: string;
  generatedAt: Date;

  overview: {
    totalPnL: number;
    realizedPnL: number;
    unrealizedPnL: number;
    positionCount: number;
    tradeCount: number;
    smartScore: number;
    lastActiveAt: Date;
  };

  rankings: {
    daily: PeriodRanking | null;
    weekly: PeriodRanking | null;
    monthly: PeriodRanking | null;
    allTime: PeriodRanking | null;
  };

  performance: {
    winRate: number;
    winCount: number;
    lossCount: number;
    avgPositionSize: number;
    avgWinAmount: number;
    avgLossAmount: number;
    uniqueMarkets: number;
  };

  categoryBreakdown: Array<{
    category: string;
    positionCount: number;
    totalPnl: number;
  }>;

  topPositions: Array<{
    market: string;
    slug?: string;
    outcome: string;
    size: number;
    avgPrice: number;
    currentPrice?: number;
    pnl: number;
    percentPnl?: number;
  }>;

  recentTrades: Array<{
    timestamp: number;
    side: 'BUY' | 'SELL';
    size: number;
    price: number;
    usdcSize?: number;
    // Market info
    title?: string;
    slug?: string;
    outcome?: string;
    conditionId?: string;
  }>;

  activitySummary: {
    totalBuys: number;
    totalSells: number;
    buyVolume: number;
    sellVolume: number;
    activeMarketsCount: number;
  };
}

/**
 * Wallet comparison result
 */
export interface WalletComparison {
  period: TimePeriod;
  generatedAt: Date;
  wallets: Array<{
    address: string;
    userName?: string;
    rank: number | null;
    pnl: number;
    volume: number;
    positionCount: number;
    winRate: number;
  }>;
}

// ============================================================================
// SmartMoneyService
// ============================================================================

export class SmartMoneyService {
  private walletService: WalletService;
  private realtimeService: RealtimeServiceV2;
  private tradingService: TradingService;
  private config: Required<SmartMoneyServiceConfig>;

  private smartMoneyCache: Map<string, SmartMoneyWallet> = new Map();
  private smartMoneySet: Set<string> = new Set();
  private cacheTimestamp: number = 0;

  private activeSubscription: { unsubscribe: () => void } | null = null;
  private tradeHandlers: Set<(trade: SmartMoneyTrade) => void> = new Set();

  constructor(
    walletService: WalletService,
    realtimeService: RealtimeServiceV2,
    tradingService: TradingService,
    config: SmartMoneyServiceConfig = {}
  ) {
    this.walletService = walletService;
    this.realtimeService = realtimeService;
    this.tradingService = tradingService;

    this.config = {
      minPnl: config.minPnl ?? 1000,
      cacheTtl: config.cacheTtl ?? 300000,
    };
  }

  // ============================================================================
  // Smart Money Info
  // ============================================================================

  /**
   * Get list of Smart Money wallets from leaderboard
   */
  async getSmartMoneyList(limit: number = 100): Promise<SmartMoneyWallet[]> {
    if (this.isCacheValid()) {
      return Array.from(this.smartMoneyCache.values());
    }

    const leaderboardPage = await this.walletService.getLeaderboard(0, limit);
    const entries = leaderboardPage.entries;

    const smartMoneyList: SmartMoneyWallet[] = [];

    for (let i = 0; i < entries.length; i++) {
      const trader = entries[i];
      if (trader.pnl < this.config.minPnl) continue;

      const wallet: SmartMoneyWallet = {
        address: trader.address.toLowerCase(),
        name: trader.userName,
        pnl: trader.pnl,
        volume: trader.volume,
        score: Math.min(100, Math.round((trader.pnl / 100000) * 50 + (trader.volume / 1000000) * 50)),
        rank: trader.rank ?? i + 1,
      };

      smartMoneyList.push(wallet);
      this.smartMoneyCache.set(wallet.address, wallet);
      this.smartMoneySet.add(wallet.address);
    }

    this.cacheTimestamp = Date.now();
    return smartMoneyList;
  }

  /**
   * Check if an address is Smart Money
   */
  async isSmartMoney(address: string): Promise<boolean> {
    const normalized = address.toLowerCase();
    if (this.isCacheValid()) {
      return this.smartMoneySet.has(normalized);
    }
    await this.getSmartMoneyList();
    return this.smartMoneySet.has(normalized);
  }

  /**
   * Get Smart Money info for an address
   */
  async getSmartMoneyInfo(address: string): Promise<SmartMoneyWallet | null> {
    const normalized = address.toLowerCase();
    if (this.isCacheValid() && this.smartMoneyCache.has(normalized)) {
      return this.smartMoneyCache.get(normalized)!;
    }
    await this.getSmartMoneyList();
    return this.smartMoneyCache.get(normalized) || null;
  }

  // ============================================================================
  // Trade Subscription - 监听交易
  // ============================================================================

  /**
   * Subscribe to trades from specific addresses
   *
   * @example
   * ```typescript
   * const sub = smartMoneyService.subscribeSmartMoneyTrades(
   *   (trade) => {
   *     console.log(`${trade.traderName} ${trade.side} ${trade.size} @ ${trade.price}`);
   *   },
   *   { filterAddresses: ['0x1234...', '0x5678...'] }
   * );
   *
   * // Stop listening
   * sub.unsubscribe();
   * ```
   */
  subscribeSmartMoneyTrades(
    onTrade: (trade: SmartMoneyTrade) => void,
    options: {
      filterAddresses?: string[];
      minSize?: number;
      smartMoneyOnly?: boolean;
    } = {}
  ): { id: string; unsubscribe: () => void } {
    this.tradeHandlers.add(onTrade);

    // Ensure cache is populated
    this.getSmartMoneyList().catch(() => {});

    // Start subscription if not active
    if (!this.activeSubscription) {
      this.activeSubscription = this.realtimeService.subscribeAllActivity({
        onTrade: (activityTrade: ActivityTrade) => {
          this.handleActivityTrade(activityTrade, options);
        },
        onError: (error) => {
          console.error('[SmartMoneyService] Subscription error:', error);
        },
      });
    }

    return {
      id: `smart_money_${Date.now()}`,
      unsubscribe: () => {
        this.tradeHandlers.delete(onTrade);
        if (this.tradeHandlers.size === 0 && this.activeSubscription) {
          this.activeSubscription.unsubscribe();
          this.activeSubscription = null;
        }
      },
    };
  }

  private async handleActivityTrade(
    trade: ActivityTrade,
    options: { filterAddresses?: string[]; minSize?: number; smartMoneyOnly?: boolean }
  ): Promise<void> {
    const rawAddress = trade.trader?.address;
    if (!rawAddress) return;

    const traderAddress = rawAddress.toLowerCase();

    // Address filter
    if (options.filterAddresses && options.filterAddresses.length > 0) {
      const normalized = options.filterAddresses.map(a => a.toLowerCase());
      if (!normalized.includes(traderAddress)) return;
    }

    // Size filter
    if (options.minSize && trade.size < options.minSize) return;

    // Smart Money filter
    const isSmartMoney = this.smartMoneySet.has(traderAddress);
    if (options.smartMoneyOnly && !isSmartMoney) return;

    const smartMoneyTrade: SmartMoneyTrade = {
      traderAddress,
      traderName: trade.trader?.name,
      conditionId: trade.conditionId,
      marketSlug: trade.marketSlug,
      side: trade.side,
      size: trade.size,
      price: trade.price,
      tokenId: trade.asset,
      outcome: trade.outcome,
      txHash: trade.transactionHash,
      timestamp: trade.timestamp,
      isSmartMoney,
      smartMoneyInfo: this.smartMoneyCache.get(traderAddress),
    };

    for (const handler of this.tradeHandlers) {
      try {
        handler(smartMoneyTrade);
      } catch (error) {
        console.error('[SmartMoneyService] Handler error:', error);
      }
    }
  }

  // ============================================================================
  // Auto Copy Trading - 自动跟单
  // ============================================================================

  /**
   * Start auto copy trading - 自动跟单
   *
   * @example
   * ```typescript
   * const sub = await smartMoneyService.startAutoCopyTrading({
   *   targetAddresses: ['0x1234...'],
   *   // 或者跟踪排行榜前N名
   *   topN: 10,
   *
   *   sizeScale: 0.1,        // 10%
   *   maxSizePerTrade: 50,   // $50
   *   maxSlippage: 0.03,     // 3%
   *   orderType: 'FOK',
   *
   *   dryRun: true,          // 测试模式
   *
   *   onTrade: (trade, result) => console.log(result),
   * });
   *
   * // 停止
   * sub.stop();
   * ```
   */
  async startAutoCopyTrading(options: AutoCopyTradingOptions): Promise<AutoCopyTradingSubscription> {
    const startTime = Date.now();

    // Build target list
    let targetAddresses: string[] = [];

    if (options.targetAddresses?.length) {
      targetAddresses = options.targetAddresses.map(a => a.toLowerCase());
    }

    if (options.topN && options.topN > 0) {
      const smartMoneyList = await this.getSmartMoneyList(options.topN);
      const topAddresses = smartMoneyList.map(w => w.address);
      targetAddresses = [...new Set([...targetAddresses, ...topAddresses])];
    }

    if (targetAddresses.length === 0) {
      throw new Error('No target addresses. Use targetAddresses or topN.');
    }

    // Stats
    const stats: AutoCopyTradingStats = {
      startTime,
      tradesDetected: 0,
      tradesExecuted: 0,
      tradesSkipped: 0,
      tradesFailed: 0,
      totalUsdcSpent: 0,
    };

    // Config
    const sizeScale = options.sizeScale ?? 0.1;
    const maxSizePerTrade = options.maxSizePerTrade ?? 50;
    const maxSlippage = options.maxSlippage ?? 0.03;
    const orderType = options.orderType ?? 'FOK';
    const minTradeSize = options.minTradeSize ?? 10;
    const sideFilter = options.sideFilter;
    const delay = options.delay ?? 0;
    const dryRun = options.dryRun ?? false;

    // Subscribe
    const subscription = this.subscribeSmartMoneyTrades(
      async (trade: SmartMoneyTrade) => {
        stats.tradesDetected++;

        try {
          // Check target
          if (!targetAddresses.includes(trade.traderAddress.toLowerCase())) {
            return;
          }

          // Filters
          const tradeValue = trade.size * trade.price;
          if (tradeValue < minTradeSize) {
            stats.tradesSkipped++;
            return;
          }

          if (sideFilter && trade.side !== sideFilter) {
            stats.tradesSkipped++;
            return;
          }

          // Calculate size
          let copySize = trade.size * sizeScale;
          let copyValue = copySize * trade.price;

          // Enforce max size
          if (copyValue > maxSizePerTrade) {
            copySize = maxSizePerTrade / trade.price;
            copyValue = maxSizePerTrade;
          }

          // Polymarket minimum order is $1
          const MIN_ORDER_SIZE = 1;
          if (copyValue < MIN_ORDER_SIZE) {
            stats.tradesSkipped++;
            return;
          }

          // Delay
          if (delay > 0) {
            await new Promise(resolve => setTimeout(resolve, delay));
          }

          // Token
          const tokenId = trade.tokenId;
          if (!tokenId) {
            stats.tradesSkipped++;
            return;
          }

          // Price with slippage
          const slippagePrice = trade.side === 'BUY'
            ? trade.price * (1 + maxSlippage)
            : trade.price * (1 - maxSlippage);

          const usdcAmount = copyValue; // Already calculated above

          // Execute
          let result: OrderResult;

          if (dryRun) {
            result = { success: true, orderId: `dry_run_${Date.now()}` };
            console.log('[DRY RUN]', {
              trader: trade.traderAddress.slice(0, 10),
              side: trade.side,
              market: trade.marketSlug,
              copy: { size: copySize.toFixed(2), usdc: usdcAmount.toFixed(2) },
            });
          } else {
            result = await this.tradingService.createMarketOrder({
              tokenId,
              side: trade.side,
              amount: usdcAmount,
              price: slippagePrice,
              orderType,
            });
          }

          if (result.success) {
            stats.tradesExecuted++;
            stats.totalUsdcSpent += usdcAmount;
          } else {
            stats.tradesFailed++;
          }

          options.onTrade?.(trade, result);
        } catch (error) {
          stats.tradesFailed++;
          options.onError?.(error instanceof Error ? error : new Error(String(error)));
        }
      },
      { filterAddresses: targetAddresses, minSize: minTradeSize }
    );

    return {
      id: subscription.id,
      targetAddresses,
      startTime,
      isActive: true,
      stats,
      stop: () => subscription.unsubscribe(),
      getStats: () => ({ ...stats }),
    };
  }

  // ============================================================================
  // Leaderboard - 排行榜
  // ============================================================================

  /**
   * Get leaderboard by time period
   *
   * @example
   * ```typescript
   * // Get weekly top 100 by PnL
   * const leaderboard = await sdk.smartMoney.getLeaderboard({
   *   period: 'week',
   *   limit: 100,
   *   sortBy: 'pnl'
   * });
   * ```
   */
  async getLeaderboard(options: LeaderboardOptions = {}): Promise<SmartMoneyLeaderboardEntry[]> {
    const period = options.period ?? 'week';
    const limit = Math.min(options.limit ?? 50, 500);
    const sortBy = options.sortBy ?? 'pnl';
    const offset = Math.min(options.offset ?? 0, 10000);

    const entries = await this.walletService.getLeaderboardByPeriod(period, limit, sortBy, 'OVERALL', offset);

    return entries.map(e => ({
      address: e.address,
      rank: e.rank,
      pnl: e.pnl,
      volume: e.volume,
      tradeCount: e.tradeCount,
      userName: e.userName,
      profileImage: e.profileImage,
    }));
  }

  // ============================================================================
  // Wallet Report - 钱包报告
  // ============================================================================

  /**
   * Generate comprehensive wallet report
   *
   * @example
   * ```typescript
   * const report = await sdk.smartMoney.getWalletReport('0x...');
   * console.log(report.overview.totalPnL);
   * console.log(report.rankings.weekly?.rank);
   * ```
   */
  async getWalletReport(address: string): Promise<WalletReport> {
    // Fetch all data in parallel
    const [
      profile,
      positions,
      activitySummary,
      dailyPnl,
      weeklyPnl,
      monthlyPnl,
      allTimePnl,
    ] = await Promise.all([
      this.walletService.getWalletProfile(address),
      this.walletService.getWalletPositions(address),
      this.walletService.getWalletActivity(address, 100),
      this.walletService.getUserPeriodPnl(address, 'day').catch(() => null),
      this.walletService.getUserPeriodPnl(address, 'week').catch(() => null),
      this.walletService.getUserPeriodPnl(address, 'month').catch(() => null),
      this.walletService.getUserPeriodPnl(address, 'all').catch(() => null),
    ]);

    // Calculate performance metrics
    const winningPositions = positions.filter(p => (p.cashPnl ?? 0) > 0);
    const losingPositions = positions.filter(p => (p.cashPnl ?? 0) < 0);

    // Use initialValue (cost basis) instead of currentValue (which is 0 for settled markets)
    const avgPositionSize = positions.length > 0
      ? positions.reduce((sum, p) => sum + (p.initialValue ?? (p.size * p.avgPrice)), 0) / positions.length
      : 0;

    const avgWinAmount = winningPositions.length > 0
      ? winningPositions.reduce((sum, p) => sum + (p.cashPnl ?? 0), 0) / winningPositions.length
      : 0;

    const avgLossAmount = losingPositions.length > 0
      ? Math.abs(losingPositions.reduce((sum, p) => sum + (p.cashPnl ?? 0), 0) / losingPositions.length)
      : 0;

    const uniqueMarkets = new Set(positions.map(p => p.conditionId)).size;

    // Category analysis
    const categoryStats = this.analyzeCategories(positions);

    // Recent trades
    const trades = activitySummary.activities.filter(a => a.type === 'TRADE');
    const recentTrades = trades.slice(0, 10);

    // Build rankings
    const toRanking = (entry: PeriodLeaderboardEntry | null): PeriodRanking | null => {
      if (!entry) return null;
      return { rank: entry.rank, pnl: entry.pnl, volume: entry.volume };
    };

    return {
      address,
      generatedAt: new Date(),

      overview: {
        totalPnL: profile.totalPnL,
        realizedPnL: profile.realizedPnL,
        unrealizedPnL: profile.unrealizedPnL,
        positionCount: positions.length,
        tradeCount: profile.tradeCount,
        smartScore: profile.smartScore,
        lastActiveAt: profile.lastActiveAt,
      },

      rankings: {
        daily: toRanking(dailyPnl),
        weekly: toRanking(weeklyPnl),
        monthly: toRanking(monthlyPnl),
        allTime: toRanking(allTimePnl),
      },

      performance: {
        winRate: positions.length > 0 ? (winningPositions.length / positions.length) * 100 : 0,
        winCount: winningPositions.length,
        lossCount: losingPositions.length,
        avgPositionSize,
        avgWinAmount,
        avgLossAmount,
        uniqueMarkets,
      },

      categoryBreakdown: categoryStats,

      topPositions: positions
        .sort((a, b) => Math.abs(b.cashPnl ?? 0) - Math.abs(a.cashPnl ?? 0))
        .slice(0, 10)
        .map(p => ({
          market: p.title,
          slug: p.slug,
          outcome: p.outcome,
          size: p.size,
          avgPrice: p.avgPrice,
          currentPrice: p.curPrice,
          pnl: p.cashPnl ?? 0,
          percentPnl: p.percentPnl,
        })),

      recentTrades: recentTrades.map(t => ({
        timestamp: t.timestamp,
        side: t.side,
        size: t.size,
        price: t.price,
        usdcSize: t.usdcSize,
        // Include market info for display
        title: t.title,
        slug: t.slug,
        outcome: t.outcome,
        conditionId: t.conditionId,
      })),

      activitySummary: {
        totalBuys: activitySummary.summary.totalBuys,
        totalSells: activitySummary.summary.totalSells,
        buyVolume: activitySummary.summary.buyVolume,
        sellVolume: activitySummary.summary.sellVolume,
        activeMarketsCount: activitySummary.summary.activeMarkets.length,
      },
    };
  }

  /**
   * Analyze position categories based on title keywords
   */
  private analyzeCategories(positions: Position[]): Array<{ category: string; positionCount: number; totalPnl: number }> {
    const categoryStats: Record<string, { count: number; totalPnl: number }> = {};

    for (const pos of positions) {
      const title = (pos.title || '').toLowerCase();
      let category = 'other';

      if (title.includes('trump') || title.includes('biden') || title.includes('election') || title.includes('president') || title.includes('congress')) {
        category = 'politics';
      } else if (title.includes('bitcoin') || title.includes('btc') || title.includes('eth') || title.includes('crypto') || title.includes('solana')) {
        category = 'crypto';
      } else if (title.includes('nba') || title.includes('nfl') || title.includes('soccer') || title.includes('football') || title.includes('ufc') || title.includes('mlb')) {
        category = 'sports';
      } else if (title.includes('fed') || title.includes('inflation') || title.includes('gdp') || title.includes('interest rate') || title.includes('unemployment')) {
        category = 'economy';
      } else if (title.includes('ai') || title.includes('openai') || title.includes('google') || title.includes('apple') || title.includes('tesla')) {
        category = 'tech';
      }

      if (!categoryStats[category]) {
        categoryStats[category] = { count: 0, totalPnl: 0 };
      }
      categoryStats[category].count++;
      categoryStats[category].totalPnl += (pos.cashPnl ?? 0);
    }

    return Object.entries(categoryStats)
      .map(([category, stats]) => ({
        category,
        positionCount: stats.count,
        totalPnl: stats.totalPnl,
      }))
      .sort((a, b) => b.positionCount - a.positionCount);
  }

  // ============================================================================
  // Wallet Comparison - 钱包对比
  // ============================================================================

  /**
   * Compare multiple wallets
   *
   * @example
   * ```typescript
   * const comparison = await sdk.smartMoney.compareWallets(
   *   ['0x111...', '0x222...', '0x333...'],
   *   { period: 'week' }
   * );
   * ```
   */
  async compareWallets(
    addresses: string[],
    options: { period?: TimePeriod } = {}
  ): Promise<WalletComparison> {
    const period = options.period ?? 'week';

    // Fetch data for all wallets in parallel
    const results = await Promise.all(
      addresses.map(async (address) => {
        const [periodPnl, positions] = await Promise.all([
          this.walletService.getUserPeriodPnl(address, period).catch(() => null),
          this.walletService.getWalletPositions(address).catch(() => []),
        ]);

        const winningPositions = positions.filter(p => (p.cashPnl ?? 0) > 0);
        const winRate = positions.length > 0
          ? (winningPositions.length / positions.length) * 100
          : 0;

        return {
          address,
          userName: periodPnl?.userName,
          rank: periodPnl?.rank ?? null,
          pnl: periodPnl?.pnl ?? 0,
          volume: periodPnl?.volume ?? 0,
          positionCount: positions.length,
          winRate,
        };
      })
    );

    // Sort by PnL descending
    results.sort((a, b) => b.pnl - a.pnl);

    return {
      period,
      generatedAt: new Date(),
      wallets: results,
    };
  }

  // ============================================================================
  // Utilities
  // ============================================================================

  private isCacheValid(): boolean {
    return Date.now() - this.cacheTimestamp < this.config.cacheTtl && this.smartMoneyCache.size > 0;
  }

  disconnect(): void {
    if (this.activeSubscription) {
      this.activeSubscription.unsubscribe();
      this.activeSubscription = null;
    }
    this.tradeHandlers.clear();
    this.smartMoneyCache.clear();
    this.smartMoneySet.clear();
  }
}
