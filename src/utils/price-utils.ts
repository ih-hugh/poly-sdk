/**
 * Price Utilities for Polymarket Trading
 *
 * Provides helpers for:
 * - Price validation and rounding to tick size
 * - Size validation
 * - Order amount calculation
 * - Fee-adjusted profit calculations
 */

// ============= Polymarket Fee Constants =============

/**
 * Polymarket taker fee for crypto short-term markets (BTC/ETH/SOL/XRP UP/DOWN)
 * As of 2025, these markets have a 3% taker fee
 */
export const POLY_CRYPTO_TAKER_FEE = 0.03;

/**
 * Polymarket taker fee for standard markets (politics, sports, etc.)
 * Most standard markets have 0% taker fee
 */
export const POLY_STANDARD_TAKER_FEE = 0;

/**
 * Market type for fee calculation
 * - 'crypto': BTC/ETH/SOL/XRP UP/DOWN short-term markets (3% taker fee)
 * - 'standard': General markets (0% taker fee)
 * - 'sports': Sports betting markets (0% taker fee, same as standard)
 */
export type MarketFeeType = 'crypto' | 'standard' | 'sports';

/**
 * Get the taker fee rate for a market type
 * @param marketType - 'crypto' for short-term crypto markets, 'standard'/'sports' for others
 * @returns Fee rate (0.03 = 3%)
 */
export function getTakerFeeRate(marketType: MarketFeeType): number {
  return marketType === 'crypto' ? POLY_CRYPTO_TAKER_FEE : POLY_STANDARD_TAKER_FEE;
}

/**
 * Calculate fee-adjusted profit for arbitrage
 * 
 * For long arb (buy YES + NO, merge for $1):
 * - Total cost = cost + (cost * feeRate * 2) for fees on both legs
 * - Profit = 1 - effectiveCost
 * 
 * @param totalCost - Raw cost of buying YES + NO
 * @param feeRate - Taker fee rate (default: POLY_CRYPTO_TAKER_FEE)
 * @returns Fee-adjusted profit (can be negative if fees exceed profit)
 * 
 * @example
 * // If YES = 0.48, NO = 0.49, totalCost = 0.97
 * // With 3% fees: effectiveCost = 0.97 * 1.06 = 1.0282
 * // Fee-adjusted profit = 1 - 1.0282 = -0.0282 (LOSS)
 * calculateFeeAdjustedProfit(0.97, 0.03) // Returns -0.0282
 */
export function calculateFeeAdjustedProfit(
  totalCost: number,
  feeRate: number = POLY_CRYPTO_TAKER_FEE
): number {
  if (totalCost <= 0 || totalCost >= 1) return 0;
  
  // Fees apply to both legs (buy YES and buy NO)
  const effectiveCost = totalCost * (1 + feeRate * 2);
  return 1 - effectiveCost;
}

/**
 * Calculate fee-adjusted profit rate (as percentage of cost)
 * 
 * @param totalCost - Raw cost of buying YES + NO
 * @param feeRate - Taker fee rate (default: POLY_CRYPTO_TAKER_FEE)
 * @returns Fee-adjusted profit rate (e.g., 0.05 = 5% profit)
 * 
 * @example
 * // If cost = 0.90 (10% gross profit), with 3% fees on both legs:
 * // effectiveCost = 0.90 * 1.06 = 0.954
 * // profit = 1 - 0.954 = 0.046 (4.6%)
 * // profitRate = 0.046 / 0.954 = 4.8%
 */
export function calculateFeeAdjustedProfitRate(
  totalCost: number,
  feeRate: number = POLY_CRYPTO_TAKER_FEE
): number {
  if (totalCost <= 0 || totalCost >= 1) return 0;
  
  const effectiveCost = totalCost * (1 + feeRate * 2);
  if (effectiveCost >= 1) return -(effectiveCost - 1) / effectiveCost; // Return negative rate for losses
  
  return (1 - effectiveCost) / effectiveCost;
}

/**
 * Calculate minimum required gross profit to break even after fees
 * 
 * @param feeRate - Taker fee rate (default: POLY_CRYPTO_TAKER_FEE)
 * @returns Minimum gross profit rate needed (e.g., 0.06 = 6%)
 * 
 * @example
 * // With 3% fees on both legs, you need at least 6% gross profit to break even
 * getMinGrossProfitForBreakeven(0.03) // Returns ~0.0638
 */
export function getMinGrossProfitForBreakeven(
  feeRate: number = POLY_CRYPTO_TAKER_FEE
): number {
  // For break-even: totalCost * (1 + feeRate * 2) = 1
  // totalCost = 1 / (1 + feeRate * 2)
  // grossProfit = 1 - totalCost
  const breakEvenCost = 1 / (1 + feeRate * 2);
  return 1 - breakEvenCost;
}

// Tick size types (as defined by Polymarket)
export type TickSize = '0.1' | '0.01' | '0.001' | '0.0001';

// Rounding configuration for each tick size
export const ROUNDING_CONFIG: Record<
  TickSize,
  { price: number; size: number; amount: number }
> = {
  '0.1': { price: 1, size: 2, amount: 2 },
  '0.01': { price: 2, size: 2, amount: 4 },
  '0.001': { price: 3, size: 2, amount: 5 },
  '0.0001': { price: 4, size: 2, amount: 6 },
};

/**
 * Round a price to the appropriate tick size
 *
 * @param price - The price to round (0 to 1)
 * @param tickSize - The tick size for the market
 * @param direction - 'floor' for sells, 'ceil' for buys, 'round' for midpoint
 * @returns Rounded price
 *
 * @example
 * roundPrice(0.523, '0.01', 'floor') // 0.52
 * roundPrice(0.523, '0.01', 'ceil') // 0.53
 */
export function roundPrice(
  price: number,
  tickSize: TickSize,
  direction: 'floor' | 'ceil' | 'round' = 'round'
): number {
  const decimals = ROUNDING_CONFIG[tickSize].price;
  const multiplier = Math.pow(10, decimals);

  let rounded: number;
  switch (direction) {
    case 'floor':
      rounded = Math.floor(price * multiplier) / multiplier;
      break;
    case 'ceil':
      rounded = Math.ceil(price * multiplier) / multiplier;
      break;
    default:
      rounded = Math.round(price * multiplier) / multiplier;
  }

  // Clamp to valid price range
  return Math.max(0.001, Math.min(0.999, rounded));
}

/**
 * Round a size to valid decimals (always 2 decimal places)
 */
export function roundSize(size: number): number {
  return Math.round(size * 100) / 100;
}

/**
 * Validate a price is within valid range and tick size
 *
 * @param price - The price to validate
 * @param tickSize - The tick size for the market
 * @returns Validation result with error message if invalid
 */
export function validatePrice(
  price: number,
  tickSize: TickSize
): { valid: boolean; error?: string } {
  // Check range
  if (price < 0.001 || price > 0.999) {
    return { valid: false, error: 'Price must be between 0.001 and 0.999' };
  }

  // Check tick size alignment
  const decimals = ROUNDING_CONFIG[tickSize].price;
  const multiplier = Math.pow(10, decimals);
  const rounded = Math.round(price * multiplier) / multiplier;

  if (Math.abs(price - rounded) > 1e-10) {
    return {
      valid: false,
      error: `Price ${price} does not align with tick size ${tickSize}. Use ${rounded} instead.`,
    };
  }

  return { valid: true };
}

/**
 * Validate minimum size requirements
 *
 * @param size - The size to validate
 * @param minOrderSize - Minimum order size from market config (usually 0.1)
 */
export function validateSize(
  size: number,
  minOrderSize = 0.1
): { valid: boolean; error?: string } {
  if (size < minOrderSize) {
    return {
      valid: false,
      error: `Size ${size} is below minimum order size ${minOrderSize}`,
    };
  }

  return { valid: true };
}

/**
 * Calculate the amount needed for a buy order (in USDC)
 *
 * @param price - Price per share
 * @param size - Number of shares to buy
 * @returns Amount in USDC
 */
export function calculateBuyAmount(price: number, size: number): number {
  return price * size;
}

/**
 * Calculate the payout for a sell order (in USDC)
 *
 * @param price - Price per share
 * @param size - Number of shares to sell
 * @returns Amount in USDC
 */
export function calculateSellPayout(price: number, size: number): number {
  return price * size;
}

/**
 * Calculate number of shares that can be bought with a given amount
 *
 * @param amount - USDC amount to spend
 * @param price - Price per share
 * @returns Number of shares
 */
export function calculateSharesForAmount(
  amount: number,
  price: number
): number {
  return roundSize(amount / price);
}

/**
 * Calculate the spread between bid and ask
 *
 * @param bid - Highest bid price
 * @param ask - Lowest ask price
 * @returns Spread as a decimal (0 to 1)
 */
export function calculateSpread(bid: number, ask: number): number {
  return ask - bid;
}

/**
 * Calculate the midpoint price between bid and ask
 *
 * @param bid - Highest bid price
 * @param ask - Lowest ask price
 * @returns Midpoint price
 */
export function calculateMidpoint(bid: number, ask: number): number {
  return (bid + ask) / 2;
}

/**
 * 计算有效价格（考虑 Polymarket 订单簿的镜像特性）
 *
 * Polymarket 的关键特性：买 YES @ P = 卖 NO @ (1-P)
 * 因此同一订单会在两个订单簿中出现
 *
 * 有效价格是考虑镜像后的最优价格
 *
 * @param yesAsk - YES token 的最低卖价
 * @param yesBid - YES token 的最高买价
 * @param noAsk - NO token 的最低卖价
 * @param noBid - NO token 的最高买价
 */
export function getEffectivePrices(
  yesAsk: number,
  yesBid: number,
  noAsk: number,
  noBid: number
): {
  effectiveBuyYes: number;
  effectiveBuyNo: number;
  effectiveSellYes: number;
  effectiveSellNo: number;
} {
  return {
    // 买 YES: 直接买 YES.ask 或 通过卖 NO (成本 = 1 - NO.bid)
    effectiveBuyYes: Math.min(yesAsk, 1 - noBid),

    // 买 NO: 直接买 NO.ask 或 通过卖 YES (成本 = 1 - YES.bid)
    effectiveBuyNo: Math.min(noAsk, 1 - yesBid),

    // 卖 YES: 直接卖 YES.bid 或 通过买 NO (收入 = 1 - NO.ask)
    effectiveSellYes: Math.max(yesBid, 1 - noAsk),

    // 卖 NO: 直接卖 NO.bid 或 通过买 YES (收入 = 1 - YES.ask)
    effectiveSellNo: Math.max(noBid, 1 - yesAsk),
  };
}

/**
 * Arbitrage opportunity result
 */
export interface ArbitrageOpportunityResult {
  type: 'long' | 'short';
  /** Gross profit (before fees) */
  grossProfit: number;
  /** Net profit (after fees) - can be negative */
  netProfit: number;
  /** Gross profit rate (before fees) */
  grossProfitRate: number;
  /** Net profit rate (after fees) - can be negative */
  netProfitRate: number;
  /** Fee rate applied */
  feeRate: number;
  /** Total fees paid (on both legs) */
  totalFees: number;
  /** Description */
  description: string;
  /** Whether this is profitable after fees */
  isProfitable: boolean;
}

/**
 * Check if there's an arbitrage opportunity
 *
 * 使用有效价格计算套利机会（正确考虑镜像订单）
 *
 * Long arb: Buy YES + Buy NO < 1 (使用有效买入价格)
 * Short arb: Sell YES + Sell NO > 1 (使用有效卖出价格)
 *
 * 详细文档见: docs/01-polymarket-orderbook-arbitrage.md
 *
 * @param yesAsk - Lowest ask for YES token
 * @param noAsk - Lowest ask for NO token
 * @param yesBid - Highest bid for YES token
 * @param noBid - Highest bid for NO token
 * @returns Arbitrage info or null (legacy - gross profit only)
 * @deprecated Use checkArbitrageWithFees for fee-aware calculations
 */
export function checkArbitrage(
  yesAsk: number,
  noAsk: number,
  yesBid: number,
  noBid: number
): { type: 'long' | 'short'; profit: number; description: string } | null {
  // 计算有效价格
  const effective = getEffectivePrices(yesAsk, yesBid, noAsk, noBid);

  // Long arbitrage: Buy complete set (YES + NO) cheaper than $1
  const effectiveLongCost = effective.effectiveBuyYes + effective.effectiveBuyNo;
  const longProfit = 1 - effectiveLongCost;

  if (longProfit > 0) {
    return {
      type: 'long',
      profit: longProfit,
      description: `Buy YES @ ${effective.effectiveBuyYes.toFixed(4)} + NO @ ${effective.effectiveBuyNo.toFixed(4)}, Merge for $1`,
    };
  }

  // Short arbitrage: Sell complete set (YES + NO) for more than $1
  const effectiveShortRevenue = effective.effectiveSellYes + effective.effectiveSellNo;
  const shortProfit = effectiveShortRevenue - 1;

  if (shortProfit > 0) {
    return {
      type: 'short',
      profit: shortProfit,
      description: `Split $1, Sell YES @ ${effective.effectiveSellYes.toFixed(4)} + NO @ ${effective.effectiveSellNo.toFixed(4)}`,
    };
  }

  return null;
}

/**
 * Check if there's an arbitrage opportunity with fee-aware calculations
 *
 * This is the recommended function for determining real profitability.
 * It accounts for Polymarket taker fees on both legs of the trade.
 *
 * For crypto markets (BTC/ETH/SOL/XRP UP/DOWN): 3% taker fee per leg = 6% total
 * For standard markets: 0% taker fee
 *
 * @param yesAsk - Lowest ask for YES token
 * @param noAsk - Lowest ask for NO token
 * @param yesBid - Highest bid for YES token
 * @param noBid - Highest bid for NO token
 * @param feeRate - Taker fee rate (default: POLY_CRYPTO_TAKER_FEE = 0.03)
 * @param minNetProfit - Minimum net profit required (default: 0)
 * @returns Arbitrage opportunity with fee details, or null if unprofitable
 *
 * @example
 * // Crypto market with 3% fees
 * const opp = checkArbitrageWithFees(0.48, 0.49, 0.47, 0.48, 0.03, 0.02);
 * // opp.grossProfit = 0.03 (3%)
 * // opp.netProfit = -0.0282 (LOSS after 6% fees)
 * // opp.isProfitable = false
 */
export function checkArbitrageWithFees(
  yesAsk: number,
  noAsk: number,
  yesBid: number,
  noBid: number,
  feeRate: number = POLY_CRYPTO_TAKER_FEE,
  minNetProfit: number = 0
): ArbitrageOpportunityResult | null {
  const effective = getEffectivePrices(yesAsk, yesBid, noAsk, noBid);

  // Long arbitrage: Buy complete set (YES + NO) cheaper than $1
  const effectiveLongCost = effective.effectiveBuyYes + effective.effectiveBuyNo;
  const longGrossProfit = 1 - effectiveLongCost;

  if (longGrossProfit > 0) {
    // Calculate fees on the purchase amount
    const totalFees = effectiveLongCost * feeRate * 2;
    const netProfit = longGrossProfit - totalFees;
    const grossProfitRate = longGrossProfit / effectiveLongCost;
    const netProfitRate = netProfit > 0 ? netProfit / (effectiveLongCost + totalFees) : netProfit / effectiveLongCost;
    const isProfitable = netProfit >= minNetProfit;

    if (isProfitable || netProfit > 0) {
      return {
        type: 'long',
        grossProfit: longGrossProfit,
        netProfit,
        grossProfitRate,
        netProfitRate,
        feeRate,
        totalFees,
        description: `Buy YES @ ${effective.effectiveBuyYes.toFixed(4)} + NO @ ${effective.effectiveBuyNo.toFixed(4)}, Merge for $1 (net: ${(netProfitRate * 100).toFixed(2)}%)`,
        isProfitable,
      };
    }
  }

  // Short arbitrage: Sell complete set (YES + NO) for more than $1
  const effectiveShortRevenue = effective.effectiveSellYes + effective.effectiveSellNo;
  const shortGrossProfit = effectiveShortRevenue - 1;

  if (shortGrossProfit > 0) {
    // Calculate fees on the sale amount
    const totalFees = effectiveShortRevenue * feeRate * 2;
    const netProfit = shortGrossProfit - totalFees;
    const grossProfitRate = shortGrossProfit; // Already a rate since revenue is compared to $1
    const netProfitRate = netProfit;
    const isProfitable = netProfit >= minNetProfit;

    if (isProfitable || netProfit > 0) {
      return {
        type: 'short',
        grossProfit: shortGrossProfit,
        netProfit,
        grossProfitRate,
        netProfitRate,
        feeRate,
        totalFees,
        description: `Split $1, Sell YES @ ${effective.effectiveSellYes.toFixed(4)} + NO @ ${effective.effectiveSellNo.toFixed(4)} (net: ${(netProfitRate * 100).toFixed(2)}%)`,
        isProfitable,
      };
    }
  }

  return null;
}

/**
 * Format price for display
 */
export function formatPrice(price: number, tickSize?: TickSize): string {
  const decimals = tickSize ? ROUNDING_CONFIG[tickSize].price : 4;
  return price.toFixed(decimals);
}

/**
 * Format amount in USDC
 */
export function formatUSDC(amount: number): string {
  return `$${amount.toFixed(2)}`;
}

/**
 * Calculate PnL for a position
 *
 * @param entryPrice - Average entry price
 * @param currentPrice - Current market price
 * @param size - Position size
 * @param side - 'long' for YES, 'short' for NO
 */
export function calculatePnL(
  entryPrice: number,
  currentPrice: number,
  size: number,
  side: 'long' | 'short' = 'long'
): { pnl: number; pnlPercent: number } {
  const pnl =
    side === 'long'
      ? (currentPrice - entryPrice) * size
      : (entryPrice - currentPrice) * size;

  const pnlPercent =
    side === 'long'
      ? ((currentPrice - entryPrice) / entryPrice) * 100
      : ((entryPrice - currentPrice) / entryPrice) * 100;

  return { pnl, pnlPercent };
}
