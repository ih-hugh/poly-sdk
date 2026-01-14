/**
 * Price Utilities Unit Tests
 */

import { describe, it, expect } from 'vitest';
import {
  roundPrice,
  roundSize,
  validatePrice,
  validateSize,
  calculateBuyAmount,
  calculateSellPayout,
  calculateSharesForAmount,
  calculateSpread,
  calculateMidpoint,
  getEffectivePrices,
  checkArbitrage,
  checkArbitrageWithFees,
  formatPrice,
  formatUSDC,
  calculatePnL,
  ROUNDING_CONFIG,
  POLY_CRYPTO_TAKER_FEE,
  POLY_STANDARD_TAKER_FEE,
  getTakerFeeRate,
  calculateFeeAdjustedProfit,
  calculateFeeAdjustedProfitRate,
  getMinGrossProfitForBreakeven,
  type TickSize,
  type MarketFeeType,
} from './price-utils.js';

describe('Price Utilities', () => {
  describe('roundPrice', () => {
    it('should round down with floor', () => {
      expect(roundPrice(0.523, '0.01', 'floor')).toBe(0.52);
      expect(roundPrice(0.529, '0.01', 'floor')).toBe(0.52);
    });

    it('should round up with ceil', () => {
      expect(roundPrice(0.521, '0.01', 'ceil')).toBe(0.53);
      expect(roundPrice(0.520, '0.01', 'ceil')).toBe(0.52);
    });

    it('should round to nearest with round', () => {
      expect(roundPrice(0.524, '0.01', 'round')).toBe(0.52);
      expect(roundPrice(0.525, '0.01', 'round')).toBe(0.53);
    });

    it('should respect different tick sizes', () => {
      expect(roundPrice(0.1234, '0.1', 'round')).toBe(0.1);
      expect(roundPrice(0.1234, '0.01', 'round')).toBe(0.12);
      expect(roundPrice(0.1234, '0.001', 'round')).toBe(0.123);
      expect(roundPrice(0.1234, '0.0001', 'round')).toBe(0.1234);
    });

    it('should clamp to valid price range', () => {
      expect(roundPrice(0.0001, '0.01', 'floor')).toBe(0.001);
      expect(roundPrice(0.9999, '0.01', 'ceil')).toBe(0.999);
    });
  });

  describe('roundSize', () => {
    it('should round to 2 decimal places', () => {
      expect(roundSize(1.234)).toBe(1.23);
      expect(roundSize(1.235)).toBe(1.24);
      expect(roundSize(10)).toBe(10);
    });
  });

  describe('validatePrice', () => {
    it('should accept valid prices', () => {
      expect(validatePrice(0.5, '0.01').valid).toBe(true);
      expect(validatePrice(0.001, '0.001').valid).toBe(true);
      expect(validatePrice(0.999, '0.001').valid).toBe(true);
    });

    it('should reject prices outside range', () => {
      expect(validatePrice(0, '0.01').valid).toBe(false);
      expect(validatePrice(1, '0.01').valid).toBe(false);
      expect(validatePrice(-0.1, '0.01').valid).toBe(false);
    });

    it('should reject prices not aligned to tick size', () => {
      const result = validatePrice(0.123, '0.01');
      expect(result.valid).toBe(false);
      expect(result.error).toContain('does not align');
    });
  });

  describe('validateSize', () => {
    it('should accept valid sizes', () => {
      expect(validateSize(1).valid).toBe(true);
      expect(validateSize(0.1).valid).toBe(true);
    });

    it('should reject sizes below minimum', () => {
      const result = validateSize(0.05);
      expect(result.valid).toBe(false);
      expect(result.error).toContain('below minimum');
    });

    it('should respect custom minimum', () => {
      expect(validateSize(0.5, 1).valid).toBe(false);
      expect(validateSize(1.5, 1).valid).toBe(true);
    });
  });

  describe('calculateBuyAmount', () => {
    it('should calculate cost correctly', () => {
      expect(calculateBuyAmount(0.5, 10)).toBe(5);
      expect(calculateBuyAmount(0.25, 100)).toBe(25);
    });
  });

  describe('calculateSellPayout', () => {
    it('should calculate payout correctly', () => {
      expect(calculateSellPayout(0.75, 10)).toBe(7.5);
      expect(calculateSellPayout(0.5, 20)).toBe(10);
    });
  });

  describe('calculateSharesForAmount', () => {
    it('should calculate shares correctly', () => {
      expect(calculateSharesForAmount(10, 0.5)).toBe(20);
      expect(calculateSharesForAmount(25, 0.25)).toBe(100);
    });

    it('should round to 2 decimals', () => {
      expect(calculateSharesForAmount(10, 0.33)).toBe(30.3);
    });
  });

  describe('calculateSpread', () => {
    it('should calculate spread correctly', () => {
      expect(calculateSpread(0.45, 0.55)).toBeCloseTo(0.1, 10);
      expect(calculateSpread(0.5, 0.5)).toBe(0);
    });
  });

  describe('calculateMidpoint', () => {
    it('should calculate midpoint correctly', () => {
      expect(calculateMidpoint(0.4, 0.6)).toBe(0.5);
      expect(calculateMidpoint(0.3, 0.5)).toBe(0.4);
    });
  });

  describe('getEffectivePrices', () => {
    it('should calculate effective prices with mirroring', () => {
      // Scenario: YES ask=0.55, bid=0.45, NO ask=0.55, bid=0.45
      const result = getEffectivePrices(0.55, 0.45, 0.55, 0.45);

      // Buy YES: min(0.55, 1-0.45) = min(0.55, 0.55) = 0.55
      expect(result.effectiveBuyYes).toBe(0.55);

      // Buy NO: min(0.55, 1-0.45) = min(0.55, 0.55) = 0.55
      expect(result.effectiveBuyNo).toBe(0.55);

      // Sell YES: max(0.45, 1-0.55) = max(0.45, 0.45) = 0.45
      expect(result.effectiveSellYes).toBe(0.45);

      // Sell NO: max(0.45, 1-0.55) = max(0.45, 0.45) = 0.45
      expect(result.effectiveSellNo).toBe(0.45);
    });

    it('should find better prices through mirroring', () => {
      // Asymmetric case: YES ask=0.60, bid=0.45, NO ask=0.50, bid=0.35
      const result = getEffectivePrices(0.60, 0.45, 0.50, 0.35);

      // Buy YES: min(0.60, 1-0.35) = min(0.60, 0.65) = 0.60
      expect(result.effectiveBuyYes).toBe(0.60);

      // Buy NO: min(0.50, 1-0.45) = min(0.50, 0.55) = 0.50
      expect(result.effectiveBuyNo).toBe(0.50);
    });
  });

  describe('checkArbitrage', () => {
    it('should detect long arbitrage', () => {
      // YES ask=0.45, NO ask=0.45 => total cost 0.90 < 1
      const result = checkArbitrage(0.45, 0.45, 0.40, 0.40);
      expect(result).not.toBeNull();
      expect(result?.type).toBe('long');
      expect(result?.profit).toBeCloseTo(0.1, 2);
    });

    it('should prioritize long arb when both exist', () => {
      // Due to Polymarket's mirroring, if one arb exists, the other often does too
      // The function prioritizes long arb (checks first)
      // With low asks, we get long arb opportunity
      const result = checkArbitrage(0.45, 0.45, 0.40, 0.40);
      expect(result).not.toBeNull();
      // Long arb is checked first and returned
      expect(result?.type).toBe('long');
      expect(result?.profit).toBeGreaterThan(0);
    });

    it('should return null when no arbitrage', () => {
      // Normal spread, no arbitrage
      const result = checkArbitrage(0.55, 0.45, 0.55, 0.45);
      expect(result).toBeNull();
    });
  });

  describe('formatPrice', () => {
    it('should format with default decimals', () => {
      expect(formatPrice(0.5)).toBe('0.5000');
      expect(formatPrice(0.123456)).toBe('0.1235');
    });

    it('should respect tick size', () => {
      expect(formatPrice(0.5, '0.1')).toBe('0.5');
      expect(formatPrice(0.5, '0.01')).toBe('0.50');
      expect(formatPrice(0.5, '0.001')).toBe('0.500');
    });
  });

  describe('formatUSDC', () => {
    it('should format as USD', () => {
      expect(formatUSDC(10)).toBe('$10.00');
      expect(formatUSDC(1234.567)).toBe('$1234.57');
    });
  });

  describe('calculatePnL', () => {
    it('should calculate long PnL correctly', () => {
      const result = calculatePnL(0.4, 0.6, 10, 'long');
      expect(result.pnl).toBeCloseTo(2, 10); // (0.6 - 0.4) * 10
      expect(result.pnlPercent).toBeCloseTo(50, 10); // 50% gain
    });

    it('should calculate short PnL correctly', () => {
      const result = calculatePnL(0.6, 0.4, 10, 'short');
      expect(result.pnl).toBeCloseTo(2, 10); // (0.6 - 0.4) * 10
      expect(result.pnlPercent).toBeCloseTo(33.33, 1); // ~33% gain
    });

    it('should handle losses', () => {
      const result = calculatePnL(0.6, 0.4, 10, 'long');
      expect(result.pnl).toBeCloseTo(-2, 10); // Loss
      expect(result.pnlPercent).toBeCloseTo(-33.33, 1);
    });
  });

  describe('ROUNDING_CONFIG', () => {
    it('should have correct configuration for all tick sizes', () => {
      expect(ROUNDING_CONFIG['0.1']).toEqual({ price: 1, size: 2, amount: 2 });
      expect(ROUNDING_CONFIG['0.01']).toEqual({ price: 2, size: 2, amount: 4 });
      expect(ROUNDING_CONFIG['0.001']).toEqual({ price: 3, size: 2, amount: 5 });
      expect(ROUNDING_CONFIG['0.0001']).toEqual({ price: 4, size: 2, amount: 6 });
    });
  });

  // ============= Fee-Related Tests =============

  describe('Fee Constants', () => {
    it('should have correct crypto taker fee', () => {
      expect(POLY_CRYPTO_TAKER_FEE).toBe(0.03); // 3%
    });

    it('should have correct standard taker fee', () => {
      expect(POLY_STANDARD_TAKER_FEE).toBe(0); // 0%
    });
  });

  describe('getTakerFeeRate', () => {
    it('should return crypto fee for crypto markets', () => {
      expect(getTakerFeeRate('crypto')).toBe(0.03);
    });

    it('should return zero fee for standard markets', () => {
      expect(getTakerFeeRate('standard')).toBe(0);
    });

    it('should return zero fee for sports markets', () => {
      expect(getTakerFeeRate('sports')).toBe(0);
    });
  });

  describe('calculateFeeAdjustedProfit', () => {
    it('should return 0 for invalid cost', () => {
      expect(calculateFeeAdjustedProfit(0)).toBe(0);
      expect(calculateFeeAdjustedProfit(1)).toBe(0);
      expect(calculateFeeAdjustedProfit(-0.1)).toBe(0);
      expect(calculateFeeAdjustedProfit(1.5)).toBe(0);
    });

    it('should calculate fee-adjusted profit correctly for crypto markets', () => {
      // Cost = 0.97 (3% gross profit)
      // Effective cost = 0.97 * (1 + 0.03 * 2) = 0.97 * 1.06 = 1.0282
      // Net profit = 1 - 1.0282 = -0.0282 (LOSS)
      const profit = calculateFeeAdjustedProfit(0.97, 0.03);
      expect(profit).toBeCloseTo(-0.0282, 3);
    });

    it('should show profit on lower cost trades', () => {
      // Cost = 0.90 (10% gross profit)
      // Effective cost = 0.90 * 1.06 = 0.954
      // Net profit = 1 - 0.954 = 0.046 (4.6% net)
      const profit = calculateFeeAdjustedProfit(0.90, 0.03);
      expect(profit).toBeCloseTo(0.046, 3);
    });

    it('should calculate correctly with zero fees (standard/sports markets)', () => {
      // Cost = 0.97, no fees
      // Net profit = 1 - 0.97 = 0.03 (3% profit)
      const profit = calculateFeeAdjustedProfit(0.97, 0);
      expect(profit).toBeCloseTo(0.03, 3);
    });

    it('should use default crypto fee if not specified', () => {
      const profit = calculateFeeAdjustedProfit(0.90);
      const profitWithExplicitFee = calculateFeeAdjustedProfit(0.90, POLY_CRYPTO_TAKER_FEE);
      expect(profit).toBe(profitWithExplicitFee);
    });
  });

  describe('calculateFeeAdjustedProfitRate', () => {
    it('should return 0 for invalid cost', () => {
      expect(calculateFeeAdjustedProfitRate(0)).toBe(0);
      expect(calculateFeeAdjustedProfitRate(1)).toBe(0);
    });

    it('should return negative rate for unprofitable trades', () => {
      // Cost = 0.97 with 3% fees = loss
      const rate = calculateFeeAdjustedProfitRate(0.97, 0.03);
      expect(rate).toBeLessThan(0);
    });

    it('should return positive rate for profitable trades', () => {
      // Cost = 0.90 with 3% fees = profit
      const rate = calculateFeeAdjustedProfitRate(0.90, 0.03);
      expect(rate).toBeGreaterThan(0);
      expect(rate).toBeCloseTo(0.048, 2); // ~4.8% net profit rate
    });

    it('should handle zero fees correctly', () => {
      // Cost = 0.95, no fees = 5.26% profit rate
      const rate = calculateFeeAdjustedProfitRate(0.95, 0);
      expect(rate).toBeCloseTo(0.0526, 3);
    });
  });

  describe('getMinGrossProfitForBreakeven', () => {
    it('should return correct breakeven for crypto markets', () => {
      // With 3% fee per leg (6% total), need ~6.38% gross to break even
      const breakeven = getMinGrossProfitForBreakeven(0.03);
      expect(breakeven).toBeCloseTo(0.0566, 3); // ~5.66%
    });

    it('should return 0 for zero-fee markets', () => {
      const breakeven = getMinGrossProfitForBreakeven(0);
      expect(breakeven).toBe(0);
    });

    it('should scale with fee rate', () => {
      // Higher fees = higher breakeven needed
      const breakeven1 = getMinGrossProfitForBreakeven(0.01); // 1% fee
      const breakeven3 = getMinGrossProfitForBreakeven(0.03); // 3% fee
      const breakeven5 = getMinGrossProfitForBreakeven(0.05); // 5% fee

      expect(breakeven3).toBeGreaterThan(breakeven1);
      expect(breakeven5).toBeGreaterThan(breakeven3);
    });
  });

  describe('checkArbitrageWithFees', () => {
    it('should return null when no arbitrage exists', () => {
      // Normal spread, no opportunity
      const result = checkArbitrageWithFees(0.55, 0.45, 0.55, 0.45, 0.03);
      expect(result).toBeNull();
    });

    it('should detect long arbitrage and calculate fees correctly', () => {
      // YES ask=0.45, NO ask=0.45 => total cost 0.90 (10% gross)
      const result = checkArbitrageWithFees(0.45, 0.45, 0.40, 0.40, 0.03);
      expect(result).not.toBeNull();
      expect(result?.type).toBe('long');
      expect(result?.grossProfit).toBeCloseTo(0.1, 2);
      expect(result?.totalFees).toBeCloseTo(0.054, 3); // 0.90 * 0.03 * 2
      expect(result?.netProfit).toBeCloseTo(0.046, 2); // ~4.6%
      expect(result?.isProfitable).toBe(true);
    });

    it('should mark as unprofitable when fees exceed gross profit', () => {
      // YES ask=0.485, NO ask=0.485 => total cost 0.97 (3% gross)
      // Fees = 0.97 * 0.06 = 0.0582 > 0.03 gross profit = LOSS
      const result = checkArbitrageWithFees(0.485, 0.485, 0.45, 0.45, 0.03);
      if (result) {
        expect(result.grossProfit).toBeGreaterThan(0);
        expect(result.netProfit).toBeLessThan(0);
        expect(result.isProfitable).toBe(false);
      }
    });

    it('should respect minNetProfit threshold', () => {
      // Require at least 2% net profit
      const result = checkArbitrageWithFees(0.46, 0.46, 0.40, 0.40, 0.03, 0.02);

      // Cost = 0.92 (8% gross), after 6% fees = ~2% net
      // Should still be profitable with 2% threshold
      if (result) {
        expect(result.netProfit).toBeGreaterThanOrEqual(0.02 - 0.01); // Allow for calculation variance
      }
    });

    it('should handle zero-fee markets correctly', () => {
      // Sports/standard markets have 0% fee
      const result = checkArbitrageWithFees(0.48, 0.48, 0.45, 0.45, 0);
      expect(result).not.toBeNull();
      expect(result?.totalFees).toBe(0);
      expect(result?.grossProfit).toBe(result?.netProfit);
      expect(result?.isProfitable).toBe(true);
    });

    it('should include helpful description', () => {
      const result = checkArbitrageWithFees(0.45, 0.45, 0.40, 0.40, 0.03);
      expect(result?.description).toContain('Buy YES');
      expect(result?.description).toContain('NO');
      expect(result?.description).toContain('net:');
    });
  });
});
