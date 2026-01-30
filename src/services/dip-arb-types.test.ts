/**
 * DipArb Types Unit Tests
 *
 * Tests for the dip arbitrage type utilities and calculation functions.
 */

import { describe, it, expect } from 'vitest';
import {
  calculateDipArbProfitRate,
  calculateDipArbNetProfitRate,
  getMaxSumTargetForNetProfit,
  estimateUpWinRate,
  createDipArbRoundState,
  getMarketContext,
  calculateDipArbLeg2NetProfit,
  calculateDipArbSettlementWinProfit,
  calculateDipArbExitValue,
  DIP_ARB_CRYPTO_TAKER_FEE,
  DEFAULT_DIP_ARB_CONFIG,
  type DipArbMarketConfig,
  type DipArbParallelOptions,
  type DipArbMonitorStatus,
} from './dip-arb-types.js';

describe('DipArb Types', () => {
  describe('DIP_ARB_CRYPTO_TAKER_FEE', () => {
    it('should be 3%', () => {
      expect(DIP_ARB_CRYPTO_TAKER_FEE).toBe(0.03);
    });
  });

  describe('DEFAULT_DIP_ARB_CONFIG', () => {
    it('should have sensible defaults', () => {
      // Key safety parameters (current optimized values)
      expect(DEFAULT_DIP_ARB_CONFIG.minDipSidePrice).toBe(0.03);
      expect(DEFAULT_DIP_ARB_CONFIG.maxLeg1Price).toBe(0.95);
      expect(DEFAULT_DIP_ARB_CONFIG.maxOpenPositions).toBe(25);

      // Profitability parameters (Settlement Hunter config)
      // P0.2: sumTarget tightened from 0.88 to 0.86 for better net profit (~7.5% instead of ~5.5%)
      expect(DEFAULT_DIP_ARB_CONFIG.sumTarget).toBe(0.86);
      expect(DEFAULT_DIP_ARB_CONFIG.dipThreshold).toBe(0.025);

      // Paper mode should be default (safe)
      expect(DEFAULT_DIP_ARB_CONFIG.paperMode).toBe(true);
    });

    it('should have settlement awareness config enabled by default', () => {
      // Settlement awareness should be enabled by default
      expect(DEFAULT_DIP_ARB_CONFIG.favorSettlement).toBe(true);

      // Conservative threshold (50%)
      expect(DEFAULT_DIP_ARB_CONFIG.settlementHoldThreshold).toBe(0.50);

      // Always hold if <3 min to market end
      expect(DEFAULT_DIP_ARB_CONFIG.minTimeToEndForHold).toBe(3);

      // 5 min buffer after market end
      expect(DEFAULT_DIP_ARB_CONFIG.settlementWaitBuffer).toBe(300);

      // Momentum validation disabled by default
      expect(DEFAULT_DIP_ARB_CONFIG.enableSettlementMomentum).toBe(false);
      expect(DEFAULT_DIP_ARB_CONFIG.settlementMomentumThreshold).toBe(0.3);
    });
  });

  describe('calculateDipArbProfitRate', () => {
    it('should return 0 for invalid costs', () => {
      expect(calculateDipArbProfitRate(0)).toBe(0);
      expect(calculateDipArbProfitRate(1)).toBe(0);
      expect(calculateDipArbProfitRate(-0.1)).toBe(0);
      expect(calculateDipArbProfitRate(1.5)).toBe(0);
    });

    it('should calculate gross profit rate correctly', () => {
      // Cost = 0.90 (10% gross profit)
      // Profit rate = 0.10 / 0.90 = 11.11%
      expect(calculateDipArbProfitRate(0.90)).toBeCloseTo(0.1111, 3);

      // Cost = 0.95 (5% gross profit)
      // Profit rate = 0.05 / 0.95 = 5.26%
      expect(calculateDipArbProfitRate(0.95)).toBeCloseTo(0.0526, 3);
    });
  });

  describe('calculateDipArbNetProfitRate', () => {
    it('should return 0 for invalid costs', () => {
      expect(calculateDipArbNetProfitRate(0)).toBe(0);
      expect(calculateDipArbNetProfitRate(1)).toBe(0);
    });

    it('should calculate net profit after fees', () => {
      // Cost = 0.92 (8% gross)
      // Fees = 0.92 * 0.03 * 2 = 0.0552 (5.52%)
      // Net profit = 0.08 - 0.0552 = 0.0248 (2.48%)
      const netRate = calculateDipArbNetProfitRate(0.92, 0.03);
      expect(netRate).toBeGreaterThan(0);
      expect(netRate).toBeLessThan(0.08); // Less than gross due to fees
    });

    it('should return negative for unprofitable trades', () => {
      // Cost = 0.97 (3% gross) with 6% fee overhead = loss
      const netRate = calculateDipArbNetProfitRate(0.97, 0.03);
      expect(netRate).toBeLessThan(0);
    });

    it('should use default crypto fee if not specified', () => {
      const withDefault = calculateDipArbNetProfitRate(0.90);
      const withExplicit = calculateDipArbNetProfitRate(0.90, DIP_ARB_CRYPTO_TAKER_FEE);
      expect(withDefault).toBe(withExplicit);
    });

    it('should equal gross profit rate when fees are zero', () => {
      const grossRate = calculateDipArbProfitRate(0.90);
      const netRateNoFees = calculateDipArbNetProfitRate(0.90, 0);
      // Should be approximately equal (slight differences due to rate calculation method)
      expect(Math.abs(grossRate - netRateNoFees)).toBeLessThan(0.001);
    });
  });

  describe('getMaxSumTargetForNetProfit', () => {
    it('should calculate max sumTarget for given net profit goal', () => {
      // For 2% net profit with 3% fees per leg
      const maxTarget = getMaxSumTargetForNetProfit(0.02, 0.03);

      // Verify the calculated target yields ~2% net profit
      const netProfit = calculateDipArbNetProfitRate(maxTarget, 0.03);
      expect(netProfit).toBeCloseTo(0.02, 2);
    });

    it('should return ~1.0 for zero profit requirement', () => {
      const maxTarget = getMaxSumTargetForNetProfit(0, 0.03);
      // For break-even with 6% fees, need totalCost of ~0.9434
      expect(maxTarget).toBeGreaterThan(0.9);
      expect(maxTarget).toBeLessThan(1);
    });

    it('should decrease as profit requirement increases', () => {
      const target1 = getMaxSumTargetForNetProfit(0.01, 0.03);
      const target2 = getMaxSumTargetForNetProfit(0.02, 0.03);
      const target5 = getMaxSumTargetForNetProfit(0.05, 0.03);

      expect(target2).toBeLessThan(target1);
      expect(target5).toBeLessThan(target2);
    });

    it('should return ~1.0 for zero-fee markets', () => {
      // With no fees, can profit on any cost < $1
      const target = getMaxSumTargetForNetProfit(0.01, 0);
      expect(target).toBeCloseTo(0.99, 2);
    });
  });

  describe('estimateUpWinRate', () => {
    it('should return 50% when current price equals price to beat', () => {
      expect(estimateUpWinRate(100, 100)).toBe(0.5);
    });

    it('should return higher probability when price is above target', () => {
      // Current price > price to beat = UP more likely
      const winRate = estimateUpWinRate(105, 100);
      expect(winRate).toBeGreaterThan(0.5);
    });

    it('should return lower probability when price is below target', () => {
      // Current price < price to beat = DOWN more likely
      const winRate = estimateUpWinRate(95, 100);
      expect(winRate).toBeLessThan(0.5);
    });

    it('should handle edge cases', () => {
      // Very far above target
      const high = estimateUpWinRate(200, 100);
      expect(high).toBeGreaterThan(0.8);
      expect(high).toBeLessThanOrEqual(1);

      // Very far below target
      const low = estimateUpWinRate(50, 100);
      expect(low).toBeLessThan(0.2);
      expect(low).toBeGreaterThanOrEqual(0);
    });
  });

  describe('createDipArbRoundState', () => {
    it('should create a valid round state with actual market endTime', () => {
      const actualEndTime = Date.now() + 10 * 60 * 1000; // 10 minutes from now
      const state = createDipArbRoundState(
        'test-round-123',
        50000, // $50,000 price to beat
        0.5,   // UP price
        0.5,   // DOWN price
        actualEndTime  // ACTUAL market end time
      );

      expect(state.roundId).toBe('test-round-123');
      expect(state.priceToBeat).toBe(50000);
      expect(state.openPrices.up).toBe(0.5);
      expect(state.openPrices.down).toBe(0.5);
      expect(state.phase).toBe('waiting');
      expect(state.leg1).toBeUndefined();
      expect(state.leg2).toBeUndefined();
      // End time should match what we passed, NOT calculated from duration
      expect(state.endTime).toBe(actualEndTime);
    });

    it('should accept Date object for marketEndTime', () => {
      const actualEndDate = new Date(Date.now() + 15 * 60 * 1000);
      const state = createDipArbRoundState(
        'test-round-date',
        94000,
        0.4,
        0.6,
        actualEndDate
      );

      expect(state.endTime).toBe(actualEndDate.getTime());
    });

    it('should fallback to duration calculation when no endTime provided', () => {
      const beforeCreate = Date.now();
      const state = createDipArbRoundState(
        'test-round-fallback',
        94000,
        0.5,
        0.5,
        undefined,  // No market end time
        15          // 15 minute duration
      );
      const afterCreate = Date.now();

      // End time should be approximately 15 minutes from creation
      const expectedEnd = beforeCreate + 15 * 60 * 1000;
      expect(state.endTime).toBeGreaterThanOrEqual(expectedEnd);
      expect(state.endTime).toBeLessThanOrEqual(afterCreate + 15 * 60 * 1000);
    });
  });

  describe('Profit Calculation Helpers', () => {
    const FEE_RATE = DIP_ARB_CRYPTO_TAKER_FEE; // 0.03

    describe('calculateDipArbLeg2NetProfit', () => {
      it('should calculate correct NET profit', () => {
        // leg1: 0.30, leg2: 0.33, shares: 50
        // totalCost = 0.63, gross = 0.37, fees = 0.0378
        // net = 0.3322 per share, total = 16.61
        const result = calculateDipArbLeg2NetProfit(0.30, 0.33, 50, FEE_RATE);
        expect(result).toBeCloseTo(16.61, 1);
      });

      it('should return negative for unprofitable trades', () => {
        // totalCost = 0.95, gross = 0.05, fees = 0.057
        // net = -0.007 per share (losing trade after fees)
        const result = calculateDipArbLeg2NetProfit(0.50, 0.45, 50, FEE_RATE);
        expect(result).toBeLessThan(0);
      });

      it('should use default fee rate when not provided', () => {
        const withExplicit = calculateDipArbLeg2NetProfit(0.30, 0.33, 50, FEE_RATE);
        const withDefault = calculateDipArbLeg2NetProfit(0.30, 0.33, 50);
        expect(withDefault).toEqual(withExplicit);
      });
    });

    describe('calculateDipArbSettlementWinProfit', () => {
      it('should subtract entry fee from settlement win', () => {
        // leg1Cost = 7.50 (50 shares @ 0.15)
        // received = 50, entryFee = 0.225
        // net = 50 - 7.50 - 0.225 = 42.275
        const result = calculateDipArbSettlementWinProfit(7.50, 50, FEE_RATE);
        expect(result).toBeCloseTo(42.275, 2);
      });

      it('should use default fee rate when not provided', () => {
        const withExplicit = calculateDipArbSettlementWinProfit(7.50, 50, FEE_RATE);
        const withDefault = calculateDipArbSettlementWinProfit(7.50, 50);
        expect(withDefault).toEqual(withExplicit);
      });
    });

    describe('calculateDipArbExitValue', () => {
      it('should apply single 3% fee (not 6%)', () => {
        // exitPrice = 0.20, shares = 50
        // value = 10 * 0.97 = 9.70
        const result = calculateDipArbExitValue(0.20, 50, FEE_RATE);
        expect(result).toBeCloseTo(9.70, 2);
      });

      it('should use default fee rate when not provided', () => {
        const withExplicit = calculateDipArbExitValue(0.20, 50, FEE_RATE);
        const withDefault = calculateDipArbExitValue(0.20, 50);
        expect(withDefault).toEqual(withExplicit);
      });
    });
  });
});
