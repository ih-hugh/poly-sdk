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
      // Key safety parameters
      expect(DEFAULT_DIP_ARB_CONFIG.minDipSidePrice).toBe(0.10);
      expect(DEFAULT_DIP_ARB_CONFIG.maxLeg1Price).toBe(0.70);
      expect(DEFAULT_DIP_ARB_CONFIG.maxOpenPositions).toBe(10);

      // Profitability parameters
      expect(DEFAULT_DIP_ARB_CONFIG.sumTarget).toBe(0.92);
      expect(DEFAULT_DIP_ARB_CONFIG.dipThreshold).toBe(0.02);

      // Paper mode should be default (safe)
      expect(DEFAULT_DIP_ARB_CONFIG.paperMode).toBe(true);
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
});
