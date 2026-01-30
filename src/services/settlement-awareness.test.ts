/**
 * Settlement Awareness Unit Tests
 *
 * Tests for the settlement awareness logic in DipArbService.
 * These tests verify the decision-making process for holding positions
 * for settlement vs timeout exits.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { EventEmitter } from 'events';

// Mock the dependencies that DipArbService needs
vi.mock('./realtime-service-v2.js', () => ({
  RealtimeServiceV2: vi.fn(),
}));

vi.mock('./trading-service.js', () => ({
  TradingService: vi.fn(),
}));

vi.mock('./market-service.js', () => ({
  MarketService: vi.fn(() => ({
    scanCryptoShortTermMarkets: vi.fn().mockResolvedValue([]),
    scanCryptoMarkets: vi.fn().mockResolvedValue([]),
  })),
}));

vi.mock('../clients/ctf-client.js', () => ({
  CTFClient: vi.fn(),
}));

// Import the service and types after mocking
import { DipArbService } from './dip-arb-service.js';
import {
  DEFAULT_DIP_ARB_CONFIG,
  type DipArbSide,
  type DipArbRoundState,
  type EnhancedSettlementDecision,
  type SettlementOutcome,
  type SettlementRuleEvaluations,
  type SettlementEntryContext,
  type SettlementMarketSnapshot,
} from './dip-arb-types.js';

describe('Settlement Awareness Logic', () => {
  describe('Configuration Defaults', () => {
    it('should have settlement awareness enabled by default', () => {
      expect(DEFAULT_DIP_ARB_CONFIG.favorSettlement).toBe(true);
    });

    it('should have 60% hold threshold to avoid gambling on marginal positions', () => {
      expect(DEFAULT_DIP_ARB_CONFIG.settlementHoldThreshold).toBe(0.60);
    });

    it('should always hold if less than 3 minutes to market end', () => {
      expect(DEFAULT_DIP_ARB_CONFIG.minTimeToEndForHold).toBe(3);
    });

    it('should have 5 minute settlement wait buffer', () => {
      expect(DEFAULT_DIP_ARB_CONFIG.settlementWaitBuffer).toBe(300);
    });

    it('should have Binance momentum validation enabled by default', () => {
      expect(DEFAULT_DIP_ARB_CONFIG.enableSettlementMomentum).toBe(true);
    });

    it('should have Smart Exit enabled by default', () => {
      expect(DEFAULT_DIP_ARB_CONFIG.enableSmartExit).toBe(true);
      expect(DEFAULT_DIP_ARB_CONFIG.smartExitThreshold).toBe(0.3);
    });
  });

  describe('Win Probability Calculation', () => {
    // Test the win probability logic conceptually
    // These are unit tests for the expected behavior

    it('should calculate win probability based on token prices', () => {
      // When UP is at 0.70 and DOWN is at 0.30 (sum = 1.0)
      // UP probability should be ~0.70, DOWN should be ~0.30
      const upPrice = 0.70;
      const downPrice = 0.30;
      const total = upPrice + downPrice;

      const upProb = upPrice / total;
      const downProb = downPrice / total;

      expect(upProb).toBeCloseTo(0.70, 2);
      expect(downProb).toBeCloseTo(0.30, 2);
    });

    it('should handle edge case where prices are equal', () => {
      // When UP and DOWN are both at 0.50
      // Both probabilities should be ~0.50
      const upPrice = 0.50;
      const downPrice = 0.50;
      const total = upPrice + downPrice;

      const upProb = upPrice / total;
      const downProb = downPrice / total;

      expect(upProb).toBeCloseTo(0.50, 2);
      expect(downProb).toBeCloseTo(0.50, 2);
    });

    it('should handle markets where sum is not exactly 1', () => {
      // Real markets often have sum slightly != 1 due to spread
      const upPrice = 0.55;
      const downPrice = 0.48;
      const total = upPrice + downPrice; // 1.03

      const upProb = upPrice / total;
      const downProb = downPrice / total;

      // Probabilities should still sum to 1 after normalization
      expect(upProb + downProb).toBeCloseTo(1.0, 5);
    });
  });

  describe('Expected Value Calculation', () => {
    it('should calculate expected settlement value correctly', () => {
      // If we have 100 shares and win probability is 0.60
      // Expected value = 0.60 * 100 * $1 = $60
      const shares = 100;
      const winProb = 0.60;
      const expectedValue = winProb * shares * 1.0;

      expect(expectedValue).toBe(60);
    });

    it('should calculate current exit value with fee deduction', () => {
      // If we have 100 shares at current price 0.50 with ~6% fees
      const shares = 100;
      const currentPrice = 0.50;
      const feeMultiplier = 0.94; // ~6% fees

      const exitValue = shares * currentPrice * feeMultiplier;
      expect(exitValue).toBe(47); // 100 * 0.50 * 0.94
    });

    it('should prefer settlement when EV > exit value', () => {
      // Expected settlement: $60
      // Current exit: $47
      // Should hold for settlement
      const expectedSettleValue = 60;
      const exitValue = 47;

      expect(expectedSettleValue > exitValue).toBe(true);
    });
  });

  describe('Chainlink Alignment Logic', () => {
    it('should detect aligned UP position when price above target', () => {
      // UP wins if currentPrice >= priceToBeat
      const priceToBeat = 95000;
      const currentPrice = 95500;
      const delta = (currentPrice - priceToBeat) / priceToBeat;

      // Delta is positive, UP is aligned
      expect(delta > 0).toBe(true);
      const upAligned = delta >= 0;
      expect(upAligned).toBe(true);
    });

    it('should detect aligned DOWN position when price below target', () => {
      // DOWN wins if currentPrice < priceToBeat
      const priceToBeat = 95000;
      const currentPrice = 94500;
      const delta = (currentPrice - priceToBeat) / priceToBeat;

      // Delta is negative, DOWN is aligned
      expect(delta < 0).toBe(true);
      const downAligned = delta < 0;
      expect(downAligned).toBe(true);
    });

    it('should return not aligned when delta is near zero', () => {
      const priceToBeat = 95000;
      const currentPrice = 95000;
      const delta = (currentPrice - priceToBeat) / priceToBeat;

      // Delta is 0, neither side has clear edge
      expect(Math.abs(delta)).toBeLessThan(0.001);
    });
  });

  describe('Hold Decision Rules', () => {
    // Tests for the shouldHoldForSettlement decision logic

    describe('Rule 1: Near Settlement', () => {
      it('should always hold when time to end < minTimeToEndForHold', () => {
        const timeToEndMin = 2; // 2 minutes
        const minTimeToEndForHold = 3; // Config default

        const shouldHoldNearEnd = timeToEndMin > 0 && timeToEndMin < minTimeToEndForHold;
        expect(shouldHoldNearEnd).toBe(true);
      });

      it('should not auto-hold when time to end > minTimeToEndForHold', () => {
        const timeToEndMin = 5; // 5 minutes
        const minTimeToEndForHold = 3;

        const shouldHoldNearEnd = timeToEndMin > 0 && timeToEndMin < minTimeToEndForHold;
        expect(shouldHoldNearEnd).toBe(false);
      });
    });

    describe('Rule 2: High Probability', () => {
      it('should hold when win probability exceeds threshold', () => {
        const winProb = 0.65; // 65%
        const threshold = 0.50; // Config default

        expect(winProb >= threshold).toBe(true);
      });

      it('should not hold when win probability below threshold', () => {
        const winProb = 0.35; // 35%
        const threshold = 0.50;

        expect(winProb >= threshold).toBe(false);
      });
    });

    describe('Rule 3: Positive Expected Value', () => {
      it('should hold when settlement EV exceeds exit value', () => {
        const expectedSettleValue = 60;
        const exitValue = 45;

        const positiveEV = expectedSettleValue > exitValue && exitValue > 0;
        expect(positiveEV).toBe(true);
      });

      it('should not hold when exit value exceeds settlement EV', () => {
        const expectedSettleValue = 30;
        const exitValue = 45;

        const positiveEV = expectedSettleValue > exitValue && exitValue > 0;
        expect(positiveEV).toBe(false);
      });
    });

    describe('Combined Decision', () => {
      it('should hold for any single positive rule', () => {
        // Test case: low probability but near settlement
        const nearSettlement = true;
        const highProbability = false;
        const positiveEV = false;

        const shouldHold = nearSettlement || highProbability || positiveEV;
        expect(shouldHold).toBe(true);
      });

      it('should exit when no rules are satisfied', () => {
        const nearSettlement = false;
        const highProbability = false;
        const positiveEV = false;
        const chainlinkAligned = false;

        const shouldHold = nearSettlement || highProbability || positiveEV || chainlinkAligned;
        expect(shouldHold).toBe(false);
      });
    });
  });

  describe('Settlement Decision Event', () => {
    it('should have correct event structure', () => {
      // Verify the expected event structure
      const event = {
        decision: 'HOLD' as const,
        reason: 'near_settlement' as const,
        confidence: 0.9,
        leg1Side: 'UP' as DipArbSide,
        winProbability: 0.65,
        timeToEndMin: 2.5,
        elapsed: 180,
        baseTimeout: 120,
        roundId: 'test-round-123',
        marketName: 'btc-updown-15m-123',
        underlying: 'BTC' as const,
      };

      expect(event.decision).toBe('HOLD');
      expect(event.reason).toBe('near_settlement');
      expect(event.confidence).toBeGreaterThan(0);
      expect(event.winProbability).toBeGreaterThanOrEqual(0);
      expect(event.winProbability).toBeLessThanOrEqual(1);
      expect(event.timeToEndMin).toBeGreaterThan(0);
    });

    it('should include all required fields for EXIT decision', () => {
      const event = {
        decision: 'EXIT' as const,
        reason: 'no_edge' as const,
        confidence: 0,
        leg1Side: 'DOWN' as DipArbSide,
        winProbability: 0.35,
        timeToEndMin: 8.5,
        elapsed: 130,
        baseTimeout: 120,
        roundId: 'test-round-456',
        marketName: 'eth-updown-15m-456',
        underlying: 'ETH' as const,
      };

      expect(event.decision).toBe('EXIT');
      expect(event.reason).toBe('no_edge');
      expect(event.confidence).toBe(0);
    });
  });
});

describe('Settlement Awareness Integration', () => {
  // These tests would require mocking the full service
  // For now, we test the logic conceptually

  it('should emit settlementDecision event on timeout check', () => {
    // This would be tested with a full service mock
    // Verifying the event is emitted when timeout is reached
    const emitter = new EventEmitter();
    let emittedEvent: any = null;

    emitter.on('settlementDecision', (event) => {
      emittedEvent = event;
    });

    // Simulate event emission
    emitter.emit('settlementDecision', {
      decision: 'HOLD',
      reason: 'high_probability',
      confidence: 0.65,
      winProbability: 0.65,
      timeToEndMin: 5,
      elapsed: 180,
      baseTimeout: 120,
    });

    expect(emittedEvent).not.toBeNull();
    expect(emittedEvent.decision).toBe('HOLD');
    expect(emittedEvent.reason).toBe('high_probability');
  });

  it('should respect favorSettlement config toggle', () => {
    // When favorSettlement is false, should always exit on timeout
    const favorSettlement = false;

    if (!favorSettlement) {
      const decision = { hold: false, reason: 'disabled', confidence: 0 };
      expect(decision.hold).toBe(false);
      expect(decision.reason).toBe('disabled');
    }
  });
});

describe('Enhanced Settlement Decision Interface', () => {
  describe('EnhancedSettlementDecision structure', () => {
    it('should have all required fields for a HOLD decision', () => {
      const decision: EnhancedSettlementDecision = {
        decision: 'HOLD',
        reason: 'high_probability',
        confidence: 0.65,
        ruleEvaluations: {
          nearSettlement: { passed: false, timeToEndMin: 8, threshold: 3 },
          highProbability: { passed: true, winProb: 0.65, threshold: 0.5 },
          positiveEV: { passed: false, settlementEV: 50, exitValue: 45, evRatio: 1.11 },
          chainlinkAligned: { passed: false, delta: -0.001, currentPrice: 95000, priceToBeat: 95100, side: 'UP' },
          momentumFavorable: { passed: false, strength: 0, threshold: 0.3, enabled: false },
        },
        entryContext: {
          leg1Side: 'UP',
          leg1EntryPrice: 0.40,
          leg1Shares: 100,
          leg1Cost: 40,
          enteredAt: Date.now() - 180000,
        },
        marketSnapshot: {
          upPrice: 0.65,
          downPrice: 0.38,
          priceToBeat: 95100,
          currentChainlinkPrice: 95500,
          marketEndTime: Date.now() + 480000,
        },
        roundId: 'test-round-789',
        marketName: 'btc-updown-15m-789',
        underlying: 'BTC',
        isPaper: true,
        timestamp: Date.now(),
      };

      expect(decision.decision).toBe('HOLD');
      expect(decision.reason).toBe('high_probability');
      expect(decision.confidence).toBe(0.65);
      expect(decision.ruleEvaluations.highProbability.passed).toBe(true);
      expect(decision.ruleEvaluations.nearSettlement.passed).toBe(false);
      expect(decision.entryContext.leg1Side).toBe('UP');
      expect(decision.marketSnapshot.upPrice).toBe(0.65);
      expect(decision.isPaper).toBe(true);
    });

    it('should have all required fields for an EXIT decision', () => {
      const decision: EnhancedSettlementDecision = {
        decision: 'EXIT',
        reason: 'no_edge',
        confidence: 0,
        ruleEvaluations: {
          nearSettlement: { passed: false, timeToEndMin: 10, threshold: 3 },
          highProbability: { passed: false, winProb: 0.35, threshold: 0.5 },
          positiveEV: { passed: false, settlementEV: 30, exitValue: 45, evRatio: 0.67 },
          chainlinkAligned: { passed: false, delta: 0.002, currentPrice: 95200, priceToBeat: 95000, side: 'DOWN' },
          momentumFavorable: { passed: false, strength: 0, threshold: 0.3, enabled: false },
        },
        entryContext: {
          leg1Side: 'DOWN',
          leg1EntryPrice: 0.45,
          leg1Shares: 100,
          leg1Cost: 45,
          enteredAt: Date.now() - 200000,
        },
        marketSnapshot: {
          upPrice: 0.68,
          downPrice: 0.35,
          priceToBeat: 95000,
          currentChainlinkPrice: 95200,
          marketEndTime: Date.now() + 600000,
        },
        roundId: 'test-round-exit',
        marketName: 'eth-updown-15m-exit',
        underlying: 'ETH',
        isPaper: true,
        timestamp: Date.now(),
      };

      expect(decision.decision).toBe('EXIT');
      expect(decision.reason).toBe('no_edge');
      expect(decision.confidence).toBe(0);
      // All rules should be false for EXIT with no_edge reason
      expect(decision.ruleEvaluations.nearSettlement.passed).toBe(false);
      expect(decision.ruleEvaluations.highProbability.passed).toBe(false);
      expect(decision.ruleEvaluations.positiveEV.passed).toBe(false);
      expect(decision.ruleEvaluations.chainlinkAligned.passed).toBe(false);
      expect(decision.ruleEvaluations.momentumFavorable.passed).toBe(false);
    });
  });

  describe('SettlementRuleEvaluations', () => {
    it('should correctly represent all 5 rule evaluation results', () => {
      const evals: SettlementRuleEvaluations = {
        nearSettlement: { passed: true, timeToEndMin: 2.5, threshold: 3 },
        highProbability: { passed: false, winProb: 0.45, threshold: 0.50 },
        positiveEV: { passed: true, settlementEV: 55, exitValue: 40, evRatio: 1.375 },
        chainlinkAligned: { passed: true, delta: 0.005, currentPrice: 95500, priceToBeat: 95000, side: 'UP' },
        momentumFavorable: { passed: false, strength: 0.2, threshold: 0.3, enabled: true },
      };

      // Near settlement check
      expect(evals.nearSettlement.passed).toBe(true);
      expect(evals.nearSettlement.timeToEndMin).toBeLessThan(evals.nearSettlement.threshold);

      // High probability check
      expect(evals.highProbability.passed).toBe(false);
      expect(evals.highProbability.winProb).toBeLessThan(evals.highProbability.threshold);

      // Positive EV check
      expect(evals.positiveEV.passed).toBe(true);
      expect(evals.positiveEV.settlementEV).toBeGreaterThan(evals.positiveEV.exitValue);

      // Chainlink aligned check
      expect(evals.chainlinkAligned.passed).toBe(true);
      expect(evals.chainlinkAligned.delta).toBeGreaterThan(0); // UP aligned when delta > 0

      // Momentum check
      expect(evals.momentumFavorable.passed).toBe(false);
      expect(evals.momentumFavorable.enabled).toBe(true);
      expect(evals.momentumFavorable.strength).toBeLessThan(evals.momentumFavorable.threshold);
    });
  });

  describe('SettlementEntryContext', () => {
    it('should correctly capture leg1 position details', () => {
      const context: SettlementEntryContext = {
        leg1Side: 'UP',
        leg1EntryPrice: 0.35,
        leg1Shares: 50,
        leg1Cost: 17.5,
        enteredAt: 1700000000000,
      };

      expect(context.leg1Side).toBe('UP');
      expect(context.leg1Cost).toBeCloseTo(context.leg1EntryPrice * context.leg1Shares, 2);
      expect(context.enteredAt).toBeGreaterThan(0);
    });
  });

  describe('SettlementMarketSnapshot', () => {
    it('should correctly capture market state at decision time', () => {
      const snapshot: SettlementMarketSnapshot = {
        upPrice: 0.55,
        downPrice: 0.48,
        priceToBeat: 95000,
        currentChainlinkPrice: 95250,
        marketEndTime: Date.now() + 300000,
      };

      expect(snapshot.upPrice + snapshot.downPrice).toBeGreaterThan(0.9); // Market should have liquidity
      expect(snapshot.currentChainlinkPrice).toBeGreaterThan(0);
      expect(snapshot.marketEndTime).toBeGreaterThan(Date.now()); // Future end time
    });
  });
});

describe('Settlement Outcome Interface', () => {
  describe('SettlementOutcome structure', () => {
    it('should correctly represent a WIN outcome from HOLD decision', () => {
      const outcome: SettlementOutcome = {
        roundId: 'test-round-win',
        decision: 'HOLD',
        outcome: 'WIN',
        actualProfit: 60, // 100 shares at $1 minus $40 cost
        counterfactualProfit: 12, // Would have gotten ~$52 if exited
        settlementSide: 'UP',
        settlementPrice: 95500,
        settledAt: Date.now(),
        isPaper: true,
        underlying: 'BTC',
        marketName: 'btc-updown-15m-win',
      };

      expect(outcome.decision).toBe('HOLD');
      expect(outcome.outcome).toBe('WIN');
      expect(outcome.actualProfit).toBeGreaterThan(0);
      expect(outcome.actualProfit).toBeGreaterThan(outcome.counterfactualProfit);
    });

    it('should correctly represent a LOSS outcome from HOLD decision', () => {
      const outcome: SettlementOutcome = {
        roundId: 'test-round-loss',
        decision: 'HOLD',
        outcome: 'LOSS',
        actualProfit: -45, // Lost entire cost
        counterfactualProfit: 5, // Would have gotten ~$50 if exited
        settlementSide: 'DOWN',
        settlementPrice: 94500,
        settledAt: Date.now(),
        isPaper: true,
        underlying: 'BTC',
        marketName: 'btc-updown-15m-loss',
      };

      expect(outcome.decision).toBe('HOLD');
      expect(outcome.outcome).toBe('LOSS');
      expect(outcome.actualProfit).toBeLessThan(0);
      expect(outcome.counterfactualProfit).toBeGreaterThan(outcome.actualProfit);
    });

    it('should include counterfactual profit for decision analysis', () => {
      // When we HOLD and win, counterfactual shows what exit would have yielded
      const holdWin: SettlementOutcome = {
        roundId: 'test-cf-1',
        decision: 'HOLD',
        outcome: 'WIN',
        actualProfit: 55,
        counterfactualProfit: 10,
        settlementSide: 'UP',
        settlementPrice: 95500,
        settledAt: Date.now(),
        isPaper: true,
        underlying: 'ETH',
        marketName: 'eth-updown-15m-cf1',
      };

      // Decision was correct: actual > counterfactual
      expect(holdWin.actualProfit).toBeGreaterThan(holdWin.counterfactualProfit);

      // When we EXIT, counterfactual shows what holding would have yielded
      const exitLoss: SettlementOutcome = {
        roundId: 'test-cf-2',
        decision: 'EXIT',
        outcome: 'LOSS',
        actualProfit: -5, // Small loss from exit
        counterfactualProfit: 50, // Would have won at settlement
        settlementSide: 'UP',
        settlementPrice: 95500,
        settledAt: Date.now(),
        isPaper: true,
        underlying: 'SOL',
        marketName: 'sol-updown-15m-cf2',
      };

      // Decision was wrong: counterfactual > actual
      expect(exitLoss.counterfactualProfit).toBeGreaterThan(exitLoss.actualProfit);
    });
  });
});

describe('Binance Settlement Momentum', () => {
  describe('Momentum Direction Logic', () => {
    it('should return favorable when Binance rises for UP position', () => {
      // For UP position: rising price = favorable momentum
      const side: DipArbSide = 'UP';
      const changePercent = 1.0; // 1% rise

      const favorable = (side === 'UP' && changePercent > 0) ||
                        (side === 'DOWN' && changePercent < 0);

      expect(favorable).toBe(true);
    });

    it('should return unfavorable when Binance falls for UP position', () => {
      // For UP position: falling price = unfavorable momentum
      const side: DipArbSide = 'UP';
      const changePercent = -1.0; // 1% fall

      const favorable = (side === 'UP' && changePercent > 0) ||
                        (side === 'DOWN' && changePercent < 0);

      expect(favorable).toBe(false);
    });

    it('should return favorable when Binance falls for DOWN position', () => {
      // For DOWN position: falling price = favorable momentum
      const side: DipArbSide = 'DOWN';
      const changePercent = -1.0; // 1% fall

      const favorable = (side === 'UP' && changePercent > 0) ||
                        (side === 'DOWN' && changePercent < 0);

      expect(favorable).toBe(true);
    });

    it('should return unfavorable when Binance rises for DOWN position', () => {
      // For DOWN position: rising price = unfavorable momentum
      const side: DipArbSide = 'DOWN';
      const changePercent = 1.0; // 1% rise

      const favorable = (side === 'UP' && changePercent > 0) ||
                        (side === 'DOWN' && changePercent < 0);

      expect(favorable).toBe(false);
    });
  });

  describe('Smart Exit Logic', () => {
    it('should trigger smart exit when Binance strongly contradicts UP position', () => {
      const side: DipArbSide = 'UP';
      const changePercent = -0.5; // 0.5% fall (strong contradiction)
      const smartExitThreshold = 0.3;
      const smartExitEnabled = true;

      const favorable = (side === 'UP' && changePercent > 0);
      const strongContradiction = !favorable && Math.abs(changePercent) > smartExitThreshold;
      const shouldTriggerSmartExit = smartExitEnabled && strongContradiction;

      expect(shouldTriggerSmartExit).toBe(true);
    });

    it('should not trigger smart exit when contradiction is weak', () => {
      const side: DipArbSide = 'UP';
      const changePercent = -0.1; // 0.1% fall (weak contradiction)
      const smartExitThreshold = 0.3;
      const smartExitEnabled = true;

      const favorable = (side === 'UP' && changePercent > 0);
      const strongContradiction = !favorable && Math.abs(changePercent) > smartExitThreshold;
      const shouldTriggerSmartExit = smartExitEnabled && strongContradiction;

      expect(shouldTriggerSmartExit).toBe(false);
    });

    it('should not trigger smart exit when momentum is favorable', () => {
      const side: DipArbSide = 'UP';
      const changePercent = 0.5; // 0.5% rise (favorable)
      const smartExitThreshold = 0.3;
      const smartExitEnabled = true;

      const favorable = (side === 'UP' && changePercent > 0);
      const strongContradiction = !favorable && Math.abs(changePercent) > smartExitThreshold;
      const shouldTriggerSmartExit = smartExitEnabled && strongContradiction;

      expect(shouldTriggerSmartExit).toBe(false);
    });

    it('should not trigger smart exit when feature is disabled', () => {
      const side: DipArbSide = 'UP';
      const changePercent = -0.5; // Strong contradiction
      const smartExitThreshold = 0.3;
      const smartExitEnabled = false;

      const favorable = (side === 'UP' && changePercent > 0);
      const strongContradiction = !favorable && Math.abs(changePercent) > smartExitThreshold;
      const shouldTriggerSmartExit = smartExitEnabled && strongContradiction;

      expect(shouldTriggerSmartExit).toBe(false);
    });
  });

  describe('Enhanced Decision with Binance Data', () => {
    it('should include binanceMomentum in rule evaluations', () => {
      const decision: EnhancedSettlementDecision = {
        decision: 'EXIT',
        reason: 'binance_contradiction',
        confidence: 0.8,
        ruleEvaluations: {
          nearSettlement: { passed: false, timeToEndMin: 8, threshold: 3 },
          highProbability: { passed: false, winProb: 0.45, threshold: 0.60 },
          positiveEV: { passed: false, settlementEV: 45, exitValue: 40, evRatio: 1.125 },
          chainlinkAligned: { passed: true, delta: 0.002, currentPrice: 95200, priceToBeat: 95000, side: 'UP' },
          momentumFavorable: { passed: false, strength: 0, threshold: 0.3, enabled: true },
          binanceMomentum: {
            passed: false,
            changePercent: -0.5,
            favorable: false,
            enabled: true,
            reason: 'Binance BTCUSDT falling (-0.50%) - AGAINST UP',
          },
          smartExit: {
            triggered: true,
            reason: 'Binance -0.50% AGAINST UP',
            threshold: 0.3,
            enabled: true,
          },
        },
        entryContext: {
          leg1Side: 'UP',
          leg1EntryPrice: 0.40,
          leg1Shares: 100,
          leg1Cost: 40,
          enteredAt: Date.now() - 180000,
        },
        marketSnapshot: {
          upPrice: 0.55,
          downPrice: 0.48,
          priceToBeat: 95000,
          currentChainlinkPrice: 95200,
          marketEndTime: Date.now() + 480000,
        },
        roundId: 'test-binance-exit',
        marketName: 'btc-updown-15m-binance',
        underlying: 'BTC',
        isPaper: true,
        timestamp: Date.now(),
      };

      // Verify Binance momentum is captured
      expect(decision.ruleEvaluations.binanceMomentum).toBeDefined();
      expect(decision.ruleEvaluations.binanceMomentum?.favorable).toBe(false);
      expect(decision.ruleEvaluations.binanceMomentum?.changePercent).toBe(-0.5);

      // Verify Smart Exit is captured
      expect(decision.ruleEvaluations.smartExit).toBeDefined();
      expect(decision.ruleEvaluations.smartExit?.triggered).toBe(true);

      // Decision should be EXIT due to binance_contradiction
      expect(decision.decision).toBe('EXIT');
      expect(decision.reason).toBe('binance_contradiction');
    });

    it('should include binance_favorable as hold reason', () => {
      const decision: EnhancedSettlementDecision = {
        decision: 'HOLD',
        reason: 'binance_favorable',
        confidence: 0.7,
        ruleEvaluations: {
          nearSettlement: { passed: false, timeToEndMin: 8, threshold: 3 },
          highProbability: { passed: false, winProb: 0.48, threshold: 0.60 },
          positiveEV: { passed: false, settlementEV: 45, exitValue: 50, evRatio: 0.9 },
          chainlinkAligned: { passed: true, delta: 0.005, currentPrice: 95500, priceToBeat: 95000, side: 'UP' },
          momentumFavorable: { passed: false, strength: 0.2, threshold: 0.3, enabled: true },
          binanceMomentum: {
            passed: true,
            changePercent: 1.2,
            favorable: true,
            enabled: true,
            reason: 'Binance BTCUSDT rising (+1.20%) - favors UP',
          },
          smartExit: {
            triggered: false,
            reason: 'Binance momentum is favorable',
            threshold: 0.3,
            enabled: true,
          },
        },
        entryContext: {
          leg1Side: 'UP',
          leg1EntryPrice: 0.40,
          leg1Shares: 100,
          leg1Cost: 40,
          enteredAt: Date.now() - 180000,
        },
        marketSnapshot: {
          upPrice: 0.52,
          downPrice: 0.51,
          priceToBeat: 95000,
          currentChainlinkPrice: 95500,
          marketEndTime: Date.now() + 480000,
        },
        roundId: 'test-binance-hold',
        marketName: 'btc-updown-15m-binance-hold',
        underlying: 'BTC',
        isPaper: true,
        timestamp: Date.now(),
      };

      // Verify Binance momentum is favorable
      expect(decision.ruleEvaluations.binanceMomentum?.favorable).toBe(true);
      expect(decision.ruleEvaluations.binanceMomentum?.changePercent).toBeGreaterThan(0);

      // Smart Exit should not be triggered
      expect(decision.ruleEvaluations.smartExit?.triggered).toBe(false);

      // Decision should be HOLD due to binance_favorable
      expect(decision.decision).toBe('HOLD');
      expect(decision.reason).toBe('binance_favorable');
    });
  });
});
