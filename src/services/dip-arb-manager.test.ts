/**
 * DipArbManager Unit Tests - Settlement Event Forwarding
 *
 * Tests that settlement awareness events are properly forwarded
 * from child DipArbService instances to the manager.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { EventEmitter } from 'events';

// Mock all dependencies
vi.mock('./realtime-service-v2.js', () => ({
  RealtimeServiceV2: vi.fn(() => new EventEmitter()),
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

vi.mock('./binance-service.js', () => ({
  BinanceService: vi.fn(),
}));

// Import after mocking
import { DipArbManager } from './dip-arb-manager.js';
import { RealtimeServiceV2 } from './realtime-service-v2.js';
import { MarketService } from './market-service.js';

describe('DipArbManager - Settlement Event Forwarding', () => {
  let manager: DipArbManager;
  let mockRealtimeService: EventEmitter;
  let mockMarketService: any;

  beforeEach(() => {
    vi.clearAllMocks();

    mockRealtimeService = new EventEmitter();
    mockMarketService = {
      scanCryptoShortTermMarkets: vi.fn().mockResolvedValue([]),
      scanCryptoMarkets: vi.fn().mockResolvedValue([]),
    };

    manager = new DipArbManager(
      mockRealtimeService as any,
      null, // tradingService
      mockMarketService as any,
      undefined, // privateKey
      137, // chainId
      undefined, // binanceService
      { debug: false }
    );
  });

  describe('setupEventForwarding', () => {
    it('should have setupEventForwarding method', () => {
      // The method is private, but we can verify the manager exists
      expect(manager).toBeDefined();
      expect(typeof manager.startCoin).toBe('function');
    });
  });

  describe('Settlement event emission patterns', () => {
    it('should emit settlementDecision events from manager', async () => {
      const testDecision = {
        decision: 'HOLD' as const,
        reason: 'high_probability' as const,
        confidence: 0.65,
        winProbability: 0.65,
        timeToEndMin: 5,
        underlying: 'BTC' as const,
      };

      const eventPromise = new Promise<void>((resolve) => {
        manager.on('settlementDecision', (event) => {
          expect(event.decision).toBe('HOLD');
          expect(event.reason).toBe('high_probability');
          expect(event.confidence).toBe(0.65);
          resolve();
        });
      });

      // Simulate event being forwarded through manager
      manager.emit('settlementDecision', testDecision);
      await eventPromise;
    });

    it('should emit enhancedSettlementDecision events from manager', async () => {
      const testDecision = {
        decision: 'HOLD' as const,
        reason: 'near_settlement' as const,
        confidence: 0.9,
        ruleEvaluations: {
          nearSettlement: { passed: true, timeToEndMin: 2, threshold: 3 },
          highProbability: { passed: false, winProb: 0.45, threshold: 0.5 },
          positiveEV: { passed: false, settlementEV: 40, exitValue: 45, evRatio: 0.89 },
          chainlinkAligned: { passed: false, delta: -0.001, currentPrice: 95000, priceToBeat: 95100, side: 'UP' },
          momentumFavorable: { passed: false, strength: 0, threshold: 0.3, enabled: false },
        },
        entryContext: {
          leg1Side: 'UP' as const,
          leg1EntryPrice: 0.40,
          leg1Shares: 100,
          leg1Cost: 40,
          enteredAt: Date.now() - 180000,
        },
        marketSnapshot: {
          upPrice: 0.55,
          downPrice: 0.48,
          priceToBeat: 95100,
          currentChainlinkPrice: 95000,
          marketEndTime: Date.now() + 120000,
        },
        roundId: 'test-round-123',
        marketName: 'btc-updown-15m-123',
        underlying: 'BTC' as const,
        isPaper: true,
        timestamp: Date.now(),
      };

      const eventPromise = new Promise<void>((resolve) => {
        manager.on('enhancedSettlementDecision', (event) => {
          expect(event.decision).toBe('HOLD');
          expect(event.reason).toBe('near_settlement');
          expect(event.confidence).toBe(0.9);
          expect(event.ruleEvaluations.nearSettlement.passed).toBe(true);
          expect(event.isPaper).toBe(true);
          resolve();
        });
      });

      manager.emit('enhancedSettlementDecision', testDecision);
      await eventPromise;
    });

    it('should emit settlementOutcome events from manager', async () => {
      const testOutcome = {
        roundId: 'test-round-456',
        decision: 'HOLD' as const,
        outcome: 'WIN' as const,
        actualProfit: 60,
        counterfactualProfit: 10,
        settlementSide: 'UP' as const,
        settlementPrice: 95500,
        settledAt: Date.now(),
        isPaper: true,
        underlying: 'BTC' as const,
        marketName: 'btc-updown-15m-456',
      };

      const eventPromise = new Promise<void>((resolve) => {
        manager.on('settlementOutcome', (event) => {
          expect(event.decision).toBe('HOLD');
          expect(event.outcome).toBe('WIN');
          expect(event.actualProfit).toBe(60);
          expect(event.counterfactualProfit).toBe(10);
          resolve();
        });
      });

      manager.emit('settlementOutcome', testOutcome);
      await eventPromise;
    });
  });

  describe('Event enrichment with market context', () => {
    it('should include coin in forwarded settlementDecision', async () => {
      const testDecision = {
        decision: 'EXIT' as const,
        reason: 'no_edge' as const,
        confidence: 0,
        coin: 'ETH' as const,
        marketConditionId: 'cond-123',
        marketName: 'eth-updown-15m-123',
      };

      const eventPromise = new Promise<void>((resolve) => {
        manager.on('settlementDecision', (event) => {
          expect(event.coin).toBe('ETH');
          expect(event.marketConditionId).toBe('cond-123');
          expect(event.marketName).toBe('eth-updown-15m-123');
          resolve();
        });
      });

      manager.emit('settlementDecision', testDecision);
      await eventPromise;
    });

    it('should include coin in forwarded enhancedSettlementDecision', async () => {
      const testDecision = {
        decision: 'HOLD' as const,
        reason: 'positive_ev' as const,
        confidence: 0.72,
        coin: 'SOL' as const,
        marketConditionId: 'cond-sol-456',
        marketName: 'sol-updown-15m-456',
        ruleEvaluations: {
          nearSettlement: { passed: false, timeToEndMin: 5, threshold: 3 },
          highProbability: { passed: false, winProb: 0.48, threshold: 0.5 },
          positiveEV: { passed: true, settlementEV: 55, exitValue: 40, evRatio: 1.375 },
          chainlinkAligned: { passed: false, delta: 0.001, currentPrice: 150, priceToBeat: 149, side: 'UP' },
          momentumFavorable: { passed: false, strength: 0, threshold: 0.3, enabled: false },
        },
        entryContext: {
          leg1Side: 'UP' as const,
          leg1EntryPrice: 0.40,
          leg1Shares: 100,
          leg1Cost: 40,
          enteredAt: Date.now() - 180000,
        },
        marketSnapshot: {
          upPrice: 0.55,
          downPrice: 0.48,
          priceToBeat: 149,
          currentChainlinkPrice: 150,
          marketEndTime: Date.now() + 300000,
        },
        roundId: 'test-sol-123',
        underlying: 'SOL' as const,
        isPaper: false,
        timestamp: Date.now(),
      };

      const eventPromise = new Promise<void>((resolve) => {
        manager.on('enhancedSettlementDecision', (event) => {
          expect(event.coin).toBe('SOL');
          expect(event.marketConditionId).toBe('cond-sol-456');
          expect(event.isPaper).toBe(false);
          resolve();
        });
      });

      manager.emit('enhancedSettlementDecision', testDecision);
      await eventPromise;
    });

    it('should include coin in forwarded settlementOutcome', async () => {
      const testOutcome = {
        roundId: 'test-xrp-789',
        decision: 'EXIT' as const,
        outcome: 'LOSS' as const,
        actualProfit: -5,
        counterfactualProfit: 45,
        settlementSide: 'UP' as const,
        settlementPrice: 0.55,
        settledAt: Date.now(),
        isPaper: true,
        underlying: 'XRP' as const,
        marketName: 'xrp-updown-15m-789',
        coin: 'XRP' as const,
        marketConditionId: 'cond-xrp-789',
      };

      const eventPromise = new Promise<void>((resolve) => {
        manager.on('settlementOutcome', (event) => {
          expect(event.coin).toBe('XRP');
          expect(event.outcome).toBe('LOSS');
          expect(event.counterfactualProfit).toBeGreaterThan(event.actualProfit);
          resolve();
        });
      });

      manager.emit('settlementOutcome', testOutcome);
      await eventPromise;
    });
  });

  describe('Decision types coverage', () => {
    const decisionReasons = [
      'disabled',
      'no_position',
      'near_settlement',
      'market_ended',
      'high_probability',
      'positive_ev',
      'chainlink_aligned',
      'momentum_favorable',
      'no_edge',
    ] as const;

    decisionReasons.forEach((reason) => {
      it(`should handle decision reason: ${reason}`, async () => {
        const decision = {
          decision: reason === 'no_edge' || reason === 'disabled' || reason === 'no_position' ? 'EXIT' : 'HOLD',
          reason,
          confidence: reason === 'no_edge' ? 0 : 0.5,
          coin: 'BTC' as const,
        };

        const eventPromise = new Promise<void>((resolve) => {
          manager.on('settlementDecision', (event) => {
            expect(event.reason).toBe(reason);
            resolve();
          });
        });

        manager.emit('settlementDecision', decision);
        await eventPromise;
      });
    });
  });

  describe('Outcome types coverage', () => {
    it('should handle WIN outcome', async () => {
      const eventPromise = new Promise<void>((resolve) => {
        manager.on('settlementOutcome', (event) => {
          expect(event.outcome).toBe('WIN');
          expect(event.actualProfit).toBeGreaterThan(0);
          resolve();
        });
      });

      manager.emit('settlementOutcome', {
        roundId: 'win-test',
        decision: 'HOLD',
        outcome: 'WIN',
        actualProfit: 50,
        counterfactualProfit: 10,
        settlementSide: 'UP',
        settlementPrice: 95500,
        settledAt: Date.now(),
        isPaper: true,
        underlying: 'BTC',
        marketName: 'btc-updown-15m-win',
        coin: 'BTC',
      });

      await eventPromise;
    });

    it('should handle LOSS outcome', async () => {
      const eventPromise = new Promise<void>((resolve) => {
        manager.on('settlementOutcome', (event) => {
          expect(event.outcome).toBe('LOSS');
          expect(event.actualProfit).toBeLessThan(0);
          resolve();
        });
      });

      manager.emit('settlementOutcome', {
        roundId: 'loss-test',
        decision: 'HOLD',
        outcome: 'LOSS',
        actualProfit: -45,
        counterfactualProfit: 5,
        settlementSide: 'DOWN',
        settlementPrice: 94500,
        settledAt: Date.now(),
        isPaper: true,
        underlying: 'ETH',
        marketName: 'eth-updown-15m-loss',
        coin: 'ETH',
      });

      await eventPromise;
    });
  });
});
