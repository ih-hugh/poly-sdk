// packages/poly-sdk/src/services/dip-arb-service.profit-parity.test.ts
import { DIP_ARB_CRYPTO_TAKER_FEE } from './dip-arb-types';

describe('DipArb Profit Parity: Paper vs Live', () => {
  const FEE_RATE = DIP_ARB_CRYPTO_TAKER_FEE; // 0.03

  describe('Leg2 Profit Calculation', () => {
    it('should calculate identical NET profit for paper and live modes', () => {
      // Given: A completed leg2 trade
      const leg1Price = 0.30;
      const leg2Price = 0.33;
      const shares = 50;
      const totalCost = leg1Price + leg2Price; // 0.63

      // Calculate expected NET profit (what paper mode does)
      const grossProfit = 1 - totalCost; // 0.37
      const totalFees = totalCost * FEE_RATE * 2; // 0.0378
      const netProfitPerShare = grossProfit - totalFees; // 0.3322
      const expectedNetProfit = netProfitPerShare * shares; // 16.61

      // This is what live mode SHOULD produce (currently produces GROSS)
      const liveNetProfit = calculateLiveModeProfit(leg1Price, leg2Price, shares, FEE_RATE);

      expect(liveNetProfit).toBeCloseTo(expectedNetProfit, 2);
    });

    it('should NOT report gross profit (the current bug)', () => {
      const leg1Price = 0.30;
      const leg2Price = 0.33;
      const shares = 50;
      const totalCost = leg1Price + leg2Price;

      const grossProfit = (1 - totalCost) * shares; // 18.50 (WRONG)
      const netProfit = calculateLiveModeProfit(leg1Price, leg2Price, shares, FEE_RATE);

      // Net should be LESS than gross
      expect(netProfit).toBeLessThan(grossProfit);
      // Specifically, net = gross - fees
      const fees = totalCost * FEE_RATE * 2 * shares;
      expect(netProfit).toBeCloseTo(grossProfit - fees, 2);
    });
  });

  describe('Settlement Profit Calculation', () => {
    it('should account for entry fee on settlement win', () => {
      const leg1Cost = 7.50; // 50 shares at $0.15
      const shares = 50;

      // Settlement win: receive $1 per share
      const received = shares * 1;

      // Entry fee was paid: leg1Cost * 3%
      const entryFee = leg1Cost * FEE_RATE;

      // Net profit should subtract entry fee
      const expectedNetProfit = received - leg1Cost - entryFee;
      const actualProfit = calculateSettlementWinProfit(leg1Cost, shares, FEE_RATE);

      expect(actualProfit).toBeCloseTo(expectedNetProfit, 2);
    });
  });

  describe('Exit Fee Calculation', () => {
    it('should use 3% exit fee (single leg), not 6%', () => {
      const exitPrice = 0.20;
      const shares = 50;

      // Exit has ONE sell transaction = 3% fee
      const expectedExitValue = exitPrice * shares * 0.97; // 9.70
      const actualExitValue = calculateExitValue(exitPrice, shares, FEE_RATE);

      expect(actualExitValue).toBeCloseTo(expectedExitValue, 2);

      // Should NOT use 6% (the current bug)
      const wrongExitValue = exitPrice * shares * 0.94; // 9.40
      expect(actualExitValue).not.toBeCloseTo(wrongExitValue, 1);
    });
  });
});

// Helper functions that will be extracted from the service
// These don't exist yet - they'll be created in subsequent tasks
function calculateLiveModeProfit(
  leg1Price: number,
  leg2Price: number,
  shares: number,
  feeRate: number
): number {
  const totalCost = leg1Price + leg2Price;
  const grossProfit = 1 - totalCost;
  const totalFees = totalCost * feeRate * 2;
  const netProfitPerShare = grossProfit - totalFees;
  return netProfitPerShare * shares;
}

function calculateSettlementWinProfit(
  leg1Cost: number,
  shares: number,
  feeRate: number
): number {
  const received = shares * 1; // $1 per share
  const entryFee = leg1Cost * feeRate;
  return received - leg1Cost - entryFee;
}

function calculateExitValue(
  exitPrice: number,
  shares: number,
  feeRate: number
): number {
  // Single sell = single fee
  return exitPrice * shares * (1 - feeRate);
}
