/**
 * TradingService Unit Tests
 *
 * Tests validation logic in TradingService without requiring network access.
 */

import { describe, it, expect, beforeAll, beforeEach, vi, afterEach } from 'vitest';
import { TradingService, MIN_ORDER_VALUE_USDC, MIN_ORDER_SIZE_SHARES } from './trading-service.js';
import { RateLimiter } from '../core/rate-limiter.js';
import { createUnifiedCache } from '../core/unified-cache.js';

describe('TradingService Validation', () => {
  let service: TradingService;
  let getTickSizeSpy: ReturnType<typeof vi.spyOn>;
  let isNegRiskSpy: ReturnType<typeof vi.spyOn>;

  beforeAll(async () => {
    // Create service with a dummy private key - won't actually trade
    service = new TradingService(new RateLimiter(), createUnifiedCache(), {
      privateKey: '0x' + '1'.repeat(64), // Dummy key
    });
  });

  beforeEach(() => {
    // Mock network-dependent methods to avoid timeouts
    // These tests focus on validation logic, not network behavior
    getTickSizeSpy = vi.spyOn(service, 'getTickSize').mockResolvedValue(0.01);
    isNegRiskSpy = vi.spyOn(service, 'isNegRisk').mockResolvedValue(false);

    // Mock ensureInitialized to return a mock client that throws on actual operations
    // This ensures tests fail fast instead of timing out
    vi.spyOn(service as unknown as { ensureInitialized(): Promise<unknown> }, 'ensureInitialized')
      .mockRejectedValue(new Error('Test: Network not available'));
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('createMarketOrder validation', () => {
    it('should reject orders below minimum value', async () => {
      const result = await service.createMarketOrder({
        tokenId: 'test-token',
        side: 'BUY',
        amount: 0.5, // Below $1 minimum
        price: 0.50,
      });

      expect(result.success).toBe(false);
      expect(result.errorMsg).toContain('below Polymarket minimum');
      expect(result.errorMsg).toContain(`$${MIN_ORDER_VALUE_USDC}`);
    });

    it('should reject orders that result in fewer than 5 shares', async () => {
      // $1.00 at $0.50 = 2 shares (passes $1 min value, fails 5 share min)
      const result = await service.createMarketOrder({
        tokenId: 'test-token',
        side: 'BUY',
        amount: 1.0,
        price: 0.50,
      });

      expect(result.success).toBe(false);
      expect(result.errorMsg).toContain('2.0 shares');
      expect(result.errorMsg).toContain(`${MIN_ORDER_SIZE_SHARES} shares`);
    });

    it('should reject order for task test case: $0.40 at $0.10', async () => {
      // Task verification case: $0.40 at $0.10 = 4 shares
      // Note: This fails value validation first ($0.40 < $1 minimum)
      const result = await service.createMarketOrder({
        tokenId: 'test-token',
        side: 'BUY',
        amount: 0.40,
        price: 0.10,
      });

      expect(result.success).toBe(false);
      // Fails value check first
      expect(result.errorMsg).toContain('below Polymarket minimum');
    });

    it('should reject orders just under the share minimum', async () => {
      // $2 at $0.50 = 4 shares (just under minimum)
      const result = await service.createMarketOrder({
        tokenId: 'test-token',
        side: 'BUY',
        amount: 2.0,
        price: 0.50,
      });

      expect(result.success).toBe(false);
      expect(result.errorMsg).toContain('4.0 shares');
    });

    it('should accept orders at exactly 5 shares', async () => {
      // $2.50 at $0.50 = 5 shares (exactly at minimum)
      // The validation should pass (not return share validation error)
      // The order may fail for other reasons (network) but not share validation
      try {
        const result = await service.createMarketOrder({
          tokenId: 'test-token',
          side: 'BUY',
          amount: 2.50,
          price: 0.50,
        });
        // If we get a result, verify no share validation error
        if (!result.success && result.errorMsg) {
          expect(result.errorMsg).not.toContain('shares, below Polymarket minimum');
        }
      } catch (error) {
        // If it throws, verify the error is not a share validation error
        const errMsg = error instanceof Error ? error.message : String(error);
        expect(errMsg).not.toContain('shares, below Polymarket minimum');
      }
    });

    it('should accept orders above 5 shares', async () => {
      // $5 at $0.50 = 10 shares (well above minimum)
      try {
        const result = await service.createMarketOrder({
          tokenId: 'test-token',
          side: 'BUY',
          amount: 5.0,
          price: 0.50,
        });
        if (!result.success && result.errorMsg) {
          expect(result.errorMsg).not.toContain('shares, below Polymarket minimum');
        }
      } catch (error) {
        const errMsg = error instanceof Error ? error.message : String(error);
        expect(errMsg).not.toContain('shares, below Polymarket minimum');
      }
    });

    it('should skip share validation when price is not provided', async () => {
      // Market orders without explicit price can't be pre-validated for shares
      try {
        const result = await service.createMarketOrder({
          tokenId: 'test-token',
          side: 'BUY',
          amount: 1.0, // Valid amount, no price
        });
        if (!result.success && result.errorMsg) {
          expect(result.errorMsg).not.toContain('shares, below Polymarket minimum');
        }
      } catch (error) {
        const errMsg = error instanceof Error ? error.message : String(error);
        expect(errMsg).not.toContain('shares, below Polymarket minimum');
      }
    });

    it('should skip share validation when price is zero', async () => {
      try {
        const result = await service.createMarketOrder({
          tokenId: 'test-token',
          side: 'BUY',
          amount: 1.0,
          price: 0, // Zero price - can't calculate shares
        });
        if (!result.success && result.errorMsg) {
          expect(result.errorMsg).not.toContain('shares, below Polymarket minimum');
        }
      } catch (error) {
        const errMsg = error instanceof Error ? error.message : String(error);
        expect(errMsg).not.toContain('shares, below Polymarket minimum');
      }
    });
  });

  describe('constants', () => {
    it('should export minimum order value constant', () => {
      expect(MIN_ORDER_VALUE_USDC).toBe(1);
    });

    it('should export minimum order size constant', () => {
      expect(MIN_ORDER_SIZE_SHARES).toBe(5);
    });
  });
});
