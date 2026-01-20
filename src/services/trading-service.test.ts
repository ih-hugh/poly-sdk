/**
 * TradingService Unit Tests
 *
 * Tests validation logic in TradingService without requiring network access.
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { TradingService, MIN_ORDER_VALUE_USDC, MIN_ORDER_SIZE_SHARES } from './trading-service.js';
import { RateLimiter } from '../core/rate-limiter.js';
import { createUnifiedCache } from '../core/unified-cache.js';

describe('TradingService Validation', () => {
  let service: TradingService;

  beforeAll(async () => {
    // Create service with a dummy private key - won't actually trade
    service = new TradingService(new RateLimiter(), createUnifiedCache(), {
      privateKey: '0x' + '1'.repeat(64), // Dummy key
    });
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
      // Note: This will still fail due to service not being initialized,
      // but it should NOT fail the share validation
      const result = await service.createMarketOrder({
        tokenId: 'test-token',
        side: 'BUY',
        amount: 2.50,
        price: 0.50,
      });

      // Should not contain the share validation error
      if (!result.success) {
        expect(result.errorMsg).not.toContain('shares, below Polymarket minimum');
      }
    });

    it('should accept orders above 5 shares', async () => {
      // $5 at $0.50 = 10 shares (well above minimum)
      const result = await service.createMarketOrder({
        tokenId: 'test-token',
        side: 'BUY',
        amount: 5.0,
        price: 0.50,
      });

      // Should not contain the share validation error
      if (!result.success) {
        expect(result.errorMsg).not.toContain('shares, below Polymarket minimum');
      }
    });

    it('should skip share validation when price is not provided', async () => {
      // Market orders without explicit price can't be pre-validated for shares
      const result = await service.createMarketOrder({
        tokenId: 'test-token',
        side: 'BUY',
        amount: 1.0, // Valid amount, no price
      });

      // Should not contain the share validation error (may fail for other reasons)
      if (!result.success) {
        expect(result.errorMsg).not.toContain('shares, below Polymarket minimum');
      }
    });

    it('should skip share validation when price is zero', async () => {
      const result = await service.createMarketOrder({
        tokenId: 'test-token',
        side: 'BUY',
        amount: 1.0,
        price: 0, // Zero price - can't calculate shares
      });

      // Should not contain the share validation error
      if (!result.success) {
        expect(result.errorMsg).not.toContain('shares, below Polymarket minimum');
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
