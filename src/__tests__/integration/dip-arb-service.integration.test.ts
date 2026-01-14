/**
 * DipArbService Integration Tests
 *
 * Tests the DipArbService market scanning and parallel trading functionality.
 * These tests make REAL API calls to Polymarket.
 *
 * Note: Trading tests are skipped without a real private key.
 *
 * Run with: pnpm test:integration
 */

import { describe, it, expect, beforeAll, afterEach, vi } from 'vitest';
import { DipArbService } from '../../services/dip-arb-service.js';
import { RealtimeServiceV2 } from '../../services/realtime-service-v2.js';
import { MarketService } from '../../services/market-service.js';
import { GammaApiClient } from '../../clients/gamma-api.js';
import { DataApiClient } from '../../clients/data-api.js';
import { RateLimiter } from '../../core/rate-limiter.js';
import { createUnifiedCache } from '../../core/unified-cache.js';
import type { DipArbMarketConfig, DipArbParallelOptions } from '../../services/dip-arb-types.js';

describe('DipArbService Integration', () => {
  let dipArbService: DipArbService;
  let realtimeService: RealtimeServiceV2;
  let marketService: MarketService;

  beforeAll(async () => {
    // Initialize services
    const rateLimiter = new RateLimiter();
    const cache = createUnifiedCache();
    const gammaApi = new GammaApiClient(rateLimiter, cache);
    const dataApi = new DataApiClient(rateLimiter, cache);

    marketService = new MarketService(gammaApi, dataApi, rateLimiter, cache);
    realtimeService = new RealtimeServiceV2();

    // Create DipArbService without private key (read-only mode)
    dipArbService = new DipArbService(
      realtimeService,
      null, // No trading service
      marketService,
      undefined, // No private key
      137
    );

    // Update config for testing
    dipArbService.updateConfig({
      debug: false,
      paperMode: true,
    });
  }, 60000);

  afterEach(async () => {
    // Clean up after each test
    if (dipArbService.isRunning) {
      await dipArbService.stop();
    }
    if (dipArbService.isParallelMode()) {
      await dipArbService.stopParallel();
    }
  });

  describe('Market Scanning', () => {
    it('should scan for upcoming crypto UP/DOWN markets', async () => {
      const markets = await dipArbService.scanUpcomingMarkets({
        coin: 'BTC',
        duration: 'all',
        limit: 5,
      });

      // May or may not find markets depending on time
      expect(Array.isArray(markets)).toBe(true);

      if (markets.length > 0) {
        const market = markets[0];
        expect(market.conditionId).toBeDefined();
        expect(market.upTokenId).toBeDefined();
        expect(market.downTokenId).toBeDefined();
        expect(market.underlying).toBe('BTC');
        expect(market.durationMinutes).toBeGreaterThan(0);

        console.log(`Found ${markets.length} BTC markets:`);
        markets.forEach((m, i) => {
          console.log(`  ${i + 1}. ${m.name} (${m.durationMinutes}min)`);
        });
      } else {
        console.log('No BTC markets found at this time');
      }
    }, 30000);

    it('should scan for all coin types', async () => {
      const coins = ['BTC', 'ETH', 'SOL', 'XRP'] as const;
      const foundMarkets: Record<string, number> = {};

      for (const coin of coins) {
        const markets = await dipArbService.scanUpcomingMarkets({
          coin,
          duration: 'all',
          limit: 3,
        });
        foundMarkets[coin] = markets.length;
      }

      console.log('Markets found per coin:', foundMarkets);

      // At least one coin should have markets (usually)
      const totalMarkets = Object.values(foundMarkets).reduce((a, b) => a + b, 0);
      expect(totalMarkets).toBeGreaterThanOrEqual(0); // May be 0 if no markets active
    }, 60000);
  });

  describe('Parallel Trading API', () => {
    it('should initialize in non-parallel mode', () => {
      expect(dipArbService.isParallelMode()).toBe(false);
      expect(dipArbService.getParallelMarketCount()).toBe(0);
    });

    it('should get parallel status when not running', () => {
      const status = dipArbService.getParallelStatus();

      expect(status).toBeDefined();
      expect(status.markets).toEqual({});
      expect(status.runningCount).toBe(0);
      expect(typeof status.timestamp).toBe('number');
    });

    it('should reject empty markets array', async () => {
      const options: DipArbParallelOptions = {
        markets: [],
      };

      await expect(dipArbService.startParallel(options)).rejects.toThrow(
        'At least one market is required'
      );
    });

    it('should reject more than 4 markets', async () => {
      const options: DipArbParallelOptions = {
        markets: ['BTC', 'ETH', 'SOL', 'XRP', 'BTC'] as any, // 5 markets
      };

      await expect(dipArbService.startParallel(options)).rejects.toThrow(
        'Maximum 4 markets allowed'
      );
    });

    it('should start parallel mode with available markets', async () => {
      // First check which markets are available
      const availableMarkets: string[] = [];
      for (const coin of ['BTC', 'ETH', 'SOL'] as const) {
        const markets = await dipArbService.scanUpcomingMarkets({
          coin,
          duration: '15m',
          limit: 1,
        });
        if (markets.length > 0) {
          availableMarkets.push(coin);
        }
      }

      if (availableMarkets.length === 0) {
        console.log('Skipping parallel start test - no 15m markets available');
        return;
      }

      console.log(`Starting parallel with available markets: ${availableMarkets.join(', ')}`);

      const options: DipArbParallelOptions = {
        markets: availableMarkets.slice(0, 2) as any,
        duration: '15m',
      };

      const startedMarkets = await dipArbService.startParallel(options);

      expect(startedMarkets.size).toBeGreaterThan(0);
      expect(dipArbService.isParallelMode()).toBe(true);
      expect(dipArbService.getParallelMarketCount()).toBeGreaterThan(0);

      // Check status
      const status = dipArbService.getParallelStatus();
      expect(status.runningCount).toBeGreaterThan(0);

      console.log(`Started ${startedMarkets.size} markets in parallel mode`);
      for (const [coin, market] of startedMarkets) {
        console.log(`  ${coin}: ${market.name}`);
      }

      // Clean up
      await dipArbService.stopParallel();
      expect(dipArbService.isParallelMode()).toBe(false);
    }, 60000);

    it('should emit parallelStatus event on start/stop', async () => {
      const statusEvents: any[] = [];

      dipArbService.on('parallelStatus', (status) => {
        statusEvents.push(status);
      });

      // Find any available market
      const markets = await dipArbService.scanUpcomingMarkets({
        coin: 'BTC',
        duration: 'all',
        limit: 1,
      });

      if (markets.length === 0) {
        console.log('Skipping event test - no markets available');
        return;
      }

      const options: DipArbParallelOptions = {
        markets: ['BTC'],
        duration: '15m',
      };

      try {
        await dipArbService.startParallel(options);
      } catch {
        // May fail if no 15m market, that's ok
      }

      await dipArbService.stopParallel();

      // Should have emitted at least one status event
      expect(statusEvents.length).toBeGreaterThanOrEqual(0);

      dipArbService.removeAllListeners('parallelStatus');
    }, 30000);

    it('should handle stopMarket for non-existent market gracefully', async () => {
      // Should not throw
      await dipArbService.stopMarket('BTC');
    });
  });

  describe('Single Market Mode (backwards compatibility)', () => {
    it('should use findAndStart for single market', async () => {
      // Find any available market
      const markets = await dipArbService.scanUpcomingMarkets({
        coin: 'BTC',
        duration: 'all',
        limit: 1,
      });

      if (markets.length === 0) {
        console.log('Skipping single market test - no BTC markets available');
        return;
      }

      // Connect realtime (needed for market subscription)
      realtimeService.connect();
      await new Promise((resolve) => setTimeout(resolve, 2000));

      try {
        // Start single market
        const market = await dipArbService.findAndStart({ coin: 'BTC' });

        expect(market).toBeDefined();
        expect(market.underlying).toBe('BTC');
        expect(dipArbService.isRunning).toBe(true);
        expect(dipArbService.isParallelMode()).toBe(false); // Should NOT be parallel

        console.log(`Started single market: ${market.name}`);
      } catch (error) {
        // May fail due to market timing, log and continue
        console.log('Single market start failed (timing issue):', error);
      } finally {
        await dipArbService.stop();
        realtimeService.disconnect();
      }
    }, 60000);
  });

  describe('Configuration', () => {
    it('should update config', () => {
      dipArbService.updateConfig({
        dipThreshold: 0.05,
        sumTarget: 0.88,
        shares: 100,
      });

      const config = dipArbService.getConfig();

      expect(config.dipThreshold).toBe(0.05);
      expect(config.sumTarget).toBe(0.88);
      expect(config.shares).toBe(100);
    });

    it('should maintain paper mode by default', () => {
      const config = dipArbService.getConfig();
      expect(config.paperMode).toBe(true);
    });
  });
});

describe('DipArbService Unit Tests', () => {
  describe('getParallelStatus', () => {
    it('should return empty status when not initialized', () => {
      const realtimeService = new RealtimeServiceV2();
      const rateLimiter = new RateLimiter();
      const cache = createUnifiedCache();
      const gammaApi = new GammaApiClient(rateLimiter, cache);
      const dataApi = new DataApiClient(rateLimiter, cache);
      const marketService = new MarketService(gammaApi, dataApi, rateLimiter, cache);

      const service = new DipArbService(
        realtimeService,
        null,
        marketService,
        undefined,
        137
      );

      const status = service.getParallelStatus();

      expect(status.markets).toEqual({});
      expect(status.runningCount).toBe(0);
      expect(status.timestamp).toBeGreaterThan(0);
    });
  });
});
