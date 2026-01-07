import { TradingService, RateLimiter, createUnifiedCache } from './dist/index.js';
import { config } from 'dotenv';
config({ path: '../.env' });

const pk = process.env.POLY_PRIVKEY;
console.log('Using wallet:', pk ? pk.slice(0,6) + '...' : 'none');

const rl = new RateLimiter();
const cache = createUnifiedCache();
const t = new TradingService(rl, cache, { privateKey: pk });
await t.initialize();

console.log('Selling 6 Down @ 0.85...');
try {
  const r1 = await t.placeOrder({ tokenId: '110083725719357051319612248159302825704980332180179347898947550019023847291360', side: 'SELL', price: 0.85, size: 6, orderType: 'FOK' });
  console.log('Down:', r1.status);
} catch(e) { console.log('Down err:', e.message); }

console.log('Selling 6 Up @ 0.09...');
try {
  const r2 = await t.placeOrder({ tokenId: '50318096510989379471642196522417738596771229353769398494442652414102097488166', side: 'SELL', price: 0.09, size: 6, orderType: 'FOK' });
  console.log('Up:', r2.status);
} catch(e) { console.log('Up err:', e.message); }
