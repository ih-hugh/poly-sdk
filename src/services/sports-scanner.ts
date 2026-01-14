/**
 * Sports Market Scanner
 *
 * Discovers and categorizes sports markets on Polymarket for arbitrage opportunities.
 *
 * Supported leagues:
 * - NBA (basketball)
 * - NFL (American football)
 * - NHL (hockey)
 * - MLB (baseball)
 * - Soccer/Football (international)
 * - UFC/MMA (combat sports)
 * - Tennis
 * - Golf
 *
 * Market types:
 * - Game outcomes (Team1 vs Team2)
 * - Spreads/Handicaps
 * - Over/Under (totals)
 * - Props (player stats, etc.)
 * - Championship/Tournament winners (NegRisk multi-outcome)
 */

import { GammaApiClient, type GammaMarket, type GammaEvent } from '../clients/gamma-api.js';
import { RateLimiter } from '../core/rate-limiter.js';
import type { UnifiedCache } from '../core/unified-cache.js';
import { createUnifiedCache } from '../core/unified-cache.js';

// ===== Types =====

/**
 * Supported sports leagues
 */
export type SportsLeague =
  | 'NBA'
  | 'NFL'
  | 'NHL'
  | 'MLB'
  | 'SOCCER'
  | 'UFC'
  | 'TENNIS'
  | 'GOLF'
  | 'OTHER';

/**
 * Sports market type
 */
export type SportsMarketType =
  | 'game_outcome'    // Team A vs Team B
  | 'spread'          // Team +/- points
  | 'over_under'      // Total points
  | 'prop'            // Player/game props
  | 'championship'    // Season/tournament winner (multi-outcome)
  | 'other';

/**
 * Parsed sports market with structured metadata
 */
export interface SportMarket {
  /** Original Gamma market data */
  raw: GammaMarket;
  /** Detected league */
  league: SportsLeague;
  /** Market type */
  marketType: SportsMarketType;
  /** Whether this is a NegRisk multi-outcome market */
  isNegRisk: boolean;
  /** Parsed team names (if applicable) */
  teams?: [string, string];
  /** Game date (if parseable from slug) */
  gameDate?: Date;
  /** Spread value (if spread market) */
  spreadValue?: number;
  /** Over/under line (if totals market) */
  totalLine?: number;
  /** Related event ID (for multi-outcome grouping) */
  eventId?: string;
  /** Event slug (for grouping related markets) */
  eventSlug?: string;
}

/**
 * NegRisk event with multiple outcome markets
 */
export interface NegRiskSportsEvent {
  /** Event metadata */
  event: GammaEvent;
  /** All markets in this event */
  markets: SportMarket[];
  /** Sum of all YES prices (should be ~1.0) */
  yesPriceSum: number;
  /** Arbitrage opportunity: 'long' if sum < 1, 'short' if sum > 1, 'none' otherwise */
  arbOpportunity: 'long' | 'short' | 'none';
  /** Profit rate (absolute deviation from 1.0) */
  profitRate: number;
  /** Total liquidity across all markets */
  totalLiquidity: number;
}

/**
 * Scan options for sports markets
 */
export interface SportsScanOptions {
  /** Filter by league(s) */
  leagues?: SportsLeague[];
  /** Filter by market type(s) */
  marketTypes?: SportsMarketType[];
  /** Include NegRisk multi-outcome markets */
  includeNegRisk?: boolean;
  /** Minimum 24h volume (USDC) */
  minVolume24h?: number;
  /** Minimum liquidity (USDC) */
  minLiquidity?: number;
  /** Only active markets */
  activeOnly?: boolean;
  /** Maximum number of results */
  limit?: number;
  /** Minimum minutes until market ends */
  minMinutesUntilEnd?: number;
}

/**
 * Scan result with arbitrage analysis
 */
export interface SportsScanResult {
  /** Binary sports markets (Team1/Team2, spreads, etc.) */
  binaryMarkets: SportMarket[];
  /** NegRisk events with multi-outcome arbitrage potential */
  negRiskEvents: NegRiskSportsEvent[];
  /** Total markets scanned */
  totalScanned: number;
  /** Scan timestamp */
  timestamp: number;
}

// ===== Constants =====

/**
 * League detection keywords
 */
const LEAGUE_KEYWORDS: Record<SportsLeague, string[]> = {
  NBA: ['nba', 'basketball', 'lakers', 'celtics', 'warriors', 'nets', 'knicks', 'heat', 'bulls', 'suns', 'mavs', 'mavericks', 'bucks', 'sixers', '76ers', 'nuggets', 'clippers', 'thunder', 'grizzlies', 'pelicans', 'spurs', 'rockets', 'timberwolves', 'jazz', 'kings', 'blazers', 'pacers', 'hawks', 'hornets', 'wizards', 'pistons', 'cavaliers', 'magic', 'raptors'],
  NFL: ['nfl', 'football', 'super bowl', 'superbowl', 'chiefs', 'eagles', '49ers', 'cowboys', 'bills', 'dolphins', 'jets', 'patriots', 'ravens', 'steelers', 'bengals', 'browns', 'texans', 'colts', 'jaguars', 'titans', 'broncos', 'chargers', 'raiders', 'commanders', 'giants', 'bears', 'lions', 'packers', 'vikings', 'falcons', 'panthers', 'saints', 'buccaneers', 'cardinals', 'rams', 'seahawks'],
  NHL: ['nhl', 'hockey', 'stanley cup', 'bruins', 'rangers', 'maple leafs', 'canadiens', 'blackhawks', 'penguins', 'flyers', 'capitals', 'lightning', 'panthers', 'hurricanes', 'devils', 'islanders', 'blue jackets', 'red wings', 'sabres', 'senators', 'predators', 'jets', 'wild', 'avalanche', 'stars', 'blues', 'coyotes', 'flames', 'oilers', 'canucks', 'golden knights', 'kraken', 'ducks', 'kings', 'sharks'],
  MLB: ['mlb', 'baseball', 'world series', 'yankees', 'red sox', 'dodgers', 'mets', 'cubs', 'cardinals', 'braves', 'astros', 'phillies', 'padres', 'mariners', 'rangers', 'orioles', 'twins', 'guardians', 'white sox', 'royals', 'tigers', 'angels', 'athletics', 'rays', 'blue jays', 'marlins', 'nationals', 'brewers', 'reds', 'pirates', 'rockies', 'diamondbacks', 'giants'],
  SOCCER: ['soccer', 'football', 'premier league', 'la liga', 'bundesliga', 'serie a', 'ligue 1', 'champions league', 'world cup', 'euro', 'mls', 'manchester united', 'manchester city', 'liverpool', 'chelsea', 'arsenal', 'tottenham', 'barcelona', 'real madrid', 'bayern', 'psg', 'juventus', 'inter milan', 'ac milan'],
  UFC: ['ufc', 'mma', 'mixed martial arts', 'bellator', 'pfl', 'boxing', 'fight', 'bout'],
  TENNIS: ['tennis', 'wimbledon', 'us open', 'french open', 'australian open', 'atp', 'wta', 'grand slam'],
  GOLF: ['golf', 'pga', 'masters', 'us open golf', 'british open', 'ryder cup'],
  OTHER: [],
};

/**
 * Market type detection patterns
 */
const MARKET_TYPE_PATTERNS: Array<{ type: SportsMarketType; patterns: RegExp[] }> = [
  {
    type: 'spread',
    patterns: [
      /spread/i,
      /handicap/i,
      /[+-]\d+\.?\d*\s*pt/i,
      /\+\d+\.?\d*/,
      /-\d+\.?\d*/,
      /cover/i,
    ],
  },
  {
    type: 'over_under',
    patterns: [
      /over\s*\/?\s*under/i,
      /total\s+points/i,
      /o\/u/i,
      /over\s+\d+/i,
      /under\s+\d+/i,
    ],
  },
  {
    type: 'championship',
    patterns: [
      /champion/i,
      /win\s+(the\s+)?(nba|nfl|nhl|mlb|world\s+series|super\s+bowl|stanley\s+cup)/i,
      /winner/i,
      /playoff/i,
      /finals/i,
      /title/i,
    ],
  },
  {
    type: 'prop',
    patterns: [
      /prop/i,
      /mvp/i,
      /score\s+first/i,
      /\d+\s*\+?\s*(points|rebounds|assists|goals|touchdowns|yards)/i,
      /player/i,
    ],
  },
  {
    type: 'game_outcome',
    patterns: [
      /will\s+.+\s+(beat|defeat|win)/i,
      /vs\.?/i,
      /versus/i,
      /moneyline/i,
    ],
  },
];

// ===== Sports Market Scanner =====

export class SportsMarketScanner {
  private gammaApi: GammaApiClient;
  private rateLimiter: RateLimiter;
  private cache: UnifiedCache;

  constructor(
    gammaApi?: GammaApiClient,
    rateLimiter?: RateLimiter,
    cache?: UnifiedCache
  ) {
    this.rateLimiter = rateLimiter || new RateLimiter();
    this.cache = cache || createUnifiedCache();
    this.gammaApi = gammaApi || new GammaApiClient(this.rateLimiter, this.cache);
  }

  // ===== Public API =====

  /**
   * Scan for sports markets with optional filters
   *
   * @example
   * ```typescript
   * const scanner = new SportsMarketScanner();
   *
   * // Find all NBA and NFL markets
   * const result = await scanner.scan({
   *   leagues: ['NBA', 'NFL'],
   *   minVolume24h: 1000,
   *   activeOnly: true,
   * });
   *
   * console.log(`Found ${result.binaryMarkets.length} binary markets`);
   * console.log(`Found ${result.negRiskEvents.length} NegRisk events`);
   * ```
   */
  async scan(options: SportsScanOptions = {}): Promise<SportsScanResult> {
    const {
      leagues,
      marketTypes,
      includeNegRisk = true,
      minVolume24h = 0,
      minLiquidity = 0,
      activeOnly = true,
      limit = 200,
      minMinutesUntilEnd = 0,
    } = options;

    // Fetch markets from Gamma API
    const allMarkets = await this.gammaApi.getMarkets({
      active: activeOnly ? true : undefined,
      closed: false,
      limit,
      order: 'volume24hr',
      ascending: false,
    });

    const now = Date.now();
    const minEndTime = now + minMinutesUntilEnd * 60 * 1000;

    // Parse and filter markets
    const sportsMarkets: SportMarket[] = [];

    for (const market of allMarkets) {
      // Check if it's a sports market
      const parsed = this.parseMarket(market);
      if (parsed.league === 'OTHER' && !this.isSportsMarket(market)) {
        continue; // Skip non-sports markets
      }

      // Apply filters
      if (leagues && leagues.length > 0 && !leagues.includes(parsed.league)) {
        continue;
      }

      if (marketTypes && marketTypes.length > 0 && !marketTypes.includes(parsed.marketType)) {
        continue;
      }

      if ((market.volume24hr || 0) < minVolume24h) {
        continue;
      }

      if (market.liquidity < minLiquidity) {
        continue;
      }

      if (market.endDate.getTime() < minEndTime) {
        continue;
      }

      sportsMarkets.push(parsed);
    }

    // Separate binary and NegRisk markets
    const binaryMarkets = sportsMarkets.filter((m) => !m.isNegRisk);
    let negRiskEvents: NegRiskSportsEvent[] = [];

    if (includeNegRisk) {
      negRiskEvents = await this.scanNegRiskEvents(options);
    }

    return {
      binaryMarkets,
      negRiskEvents,
      totalScanned: allMarkets.length,
      timestamp: now,
    };
  }

  /**
   * Scan for NegRisk multi-outcome events (championships, tournaments)
   */
  async scanNegRiskEvents(options: SportsScanOptions = {}): Promise<NegRiskSportsEvent[]> {
    const { leagues, minLiquidity = 0, limit = 50 } = options;

    // Fetch events from Gamma API
    const events = await this.gammaApi.getEvents({
      active: true,
      limit,
    });

    const negRiskEvents: NegRiskSportsEvent[] = [];

    for (const event of events) {
      // Skip events without multiple markets
      if (!event.markets || event.markets.length < 2) {
        continue;
      }

      // Check if it's a sports-related event
      const eventText = `${event.title} ${event.description || ''}`.toLowerCase();
      const detectedLeague = this.detectLeague(eventText);

      if (detectedLeague === 'OTHER') {
        // Check if any market suggests sports
        const hasSportsMarket = event.markets.some((m) => {
          const text = `${m.question} ${m.description || ''}`.toLowerCase();
          return this.detectLeague(text) !== 'OTHER';
        });
        if (!hasSportsMarket) continue;
      }

      if (leagues && leagues.length > 0 && !leagues.includes(detectedLeague)) {
        continue;
      }

      // Parse markets
      const parsedMarkets = event.markets.map((m) => this.parseMarket(m));

      // Calculate YES price sum for arbitrage detection
      const yesPriceSum = event.markets.reduce((sum, m) => {
        const yesPrice = m.outcomePrices?.[0] || 0.5;
        return sum + yesPrice;
      }, 0);

      // Calculate total liquidity
      const totalLiquidity = event.markets.reduce((sum, m) => sum + m.liquidity, 0);

      if (totalLiquidity < minLiquidity) {
        continue;
      }

      // Determine arbitrage opportunity
      let arbOpportunity: 'long' | 'short' | 'none' = 'none';
      const deviation = yesPriceSum - 1;

      if (yesPriceSum < 0.99) {
        arbOpportunity = 'long';
      } else if (yesPriceSum > 1.01) {
        arbOpportunity = 'short';
      }

      negRiskEvents.push({
        event,
        markets: parsedMarkets.map((m) => ({ ...m, isNegRisk: true, eventId: event.id })),
        yesPriceSum,
        arbOpportunity,
        profitRate: Math.abs(deviation),
        totalLiquidity,
      });
    }

    // Sort by profit potential
    negRiskEvents.sort((a, b) => b.profitRate - a.profitRate);

    return negRiskEvents;
  }

  /**
   * Get markets for a specific league
   */
  async getLeagueMarkets(league: SportsLeague, options: Omit<SportsScanOptions, 'leagues'> = {}): Promise<SportMarket[]> {
    const result = await this.scan({ ...options, leagues: [league] });
    return result.binaryMarkets;
  }

  /**
   * Find arbitrage opportunities in binary sports markets
   *
   * Looks for markets where effective buy prices sum to less than $1 (long arb)
   * or effective sell prices sum to more than $1 (short arb)
   */
  async findBinaryArbitrageOpportunities(options: SportsScanOptions = {}): Promise<SportMarket[]> {
    const result = await this.scan(options);

    // Filter to markets with potential mispricing
    // A binary market has arb if YES + NO prices don't sum to ~1.0
    return result.binaryMarkets.filter((market) => {
      const prices = market.raw.outcomePrices;
      if (!prices || prices.length < 2) return false;

      const sum = prices[0] + prices[1];
      // Significant deviation from 1.0 indicates potential arb
      return Math.abs(sum - 1) > 0.005;
    });
  }

  /**
   * Find NegRisk arbitrage opportunities
   */
  async findNegRiskArbitrageOpportunities(
    minProfitRate = 0.005
  ): Promise<NegRiskSportsEvent[]> {
    const events = await this.scanNegRiskEvents();

    return events.filter(
      (e) => e.arbOpportunity !== 'none' && e.profitRate >= minProfitRate
    );
  }

  // ===== Parsing Helpers =====

  /**
   * Parse a Gamma market into structured SportMarket
   */
  parseMarket(market: GammaMarket): SportMarket {
    const text = `${market.question} ${market.description || ''} ${market.slug}`.toLowerCase();

    const league = this.detectLeague(text);
    const marketType = this.detectMarketType(text);
    const isNegRisk = this.isChampionshipMarket(text);

    // Parse teams from slug or question
    const teams = this.parseTeams(market);

    // Parse game date from slug
    const gameDate = this.parseDateFromSlug(market.slug);

    // Parse spread value if applicable
    const spreadValue = marketType === 'spread' ? this.parseSpreadValue(text) : undefined;

    // Parse total line if applicable
    const totalLine = marketType === 'over_under' ? this.parseTotalLine(text) : undefined;

    // Extract event slug from market slug
    const eventSlug = this.extractEventSlug(market.slug);

    return {
      raw: market,
      league,
      marketType,
      isNegRisk,
      teams,
      gameDate,
      spreadValue,
      totalLine,
      eventSlug,
    };
  }

  /**
   * Detect league from text
   */
  private detectLeague(text: string): SportsLeague {
    const lowerText = text.toLowerCase();

    for (const [league, keywords] of Object.entries(LEAGUE_KEYWORDS)) {
      if (league === 'OTHER') continue;

      for (const keyword of keywords) {
        if (lowerText.includes(keyword)) {
          return league as SportsLeague;
        }
      }
    }

    return 'OTHER';
  }

  /**
   * Detect market type from text
   */
  private detectMarketType(text: string): SportsMarketType {
    for (const { type, patterns } of MARKET_TYPE_PATTERNS) {
      for (const pattern of patterns) {
        if (pattern.test(text)) {
          return type;
        }
      }
    }

    return 'other';
  }

  /**
   * Check if this is a sports market
   */
  private isSportsMarket(market: GammaMarket): boolean {
    const text = `${market.question} ${market.description || ''} ${market.slug}`.toLowerCase();

    // Check for any sports-related keywords
    for (const [league, keywords] of Object.entries(LEAGUE_KEYWORDS)) {
      if (league === 'OTHER') continue;
      for (const keyword of keywords) {
        if (text.includes(keyword)) {
          return true;
        }
      }
    }

    // Check tags
    if (market.tags) {
      const sportsTags = ['sports', 'nba', 'nfl', 'nhl', 'mlb', 'soccer', 'ufc', 'tennis', 'golf'];
      if (market.tags.some((tag) => sportsTags.includes(tag.toLowerCase()))) {
        return true;
      }
    }

    return false;
  }

  /**
   * Check if this is a championship/tournament market
   */
  private isChampionshipMarket(text: string): boolean {
    const championshipPatterns = [
      /champion/i,
      /winner/i,
      /win\s+the/i,
      /playoff/i,
      /finals/i,
      /super\s*bowl/i,
      /world\s*series/i,
      /stanley\s*cup/i,
    ];

    return championshipPatterns.some((pattern) => pattern.test(text));
  }

  /**
   * Parse teams from market data
   */
  private parseTeams(market: GammaMarket): [string, string] | undefined {
    // Try to extract from outcomes
    if (market.outcomes && market.outcomes.length === 2) {
      const [team1, team2] = market.outcomes;
      // Skip if outcomes are just Yes/No
      if (team1.toLowerCase() !== 'yes' && team2.toLowerCase() !== 'no') {
        return [team1, team2];
      }
    }

    // Try to parse from slug (e.g., nba-lal-bos-2025-01-15)
    const slugMatch = market.slug.match(
      /(?:nba|nfl|nhl|mlb)-([a-z]{2,4})-([a-z]{2,4})-\d{4}-\d{2}-\d{2}/i
    );
    if (slugMatch) {
      return [slugMatch[1].toUpperCase(), slugMatch[2].toUpperCase()];
    }

    // Try to parse from question
    const vsMatch = market.question.match(/(.+?)\s+vs\.?\s+(.+?)(?:\?|$)/i);
    if (vsMatch) {
      return [vsMatch[1].trim(), vsMatch[2].trim()];
    }

    return undefined;
  }

  /**
   * Parse date from slug
   */
  private parseDateFromSlug(slug: string): Date | undefined {
    // Match patterns like: 2025-01-15 or 2025-1-15
    const dateMatch = slug.match(/(\d{4})-(\d{1,2})-(\d{1,2})/);
    if (dateMatch) {
      const [, year, month, day] = dateMatch;
      return new Date(parseInt(year), parseInt(month) - 1, parseInt(day));
    }

    // Match Unix timestamp patterns
    const timestampMatch = slug.match(/-(\d{10})$/);
    if (timestampMatch) {
      return new Date(parseInt(timestampMatch[1]) * 1000);
    }

    return undefined;
  }

  /**
   * Parse spread value from text
   */
  private parseSpreadValue(text: string): number | undefined {
    const spreadMatch = text.match(/([+-]?\d+\.?\d*)\s*(?:pt|point|spread)/i);
    if (spreadMatch) {
      return parseFloat(spreadMatch[1]);
    }

    const simpleMatch = text.match(/\s([+-]\d+\.?\d*)\s/);
    if (simpleMatch) {
      return parseFloat(simpleMatch[1]);
    }

    return undefined;
  }

  /**
   * Parse total/over-under line from text
   */
  private parseTotalLine(text: string): number | undefined {
    const ouMatch = text.match(/(?:over|under|o\/u|total)\s*(\d+\.?\d*)/i);
    if (ouMatch) {
      return parseFloat(ouMatch[1]);
    }

    return undefined;
  }

  /**
   * Extract event slug from market slug
   */
  private extractEventSlug(marketSlug: string): string | undefined {
    // Remove market-specific suffixes to get event slug
    // e.g., 'nba-lal-bos-2025-01-15-spread-home-5pt5' → 'nba-lal-bos-2025-01-15'
    const match = marketSlug.match(
      /^((?:nba|nfl|nhl|mlb|ufc|soccer|tennis|golf)-[a-z]+-[a-z]+-\d{4}-\d{2}-\d{2})/i
    );
    if (match) {
      return match[1];
    }

    return undefined;
  }
}

// ===== Helper Functions =====

/**
 * Get all supported leagues
 */
export function getSupportedLeagues(): SportsLeague[] {
  return ['NBA', 'NFL', 'NHL', 'MLB', 'SOCCER', 'UFC', 'TENNIS', 'GOLF'];
}

/**
 * Get keywords for a specific league
 */
export function getLeagueKeywords(league: SportsLeague): string[] {
  return LEAGUE_KEYWORDS[league] || [];
}
