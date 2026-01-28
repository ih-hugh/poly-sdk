/**
 * ClobWebSocketService - CLOB WebSocket Client for Orderbook Data
 *
 * Connects to the new Polymarket CLOB WebSocket endpoint for orderbook data:
 * wss://ws-subscriptions-clob.polymarket.com/ws/market
 *
 * This service replaces the deprecated CLOB messages on RTDS (wss://ws-live-data.polymarket.com).
 * As of January 26, 2026, CLOB messages on RTDS return error 400.
 *
 * Used by DipArbService for:
 * - Orderbook snapshots (book events)
 * - Price changes (price_change events)
 * - Last trade prices (last_trade_price events)
 *
 * Chainlink prices remain on RTDS via RealtimeServiceV2.
 *
 * @see https://docs.polymarket.com/developers/CLOB/websocket/market-channel
 */

import { EventEmitter } from 'events';
import WebSocket from 'ws';
import type { OrderbookSnapshot, MarketSubscription, MarketDataHandlers } from './realtime-service-v2.js';

// ============================================================================
// Types
// ============================================================================

export interface ClobWebSocketConfig {
  /** Enable debug logging (default: false) */
  debug?: boolean;
  /** Maximum reconnection attempts before giving up (default: 10) */
  maxReconnectAttempts?: number;
  /** Initial reconnection delay in ms (default: 1000) */
  initialReconnectDelay?: number;
  /** Maximum reconnection delay in ms (default: 30000) */
  maxReconnectDelay?: number;
}

export interface ClobSubscription {
  id: string;
  tokenIds: string[];
  unsubscribe: () => void;
}

// CLOB WebSocket message types
interface ClobBookMessage {
  event_type: 'book';
  asset_id: string;
  market: string;
  timestamp: string;
  bids: Array<{ price: string; size: string }>;
  asks: Array<{ price: string; size: string }>;
  hash: string;
}

interface ClobPriceChangeMessage {
  event_type: 'price_change';
  asset_id: string;
  price: string;
  side: 'BUY' | 'SELL';
  size: string;
  timestamp: string;
}

interface ClobLastTradePriceMessage {
  event_type: 'last_trade_price';
  asset_id: string;
  price: string;
  timestamp: string;
}

type ClobMessage = ClobBookMessage | ClobPriceChangeMessage | ClobLastTradePriceMessage;

// ============================================================================
// ClobWebSocketService Implementation
// ============================================================================

export class ClobWebSocketService extends EventEmitter {
  private ws: WebSocket | null = null;
  private pingInterval: NodeJS.Timeout | null = null;
  private subscriptions: Map<string, ClobSubscription> = new Map();
  private subscriptionIdCounter = 0;

  // Configuration
  private config: Required<ClobWebSocketConfig>;

  // Connection state
  private connected = false;
  private manualDisconnect = false;
  private isReconnecting = false;
  private reconnectAttempts = 0;
  private reconnectTimer: NodeJS.Timeout | null = null;

  // Tracked token IDs for subscriptions
  private subscribedTokenIds: Set<string> = new Set();

  // Pending subscriptions queue (for subscriptions received before connection)
  private pendingTokenIds: Set<string> = new Set();

  // Track if initial subscription was sent
  private initialSubscriptionSent = false;

  // Track last message received (for health monitoring)
  private lastMessageTime = 0;
  private messageCount = 0;

  // Endpoint and timing constants
  private readonly ENDPOINT = 'wss://ws-subscriptions-clob.polymarket.com/ws/market';
  private readonly PING_INTERVAL_MS = 10_000; // Required by Polymarket

  // Cache for orderbooks (keyed by asset_id)
  private bookCache: Map<string, OrderbookSnapshot> = new Map();

  constructor(config: ClobWebSocketConfig = {}) {
    super();
    this.config = {
      debug: config.debug ?? false,
      maxReconnectAttempts: config.maxReconnectAttempts ?? 10,
      initialReconnectDelay: config.initialReconnectDelay ?? 1000,
      maxReconnectDelay: config.maxReconnectDelay ?? 30000,
    };
  }

  // ============================================================================
  // Connection Management
  // ============================================================================

  /**
   * Connect to the CLOB WebSocket server
   */
  connect(): this {
    // Only skip if we're actually connected or actively connecting
    if (this.connected && this.ws?.readyState === WebSocket.OPEN) {
      this.log('Already connected');
      return this;
    }

    // If there's a WebSocket in CONNECTING state, wait for it
    if (this.ws?.readyState === WebSocket.CONNECTING) {
      this.log('Connection in progress, waiting');
      return this;
    }

    // Cancel any pending reconnection timer since we're manually connecting
    this.cancelReconnect();

    this.manualDisconnect = false;
    this.cleanupConnection();

    console.log(`[ClobWebSocket] Connecting to ${this.ENDPOINT}`);
    this.log(`Connecting to ${this.ENDPOINT}`);

    try {
      this.ws = new WebSocket(this.ENDPOINT);

      this.ws.on('open', () => this.handleOpen());
      this.ws.on('message', (data: Buffer) => this.handleMessage(data));
      this.ws.on('close', (code: number, reason: Buffer) => this.handleClose(code, reason.toString()));
      this.ws.on('error', (error: Error) => this.handleError(error));
    } catch (error) {
      console.log(`[ClobWebSocket] Connection error: ${error}`);
      this.log(`Connection error: ${error}`);
      this.scheduleReconnect();
    }

    return this;
  }

  /**
   * Disconnect from the WebSocket server
   */
  disconnect(): void {
    this.manualDisconnect = true;
    this.cancelReconnect();
    this.cleanupConnection();
    this.subscriptions.clear();
    this.subscribedTokenIds.clear();
    this.pendingTokenIds.clear();
    this.bookCache.clear();
    this.reconnectAttempts = 0;
    this.initialSubscriptionSent = false;
  }

  /**
   * Check if connected
   */
  isConnected(): boolean {
    return this.connected;
  }

  /**
   * Get connection stats for debugging
   */
  getStats(): { connected: boolean; subscribedTokens: number; pendingTokens: number; messagesReceived: number; lastMessageAge: number } {
    return {
      connected: this.connected,
      subscribedTokens: this.subscribedTokenIds.size,
      pendingTokens: this.pendingTokenIds.size,
      messagesReceived: this.messageCount,
      lastMessageAge: this.lastMessageTime > 0 ? Date.now() - this.lastMessageTime : -1,
    };
  }

  // ============================================================================
  // Subscription API
  // ============================================================================

  /**
   * Subscribe to market data for specific token IDs
   * Compatible with RealtimeServiceV2.subscribeMarkets() API
   */
  subscribeMarkets(tokenIds: string[], handlers: MarketDataHandlers = {}): MarketSubscription {
    const subId = `clob_${++this.subscriptionIdCounter}`;

    console.log(`[ClobWebSocket] subscribeMarkets called: ${tokenIds.length} tokens, connected=${this.connected}`);
    this.log(`subscribeMarkets: ${tokenIds.length} tokens, connected=${this.connected}`);

    // Track new token IDs
    const newTokenIds = tokenIds.filter(id => !this.subscribedTokenIds.has(id));
    newTokenIds.forEach(id => this.subscribedTokenIds.add(id));

    if (newTokenIds.length > 0) {
      if (this.connected) {
        // Send subscription immediately
        console.log(`[ClobWebSocket] Sending subscription for ${newTokenIds.length} new tokens`);
        this.sendSubscribe(newTokenIds);
      } else {
        // Queue for later when connected
        console.log(`[ClobWebSocket] Queueing ${newTokenIds.length} tokens (not connected)`);
        newTokenIds.forEach(id => this.pendingTokenIds.add(id));
      }
    }

    // Register event handlers
    const orderbookHandler = (book: OrderbookSnapshot) => {
      if (tokenIds.includes(book.tokenId)) {
        handlers.onOrderbook?.(book);
      }
    };

    this.on('orderbook', orderbookHandler);

    const subscription: MarketSubscription = {
      id: subId,
      topic: 'clob_market',
      type: '*',
      tokenIds,
      unsubscribe: () => {
        this.off('orderbook', orderbookHandler);
        this.subscriptions.delete(subId);

        // Remove token IDs not used by other subscriptions
        const stillUsed = new Set<string>();
        for (const sub of this.subscriptions.values()) {
          sub.tokenIds.forEach(id => stillUsed.add(id));
        }
        tokenIds.forEach(id => {
          if (!stillUsed.has(id)) {
            this.subscribedTokenIds.delete(id);
            this.pendingTokenIds.delete(id);
          }
        });
      },
    };

    this.subscriptions.set(subId, subscription);
    return subscription;
  }

  /**
   * Get cached orderbook for an asset
   */
  getBook(assetId: string): OrderbookSnapshot | undefined {
    return this.bookCache.get(assetId);
  }

  // ============================================================================
  // Private: WebSocket Handlers
  // ============================================================================

  private handleOpen(): void {
    this.connected = true;
    this.reconnectAttempts = 0;
    this.initialSubscriptionSent = false;
    console.log('[ClobWebSocket] ✅ Connected to CLOB WebSocket');
    this.log('Connected to CLOB WebSocket');

    // Start ping interval
    this.startPing();

    // Send ALL tracked subscriptions on connect/reconnect
    // This includes both subscribedTokenIds and pendingTokenIds
    const allTokenIds = new Set([...this.subscribedTokenIds, ...this.pendingTokenIds]);
    if (allTokenIds.size > 0) {
      const tokenIds = Array.from(allTokenIds);
      console.log(`[ClobWebSocket] Sending initial subscription for ${tokenIds.length} tokens`);
      this.log(`Subscribing to ${tokenIds.length} tokens on connect`);
      this.sendInitialSubscription(tokenIds);
      this.initialSubscriptionSent = true;
      // Clear pending as they've been sent
      this.pendingTokenIds.clear();
      // Ensure all are in subscribedTokenIds
      tokenIds.forEach(id => this.subscribedTokenIds.add(id));
    } else {
      console.log('[ClobWebSocket] No tokens to subscribe to on connect');
    }

    this.emit('connected');
  }

  private handleMessage(data: Buffer): void {
    this.lastMessageTime = Date.now();
    this.messageCount++;

    try {
      const rawMessage = data.toString();

      // Log first few messages for debugging
      if (this.messageCount <= 3) {
        console.log(`[ClobWebSocket] Message #${this.messageCount}: ${rawMessage.substring(0, 200)}...`);
      }

      const message = JSON.parse(rawMessage) as ClobMessage | ClobMessage[];

      // Handle array of messages or single message
      const messages = Array.isArray(message) ? message : [message];

      for (const msg of messages) {
        this.processMessage(msg);
      }
    } catch (error) {
      console.log(`[ClobWebSocket] Failed to parse message: ${error}`);
      this.log(`Failed to parse message: ${error}`);
    }
  }

  private processMessage(msg: ClobMessage): void {
    // Handle messages that might not have event_type (e.g., error messages)
    if (!msg || typeof msg !== 'object') {
      return;
    }

    // Check for error response
    if ('statusCode' in msg || 'error' in msg || 'message' in msg) {
      const errorMsg = msg as unknown as { statusCode?: number; message?: string; error?: string };
      console.log(`[ClobWebSocket] ⚠️ Server error: ${JSON.stringify(errorMsg)}`);
      this.emit('serverError', errorMsg);
      return;
    }

    switch (msg.event_type) {
      case 'book':
        this.handleBookMessage(msg);
        break;
      case 'price_change':
        // Price changes are deltas - we could update the book cache here
        // For now, we rely on full book snapshots
        this.log(`Price change: ${msg.asset_id} ${msg.side} ${msg.price} x ${msg.size}`);
        break;
      case 'last_trade_price':
        this.log(`Last trade: ${msg.asset_id} @ ${msg.price}`);
        this.emit('lastTrade', {
          assetId: msg.asset_id,
          price: parseFloat(msg.price),
          timestamp: this.normalizeTimestamp(msg.timestamp),
        });
        break;
      default:
        // Unknown message type - might be a different format
        if (this.config.debug) {
          console.log(`[ClobWebSocket] Unknown message type: ${JSON.stringify(msg).substring(0, 100)}`);
        }
        break;
    }
  }

  private handleBookMessage(msg: ClobBookMessage): void {
    // Log orderbook reception for debugging
    if (this.messageCount <= 5) {
      console.log(`[ClobWebSocket] 📚 Orderbook received for ${msg.asset_id}: ${msg.bids?.length || 0} bids, ${msg.asks?.length || 0} asks`);
    }

    // Parse bids and asks, sort appropriately
    const bids = (msg.bids || [])
      .map(l => ({ price: parseFloat(l.price), size: parseFloat(l.size) }))
      .sort((a, b) => b.price - a.price);

    const asks = (msg.asks || [])
      .map(l => ({ price: parseFloat(l.price), size: parseFloat(l.size) }))
      .sort((a, b) => a.price - b.price);

    const book: OrderbookSnapshot = {
      tokenId: msg.asset_id,
      assetId: msg.asset_id, // Backward compatibility
      market: msg.market,
      bids,
      asks,
      timestamp: this.normalizeTimestamp(msg.timestamp),
      tickSize: '0.01',
      minOrderSize: '1',
      hash: msg.hash,
    };

    // Cache and emit
    this.bookCache.set(msg.asset_id, book);
    this.emit('orderbook', book);
  }

  private handleClose(code: number, reason: string): void {
    this.connected = false;
    this.stopPing();
    console.log(`[ClobWebSocket] ❌ Disconnected: code=${code}, reason=${reason}`);
    this.log(`Disconnected: code=${code}, reason=${reason}`);

    // Clean up the closed WebSocket so reconnection can create a new one
    if (this.ws) {
      try {
        this.ws.removeAllListeners();
      } catch {
        // Ignore cleanup errors
      }
      this.ws = null;
    }
    this.initialSubscriptionSent = false;

    this.emit('disconnected', { code, reason });

    // Reconnect if not manual disconnect
    if (!this.manualDisconnect) {
      this.scheduleReconnect();
    }
  }

  private handleError(error: Error): void {
    console.log(`[ClobWebSocket] ⚠️ WebSocket error: ${error.message}`);
    this.log(`WebSocket error: ${error.message}`);
    this.emit('error', error);
  }

  // ============================================================================
  // Private: Ping/Pong
  // ============================================================================

  private startPing(): void {
    this.stopPing();
    this.pingInterval = setInterval(() => {
      if (this.ws && this.connected) {
        try {
          this.ws.ping();
        } catch (error) {
          this.log(`Ping error: ${error}`);
        }
      }
    }, this.PING_INTERVAL_MS);
  }

  private stopPing(): void {
    if (this.pingInterval) {
      clearInterval(this.pingInterval);
      this.pingInterval = null;
    }
  }

  // ============================================================================
  // Private: Subscription Messages
  // ============================================================================

  /**
   * Send initial subscription message on connect
   * Format: { assets_ids: ["token1", "token2"], type: "market" }
   */
  private sendInitialSubscription(tokenIds: string[]): void {
    if (!this.ws || !this.connected) {
      console.log('[ClobWebSocket] Cannot send initial subscription: not connected');
      return;
    }

    const msg = {
      assets_ids: tokenIds,
      type: 'market',
    };

    console.log(`[ClobWebSocket] 📤 Sending initial subscription: ${tokenIds.length} tokens`);
    this.log(`Sending initial subscription: ${JSON.stringify(msg)}`);

    try {
      this.ws.send(JSON.stringify(msg));
    } catch (error) {
      console.log(`[ClobWebSocket] Failed to send subscription: ${error}`);
    }
  }

  /**
   * Send dynamic subscription for new tokens
   * Format: { assets_ids: ["token3"], operation: "subscribe" }
   */
  private sendSubscribe(tokenIds: string[]): void {
    if (!this.ws || !this.connected) {
      console.log('[ClobWebSocket] Cannot send subscribe: not connected, queueing');
      tokenIds.forEach(id => this.pendingTokenIds.add(id));
      return;
    }

    // Use initial format if this is the first subscription
    if (!this.initialSubscriptionSent) {
      this.sendInitialSubscription(tokenIds);
      this.initialSubscriptionSent = true;
      return;
    }

    const msg = {
      assets_ids: tokenIds,
      operation: 'subscribe',
    };

    console.log(`[ClobWebSocket] 📤 Sending subscribe: ${tokenIds.length} tokens`);
    this.log(`Sending subscribe: ${JSON.stringify(msg)}`);

    try {
      this.ws.send(JSON.stringify(msg));
    } catch (error) {
      console.log(`[ClobWebSocket] Failed to send subscribe: ${error}`);
    }
  }

  // ============================================================================
  // Private: Reconnection
  // ============================================================================

  private cleanupConnection(): void {
    this.stopPing();

    if (this.ws) {
      try {
        this.ws.removeAllListeners();
        this.ws.terminate();
      } catch {
        // Ignore cleanup errors
      }
      this.ws = null;
    }

    this.connected = false;
    this.initialSubscriptionSent = false;
  }

  private cancelReconnect(): void {
    if (this.reconnectTimer) {
      console.log('[ClobWebSocket] Cancelling pending reconnection timer');
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.isReconnecting = false;
  }

  private scheduleReconnect(): void {
    if (this.manualDisconnect || this.isReconnecting) {
      return;
    }

    if (this.reconnectAttempts >= this.config.maxReconnectAttempts) {
      console.log(`[ClobWebSocket] Max reconnection attempts (${this.config.maxReconnectAttempts}) exceeded`);
      this.log(`Max reconnection attempts (${this.config.maxReconnectAttempts}) exceeded`);
      this.emit('reconnectFailed', {
        attempts: this.reconnectAttempts,
        message: 'Max reconnection attempts exceeded',
      });
      return;
    }

    this.isReconnecting = true;
    this.reconnectAttempts++;

    // Exponential backoff with jitter
    const baseDelay = this.config.initialReconnectDelay * Math.pow(2, this.reconnectAttempts - 1);
    const jitter = Math.random() * 1000;
    const delay = Math.min(baseDelay + jitter, this.config.maxReconnectDelay);

    console.log(`[ClobWebSocket] Reconnecting in ${Math.round(delay)}ms (attempt ${this.reconnectAttempts}/${this.config.maxReconnectAttempts})`);
    this.log(`Reconnecting in ${Math.round(delay)}ms (attempt ${this.reconnectAttempts}/${this.config.maxReconnectAttempts})`);

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.isReconnecting = false;

      if (!this.manualDisconnect) {
        console.log(`[ClobWebSocket] Attempting reconnection (attempt ${this.reconnectAttempts}/${this.config.maxReconnectAttempts})`);
        this.connect();
      } else {
        console.log('[ClobWebSocket] Reconnection cancelled (manual disconnect)');
      }
    }, delay);
  }

  // ============================================================================
  // Private: Utilities
  // ============================================================================

  private normalizeTimestamp(ts: unknown): number {
    if (typeof ts === 'string') {
      const parsed = parseInt(ts, 10);
      if (isNaN(parsed)) return Date.now();
      // Convert seconds to milliseconds if needed
      return parsed < 1e12 ? parsed * 1000 : parsed;
    }
    if (typeof ts === 'number') {
      return ts < 1e12 ? ts * 1000 : ts;
    }
    return Date.now();
  }

  private log(message: string): void {
    if (this.config.debug) {
      console.log(`[ClobWebSocket] ${message}`);
    }
  }
}

export default ClobWebSocketService;
