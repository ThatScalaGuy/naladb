import { LitElement, html, css } from 'lit';
import { SSEClient } from '../lib/sse-client.js';
import { formatHLC } from '../lib/api-client.js';

export class NalaWatchPanel extends LitElement {
  static properties = {
    _keys: { state: true },
    _events: { state: true },
    _connected: { state: true },
  };

  static styles = css`
    :host {
      display: flex;
      flex-direction: column;
      height: 100%;
      gap: var(--nala-space-md);
    }
    .controls {
      display: flex;
      align-items: center;
      gap: var(--nala-space-sm);
    }
    input[type="text"] {
      flex: 1;
      padding: var(--nala-space-sm) var(--nala-space-md);
      background: var(--nala-surface);
      border: 1px solid var(--nala-border);
      border-radius: var(--nala-radius-sm);
      font-family: var(--nala-font-mono);
      font-size: 0.9em;
      color: var(--nala-text);
    }
    input:focus { border-color: var(--nala-primary); }
    .btn {
      padding: var(--nala-space-sm) var(--nala-space-md);
      border-radius: var(--nala-radius-sm);
      font-weight: 600;
      font-size: 0.9em;
      white-space: nowrap;
    }
    .btn-primary { background: var(--nala-primary); color: #fff; }
    .btn-primary:hover { background: var(--nala-primary-hover); }
    .btn-danger { background: var(--nala-error); color: #fff; }
    .btn-secondary {
      background: var(--nala-surface);
      border: 1px solid var(--nala-border);
    }
    .status {
      display: flex;
      align-items: center;
      gap: var(--nala-space-xs);
      font-size: 0.8em;
      color: var(--nala-text-muted);
    }
    .status-dot {
      width: 0.5em;
      height: 0.5em;
      border-radius: 50%;
    }
    .status-dot.on { background: var(--nala-accent); }
    .status-dot.off { background: var(--nala-error); }
    .event-log {
      flex: 1;
      overflow-y: auto;
      border: 1px solid var(--nala-border);
      border-radius: var(--nala-radius-md);
      background: var(--nala-surface);
    }
    .event-row {
      display: grid;
      grid-template-columns: 10em 8em 1fr auto;
      gap: var(--nala-space-sm);
      padding: var(--nala-space-sm) var(--nala-space-md);
      border-bottom: 1px solid var(--nala-border);
      font-family: var(--nala-font-mono);
      font-size: 0.85em;
      align-items: center;
    }
    .event-row:hover { background: var(--nala-surface-alt); }
    .event-time { color: var(--nala-text-muted); }
    .event-key { color: var(--nala-primary); }
    .event-value {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .badge-deleted {
      background: rgba(255, 107, 107, 0.2);
      color: var(--nala-error);
      padding: 0.1em 0.4em;
      border-radius: var(--nala-radius-sm);
      font-size: 0.8em;
    }
    .empty {
      padding: var(--nala-space-xl);
      text-align: center;
      color: var(--nala-text-muted);
    }
    .header {
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
  `;

  constructor() {
    super();
    this._keys = '';
    this._events = [];
    this._connected = false;
    this._sse = new SSEClient();
  }

  disconnectedCallback() {
    super.disconnectedCallback();
    this._sse.disconnect();
  }

  render() {
    return html`
      <div class="controls">
        <input
          type="text"
          placeholder="Enter comma-separated keys to watch..."
          .value=${this._keys}
          @input=${e => this._keys = e.target.value}
          @keydown=${e => e.key === 'Enter' && this._subscribe()}
        />
        ${this._connected
          ? html`<button class="btn btn-danger" @click=${this._disconnect}>Disconnect</button>`
          : html`<button class="btn btn-primary" @click=${this._subscribe}>Subscribe</button>`
        }
      </div>
      <div class="header">
        <div class="status">
          <span class="status-dot ${this._connected ? 'on' : 'off'}"></span>
          ${this._connected ? 'Connected' : 'Disconnected'}
        </div>
        ${this._events.length ? html`
          <button class="btn btn-secondary" @click=${() => this._events = []}>Clear</button>
        ` : ''}
      </div>
      <div class="event-log">
        ${this._events.length === 0
          ? html`<div class="empty">No events yet. Subscribe to keys to start watching.</div>`
          : this._events.map(ev => html`
            <div class="event-row">
              <span class="event-time">${formatHLC(ev.timestamp)}</span>
              <span class="event-key">${ev.key}</span>
              <span class="event-value" title="${ev.value}">${ev.value}</span>
              ${ev.deleted ? html`<span class="badge-deleted">DEL</span>` : ''}
            </div>
          `)
        }
      </div>
    `;
  }

  _subscribe() {
    const keys = this._keys.split(',').map(k => k.trim()).filter(Boolean);
    if (!keys.length) return;

    this._sse.subscribe(keys, {
      onEvent: (data) => {
        this._events = [data, ...this._events].slice(0, 500);
      },
      onError: () => {
        this._connected = false;
      },
      onOpen: () => {
        this._connected = true;
      },
    });
    this._connected = true;
  }

  _disconnect() {
    this._sse.disconnect();
    this._connected = false;
  }
}

customElements.define('nala-watch-panel', NalaWatchPanel);
