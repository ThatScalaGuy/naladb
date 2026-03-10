import { LitElement, html, css } from 'lit';
import { fetchStats } from '../lib/api-client.js';

export class NalaStatsDashboard extends LitElement {
  static properties = {
    _stats: { state: true },
    _error: { state: true },
  };

  static styles = css`
    :host {
      display: block;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(16em, 1fr));
      gap: var(--nala-space-md);
    }
    .card {
      background: var(--nala-surface);
      border: 1px solid var(--nala-border);
      border-radius: var(--nala-radius-md);
      padding: var(--nala-space-lg);
    }
    .card-title {
      font-size: 0.8em;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--nala-text-muted);
      margin-bottom: var(--nala-space-md);
    }
    .stat-row {
      display: flex;
      justify-content: space-between;
      padding: var(--nala-space-xs) 0;
    }
    .stat-label { color: var(--nala-text-muted); font-size: 0.9em; }
    .stat-value {
      font-family: var(--nala-font-mono);
      font-weight: 600;
      color: var(--nala-text);
    }
    .breakdown-item {
      display: flex;
      justify-content: space-between;
      padding: var(--nala-space-xs) 0;
      font-size: 0.9em;
    }
    .breakdown-label {
      color: var(--nala-primary);
      font-family: var(--nala-font-mono);
    }
    .breakdown-value {
      font-family: var(--nala-font-mono);
    }
    .error {
      padding: var(--nala-space-md);
      background: rgba(255, 107, 107, 0.1);
      border: 1px solid var(--nala-error);
      border-radius: var(--nala-radius-md);
      color: var(--nala-error);
    }
    .loading {
      text-align: center;
      padding: var(--nala-space-xl);
      color: var(--nala-text-muted);
    }
  `;

  constructor() {
    super();
    this._stats = null;
    this._error = null;
    this._interval = null;
  }

  connectedCallback() {
    super.connectedCallback();
    this._load();
    this._interval = setInterval(() => this._load(), 5000);
  }

  disconnectedCallback() {
    super.disconnectedCallback();
    if (this._interval) {
      clearInterval(this._interval);
      this._interval = null;
    }
  }

  async _load() {
    try {
      this._stats = await fetchStats();
      this._error = null;
    } catch (err) {
      this._error = err.message;
    }
  }

  render() {
    if (this._error) return html`<div class="error">${this._error}</div>`;
    if (!this._stats) return html`<div class="loading">Loading stats...</div>`;

    const s = this._stats;
    return html`
      <div class="grid">
        <div class="card">
          <div class="card-title">Store</div>
          ${this._stat('Keys', s.total_keys)}
          ${this._stat('Versions', s.total_versions)}
          ${this._stat('Tombstones', s.tombstones)}
          ${this._stat('Segments', s.segments)}
          ${this._stat('Size', this._fmtBytes(s.segment_bytes))}
        </div>
        <div class="card">
          <div class="card-title">Graph - Nodes</div>
          ${this._stat('Total', s.nodes_total)}
          ${this._stat('Active', s.nodes_active)}
          ${this._stat('Deleted', s.nodes_deleted)}
        </div>
        <div class="card">
          <div class="card-title">Graph - Edges</div>
          ${this._stat('Total', s.edges_total)}
          ${this._stat('Active', s.edges_active)}
          ${this._stat('Deleted', s.edges_deleted)}
        </div>
        ${this._breakdownCard('Nodes by Type', s.nodes_by_type)}
        ${this._breakdownCard('Edges by Relation', s.edges_by_relation)}
      </div>
    `;
  }

  _stat(label, value) {
    return html`
      <div class="stat-row">
        <span class="stat-label">${label}</span>
        <span class="stat-value">${value ?? 0}</span>
      </div>
    `;
  }

  _breakdownCard(title, map) {
    if (!map || Object.keys(map).length === 0) return '';
    return html`
      <div class="card">
        <div class="card-title">${title}</div>
        ${Object.entries(map).sort((a, b) => b[1] - a[1]).map(([k, v]) => html`
          <div class="breakdown-item">
            <span class="breakdown-label">${k}</span>
            <span class="breakdown-value">${v}</span>
          </div>
        `)}
      </div>
    `;
  }

  _fmtBytes(bytes) {
    if (!bytes) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB'];
    let i = 0;
    let val = Number(bytes);
    while (val >= 1024 && i < units.length - 1) { val /= 1024; i++; }
    return `${val.toFixed(i > 0 ? 1 : 0)} ${units[i]}`;
  }
}

customElements.define('nala-stats-dashboard', NalaStatsDashboard);
