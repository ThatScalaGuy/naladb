import { LitElement, html, css } from 'lit';

export class NalaGraphControls extends LitElement {
  static properties = {
    nodeTypes: { type: Array },
    _depth: { state: true },
    _direction: { state: true },
    _startNode: { state: true },
    _activeTypes: { state: true },
  };

  static styles = css`
    :host {
      display: flex;
      flex-direction: column;
      gap: var(--nala-space-sm);
      padding: var(--nala-space-md);
      background: var(--nala-surface);
      border: 1px solid var(--nala-border);
      border-radius: var(--nala-radius-md);
      font-size: 0.85em;
    }
    label {
      color: var(--nala-text-muted);
      font-size: 0.85em;
    }
    .row {
      display: flex;
      align-items: center;
      gap: var(--nala-space-sm);
    }
    input[type="text"], input[type="range"] {
      flex: 1;
    }
    input[type="text"] {
      padding: var(--nala-space-xs) var(--nala-space-sm);
      background: var(--nala-surface-alt);
      border: 1px solid var(--nala-border);
      border-radius: var(--nala-radius-sm);
      color: var(--nala-text);
      font-family: var(--nala-font-mono);
      font-size: 0.9em;
    }
    select {
      padding: var(--nala-space-xs) var(--nala-space-sm);
      background: var(--nala-surface-alt);
      border: 1px solid var(--nala-border);
      border-radius: var(--nala-radius-sm);
      color: var(--nala-text);
    }
    .types {
      display: flex;
      flex-wrap: wrap;
      gap: var(--nala-space-xs);
    }
    .type-tag {
      padding: var(--nala-space-xs) var(--nala-space-sm);
      border-radius: var(--nala-radius-sm);
      font-size: 0.85em;
      cursor: pointer;
      border: 1px solid var(--nala-border);
      background: var(--nala-surface-alt);
      transition: all 0.15s;
    }
    .type-tag.active {
      border-color: var(--nala-primary);
      background: rgba(108, 99, 255, 0.2);
      color: var(--nala-primary);
    }
    .btn {
      padding: var(--nala-space-sm) var(--nala-space-md);
      background: var(--nala-primary);
      color: #fff;
      border-radius: var(--nala-radius-sm);
      font-weight: 600;
      font-size: 0.9em;
    }
    .btn:hover { background: var(--nala-primary-hover); }
  `;

  constructor() {
    super();
    this.nodeTypes = [];
    this._depth = 3;
    this._direction = 'out';
    this._startNode = '';
    this._activeTypes = new Set();
  }

  updated(changed) {
    if (changed.has('nodeTypes') && this.nodeTypes.length && this._activeTypes.size === 0) {
      this._activeTypes = new Set(this.nodeTypes);
    }
  }

  render() {
    return html`
      <div class="row">
        <label>Start node:</label>
        <input type="text" placeholder="node ID" .value=${this._startNode} @input=${e => this._startNode = e.target.value} />
        <button class="btn" @click=${this._emitTraverse}>Traverse</button>
      </div>
      <div class="row">
        <label>Depth: ${this._depth}</label>
        <input type="range" min="1" max="10" .value=${String(this._depth)} @input=${e => this._depth = parseInt(e.target.value)} />
      </div>
      <div class="row">
        <label>Direction:</label>
        <select .value=${this._direction} @change=${e => this._direction = e.target.value}>
          <option value="out">Outgoing</option>
          <option value="in">Incoming</option>
          <option value="both">Both</option>
        </select>
      </div>
      ${this.nodeTypes.length ? html`
        <label>Node types:</label>
        <div class="types">
          ${this.nodeTypes.map(t => html`
            <span class="type-tag ${this._activeTypes.has(t) ? 'active' : ''}" @click=${() => this._toggleType(t)}>${t}</span>
          `)}
        </div>
      ` : ''}
      <button class="btn" @click=${this._emitRefresh}>Refresh Graph</button>
    `;
  }

  _toggleType(type) {
    const next = new Set(this._activeTypes);
    if (next.has(type)) next.delete(type);
    else next.add(type);
    this._activeTypes = next;
    this._emitFilter();
  }

  _emitFilter() {
    this.dispatchEvent(new CustomEvent('filter-changed', {
      detail: { types: [...this._activeTypes] },
      bubbles: true, composed: true,
    }));
  }

  _emitTraverse() {
    if (!this._startNode.trim()) return;
    this.dispatchEvent(new CustomEvent('traverse', {
      detail: {
        start: this._startNode.trim(),
        depth: this._depth,
        direction: this._direction,
      },
      bubbles: true, composed: true,
    }));
  }

  _emitRefresh() {
    this.dispatchEvent(new CustomEvent('refresh', { bubbles: true, composed: true }));
  }
}

customElements.define('nala-graph-controls', NalaGraphControls);
