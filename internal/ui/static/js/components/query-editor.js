import { LitElement, html, css } from 'lit';
import { executeQuery } from '../lib/api-client.js';

const EXAMPLES = [
  { label: 'Show Keys', query: 'SHOW KEYS' },
  { label: 'Show Nodes', query: 'SHOW NODES' },
  { label: 'Show Edges', query: 'SHOW EDGES' },
  { label: 'Describe Nodes', query: 'DESCRIBE NODES\nLIMIT 25' },
  { label: 'Describe Node', query: 'DESCRIBE NODE "node_id"' },
  { label: 'Describe Edges', query: 'DESCRIBE EDGES\nLIMIT 25' },
  { label: 'Match Pattern', query: 'MATCH (a)-[r]->(b)\nRETURN a.id, r.relation, b.id\nLIMIT 25' },
  { label: 'Key History', query: 'GET history("mykey")\nLAST 50' },
  { label: 'Causal', query: 'CAUSAL FROM "node_id"\nDEPTH 3 WINDOW 60s' },
];

export class NalaQueryEditor extends LitElement {
  static properties = {
    _query: { state: true },
    _result: { state: true },
    _loading: { state: true },
    _error: { state: true },
  };

  static styles = css`
    :host {
      display: flex;
      flex-direction: column;
      height: 100%;
      gap: var(--nala-space-md);
    }
    .editor-area {
      display: flex;
      flex-direction: column;
      gap: var(--nala-space-sm);
    }
    textarea {
      width: 100%;
      min-height: 8em;
      padding: var(--nala-space-md);
      background: var(--nala-surface);
      border: 1px solid var(--nala-border);
      border-radius: var(--nala-radius-md);
      font-family: var(--nala-font-mono);
      font-size: 0.9em;
      color: var(--nala-text);
      resize: vertical;
      tab-size: 2;
    }
    textarea::placeholder { color: var(--nala-text-muted); }
    textarea:focus { border-color: var(--nala-primary); }
    .toolbar {
      display: flex;
      align-items: center;
      gap: var(--nala-space-sm);
      flex-wrap: wrap;
    }
    .btn-run {
      padding: var(--nala-space-sm) var(--nala-space-lg);
      background: var(--nala-primary);
      color: #fff;
      border-radius: var(--nala-radius-sm);
      font-weight: 600;
      font-size: 0.9em;
      transition: background 0.15s;
    }
    .btn-run:hover { background: var(--nala-primary-hover); }
    .btn-run:disabled { opacity: 0.5; cursor: not-allowed; }
    select {
      padding: var(--nala-space-sm);
      background: var(--nala-surface);
      border: 1px solid var(--nala-border);
      border-radius: var(--nala-radius-sm);
      font-size: 0.85em;
      cursor: pointer;
    }
    .meta {
      font-size: 0.8em;
      color: var(--nala-text-muted);
    }
    .shortcut {
      font-size: 0.75em;
      color: var(--nala-text-muted);
    }
    .error {
      padding: var(--nala-space-md);
      background: rgba(255, 107, 107, 0.1);
      border: 1px solid var(--nala-error);
      border-radius: var(--nala-radius-md);
      color: var(--nala-error);
      font-family: var(--nala-font-mono);
      font-size: 0.85em;
    }
    .results-area {
      flex: 1;
      min-height: 0;
      overflow: auto;
    }
  `;

  constructor() {
    super();
    this._query = '';
    this._result = null;
    this._loading = false;
    this._error = null;
  }

  render() {
    return html`
      <div class="editor-area">
        <textarea
          .value=${this._query}
          @input=${e => this._query = e.target.value}
          @keydown=${this._onKeyDown}
          placeholder="Enter a NalaQL query..."
          spellcheck="false"
        ></textarea>
        <div class="toolbar">
          <button class="btn-run" @click=${this._run} ?disabled=${this._loading}>
            ${this._loading ? 'Running...' : 'Run'}
          </button>
          <select @change=${this._onExample}>
            <option value="">Examples...</option>
            ${EXAMPLES.map(e => html`<option value=${e.query}>${e.label}</option>`)}
          </select>
          <span class="shortcut">Ctrl+Enter to run</span>
          ${this._result ? html`
            <span class="meta">${this._result.rows.length} rows, ${this._result.elapsed_ms}ms</span>
          ` : ''}
        </div>
      </div>
      ${this._error ? html`<div class="error">${this._error}</div>` : ''}
      <div class="results-area">
        ${this._result ? html`
          <nala-query-results .columns=${this._result.columns} .rows=${this._result.rows}></nala-query-results>
        ` : ''}
      </div>
    `;
  }

  _onKeyDown(e) {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      e.preventDefault();
      this._run();
    }
  }

  _onExample(e) {
    if (e.target.value) {
      this._query = e.target.value;
      e.target.selectedIndex = 0;
    }
  }

  async _run() {
    const q = this._query.trim();
    if (!q) return;
    this._loading = true;
    this._error = null;
    try {
      this._result = await executeQuery(q);
    } catch (err) {
      this._error = err.message;
      this._result = null;
    } finally {
      this._loading = false;
    }
  }
}

customElements.define('nala-query-editor', NalaQueryEditor);
