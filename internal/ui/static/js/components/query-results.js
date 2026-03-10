import { LitElement, html, css } from 'lit';

export class NalaQueryResults extends LitElement {
  static properties = {
    columns: { type: Array },
    rows: { type: Array },
    _page: { state: true },
    _pageSize: { state: true },
    _sortCol: { state: true },
    _sortAsc: { state: true },
  };

  static styles = css`
    :host { display: block; }
    .table-wrap {
      overflow: auto;
      border: 1px solid var(--nala-border);
      border-radius: var(--nala-radius-md);
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-family: var(--nala-font-mono);
      font-size: 0.85em;
    }
    thead { position: sticky; top: 0; z-index: 1; }
    th {
      background: var(--nala-surface-alt);
      padding: var(--nala-space-sm) var(--nala-space-md);
      text-align: left;
      white-space: nowrap;
      cursor: pointer;
      user-select: none;
      border-bottom: 2px solid var(--nala-border);
    }
    th:hover { color: var(--nala-primary); }
    .sort-arrow { font-size: 0.8em; margin-left: 0.3em; }
    td {
      padding: var(--nala-space-sm) var(--nala-space-md);
      border-bottom: 1px solid var(--nala-border);
      white-space: nowrap;
      max-width: 20em;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    tr:hover td { background: var(--nala-surface-alt); }
    .pagination {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: var(--nala-space-sm) var(--nala-space-md);
      font-size: 0.85em;
      color: var(--nala-text-muted);
    }
    .pagination button {
      padding: var(--nala-space-xs) var(--nala-space-sm);
      background: var(--nala-surface);
      border: 1px solid var(--nala-border);
      border-radius: var(--nala-radius-sm);
      font-size: 0.9em;
    }
    .pagination button:hover:not(:disabled) { border-color: var(--nala-primary); }
    .pagination button:disabled { opacity: 0.4; cursor: default; }
    .page-controls {
      display: flex;
      align-items: center;
      gap: var(--nala-space-sm);
    }
    select {
      padding: var(--nala-space-xs);
      background: var(--nala-surface);
      border: 1px solid var(--nala-border);
      border-radius: var(--nala-radius-sm);
      font-size: 0.85em;
    }
    .empty {
      padding: var(--nala-space-xl);
      text-align: center;
      color: var(--nala-text-muted);
    }
  `;

  constructor() {
    super();
    this.columns = [];
    this.rows = [];
    this._page = 0;
    this._pageSize = 25;
    this._sortCol = null;
    this._sortAsc = true;
  }

  get _sortedRows() {
    if (!this._sortCol) return this.rows;
    const col = this._sortCol;
    const dir = this._sortAsc ? 1 : -1;
    return [...this.rows].sort((a, b) => {
      const va = a[col] || '';
      const vb = b[col] || '';
      return va.localeCompare(vb, undefined, { numeric: true }) * dir;
    });
  }

  get _pagedRows() {
    const start = this._page * this._pageSize;
    return this._sortedRows.slice(start, start + this._pageSize);
  }

  get _totalPages() {
    return Math.max(1, Math.ceil(this.rows.length / this._pageSize));
  }

  updated(changed) {
    if (changed.has('rows')) this._page = 0;
  }

  render() {
    if (!this.columns?.length) {
      return html`<div class="empty">No results</div>`;
    }

    return html`
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              ${this.columns.map(col => html`
                <th @click=${() => this._toggleSort(col)}>
                  ${col}
                  ${this._sortCol === col ? html`
                    <span class="sort-arrow">${this._sortAsc ? '\u25B2' : '\u25BC'}</span>
                  ` : ''}
                </th>
              `)}
            </tr>
          </thead>
          <tbody>
            ${this._pagedRows.map(row => html`
              <tr>
                ${this.columns.map(col => html`<td title="${row[col] || ''}">${row[col] || ''}</td>`)}
              </tr>
            `)}
          </tbody>
        </table>
      </div>
      ${this.rows.length > this._pageSize ? html`
        <div class="pagination">
          <span>Page ${this._page + 1} of ${this._totalPages}</span>
          <div class="page-controls">
            <select .value=${String(this._pageSize)} @change=${this._onPageSize}>
              <option value="25">25</option>
              <option value="50">50</option>
              <option value="100">100</option>
            </select>
            <button @click=${() => this._page = Math.max(0, this._page - 1)} ?disabled=${this._page === 0}>Prev</button>
            <button @click=${() => this._page = Math.min(this._totalPages - 1, this._page + 1)} ?disabled=${this._page >= this._totalPages - 1}>Next</button>
          </div>
        </div>
      ` : ''}
    `;
  }

  _toggleSort(col) {
    if (this._sortCol === col) {
      this._sortAsc = !this._sortAsc;
    } else {
      this._sortCol = col;
      this._sortAsc = true;
    }
  }

  _onPageSize(e) {
    this._pageSize = parseInt(e.target.value);
    this._page = 0;
  }
}

customElements.define('nala-query-results', NalaQueryResults);
