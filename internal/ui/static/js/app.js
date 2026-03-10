import { LitElement, html, css } from 'lit';
import './components/nav-bar.js';
import './components/query-editor.js';
import './components/query-results.js';
import './components/graph-explorer.js';
import './components/graph-controls.js';
import './components/time-picker.js';
import './components/watch-panel.js';
import './components/stats-dashboard.js';

export class NalaApp extends LitElement {
  static properties = {
    _page: { state: true },
  };

  static styles = css`
    :host {
      display: flex;
      flex-direction: column;
      height: 100vh;
    }
    .content {
      flex: 1;
      min-height: 0;
      padding: var(--nala-space-lg);
    }
    .content > * {
      height: 100%;
    }
  `;

  constructor() {
    super();
    this._page = this._getHash();
    window.addEventListener('hashchange', () => {
      this._page = this._getHash();
    });
  }

  _getHash() {
    const h = window.location.hash.replace('#', '') || 'query';
    return h;
  }

  render() {
    return html`
      <nala-nav active=${this._page} server-addr=${document.body.dataset.serverAddr || ''}></nala-nav>
      <div class="content">
        ${this._renderPage()}
      </div>
    `;
  }

  _renderPage() {
    switch (this._page) {
      case 'query': return html`<nala-query-editor></nala-query-editor>`;
      case 'graph': return html`<nala-graph-explorer></nala-graph-explorer>`;
      case 'watch': return html`<nala-watch-panel></nala-watch-panel>`;
      case 'stats': return html`<nala-stats-dashboard></nala-stats-dashboard>`;
      default: return html`<nala-query-editor></nala-query-editor>`;
    }
  }
}

customElements.define('nala-app', NalaApp);
