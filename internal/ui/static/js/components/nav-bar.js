import { LitElement, html, css } from 'lit';

export class NalaNav extends LitElement {
  static properties = {
    active: { type: String },
    serverAddr: { type: String, attribute: 'server-addr' },
  };

  static styles = css`
    :host {
      display: block;
      background: var(--nala-surface);
      border-bottom: 1px solid var(--nala-border);
      padding: 0 var(--nala-space-lg);
    }
    nav {
      display: flex;
      align-items: center;
      height: 3em;
      gap: var(--nala-space-xs);
    }
    .logo {
      font-family: var(--nala-font-mono);
      font-weight: 700;
      font-size: 1.1em;
      color: var(--nala-primary);
      margin-right: var(--nala-space-lg);
      white-space: nowrap;
    }
    a {
      display: flex;
      align-items: center;
      padding: var(--nala-space-sm) var(--nala-space-md);
      border-radius: var(--nala-radius-sm);
      color: var(--nala-text-muted);
      font-size: 0.9em;
      text-decoration: none;
      transition: color 0.15s, background 0.15s;
    }
    a:hover {
      color: var(--nala-text);
      background: var(--nala-surface-alt);
    }
    a.active {
      color: var(--nala-primary);
      background: var(--nala-surface-alt);
    }
    .spacer { flex: 1; }
    .server {
      font-family: var(--nala-font-mono);
      font-size: 0.75em;
      color: var(--nala-text-muted);
    }
    .dot {
      display: inline-block;
      width: 0.5em;
      height: 0.5em;
      border-radius: 50%;
      background: var(--nala-accent);
      margin-right: var(--nala-space-xs);
    }
  `;

  constructor() {
    super();
    this.active = 'query';
    this.serverAddr = '';
  }

  render() {
    const tabs = [
      { id: 'query', label: 'Query' },
      { id: 'graph', label: 'Graph' },
      { id: 'watch', label: 'Watch' },
      { id: 'stats', label: 'Stats' },
    ];

    return html`
      <nav>
        <span class="logo">NalaDB</span>
        ${tabs.map(t => html`
          <a href="#${t.id}" class="${this.active === t.id ? 'active' : ''}">${t.label}</a>
        `)}
        <span class="spacer"></span>
        <span class="server"><span class="dot"></span>${this.serverAddr}</span>
      </nav>
    `;
  }
}

customElements.define('nala-nav', NalaNav);
