import { LitElement, html, css } from 'lit';

export class NalaTimePicker extends LitElement {
  static properties = {
    _mode: { state: true },
    _value: { state: true },
  };

  static styles = css`
    :host {
      display: flex;
      align-items: center;
      gap: var(--nala-space-sm);
      font-size: 0.85em;
    }
    .mode-btns {
      display: flex;
      border: 1px solid var(--nala-border);
      border-radius: var(--nala-radius-sm);
      overflow: hidden;
    }
    .mode-btns button {
      padding: var(--nala-space-xs) var(--nala-space-sm);
      background: var(--nala-surface);
      border-right: 1px solid var(--nala-border);
      font-size: 0.9em;
    }
    .mode-btns button:last-child { border-right: none; }
    .mode-btns button.active {
      background: var(--nala-primary);
      color: #fff;
    }
    input[type="datetime-local"] {
      padding: var(--nala-space-xs) var(--nala-space-sm);
      background: var(--nala-surface);
      border: 1px solid var(--nala-border);
      border-radius: var(--nala-radius-sm);
      color: var(--nala-text);
    }
    .presets {
      display: flex;
      gap: var(--nala-space-xs);
    }
    .presets button {
      padding: var(--nala-space-xs) var(--nala-space-sm);
      background: var(--nala-surface-alt);
      border-radius: var(--nala-radius-sm);
      font-size: 0.85em;
      color: var(--nala-text-muted);
    }
    .presets button:hover { color: var(--nala-text); }
  `;

  constructor() {
    super();
    this._mode = 'current';
    this._value = '';
  }

  render() {
    return html`
      <div class="mode-btns">
        <button class="${this._mode === 'current' ? 'active' : ''}" @click=${() => this._setMode('current')}>Current</button>
        <button class="${this._mode === 'at' ? 'active' : ''}" @click=${() => this._setMode('at')}>At</button>
      </div>
      ${this._mode === 'at' ? html`
        <input type="datetime-local" step="1" .value=${this._value} @change=${this._onInput} />
        <div class="presets">
          ${[
            { label: '5m ago', mins: 5 },
            { label: '1h ago', mins: 60 },
            { label: '24h ago', mins: 1440 },
            { label: '7d ago', mins: 10080 },
          ].map(p => html`
            <button @click=${() => this._preset(p.mins)}>${p.label}</button>
          `)}
        </div>
      ` : ''}
    `;
  }

  _setMode(mode) {
    this._mode = mode;
    if (mode === 'current') {
      this._value = '';
      this._emit('');
    }
  }

  _onInput(e) {
    this._value = e.target.value;
    this._emit(this._toRFC3339(this._value));
  }

  _preset(mins) {
    const d = new Date(Date.now() - mins * 60000);
    // Format for datetime-local input
    this._value = d.toISOString().slice(0, 19);
    this._emit(d.toISOString());
  }

  _toRFC3339(localStr) {
    if (!localStr) return '';
    return new Date(localStr).toISOString();
  }

  _emit(value) {
    this.dispatchEvent(new CustomEvent('value-changed', {
      detail: { value },
      bubbles: true,
      composed: true,
    }));
  }
}

customElements.define('nala-time-picker', NalaTimePicker);
