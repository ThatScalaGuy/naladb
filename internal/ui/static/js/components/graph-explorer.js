import { LitElement, html, css } from 'lit';
import { fetchGraphNodes, fetchGraphEdges, fetchGraphTraverse, formatHLC, fetchLatestValue, fetchSensorHistory } from '../lib/api-client.js';
import * as d3Force from 'd3-force';
import * as d3Selection from 'd3-selection';
import * as d3Zoom from 'd3-zoom';
import * as d3Drag from 'd3-drag';

const TYPE_COLORS = [
  '#6c63ff', '#00d4aa', '#ff6b6b', '#ffd93d', '#ff8a65',
  '#4fc3f7', '#ba68c8', '#81c784', '#e0e0e8', '#f06292',
];

export class NalaGraphExplorer extends LitElement {
  static properties = {
    _nodes: { state: true },
    _links: { state: true },
    _nodeTypes: { state: true },
    _selected: { state: true },
    _loading: { state: true },
    _error: { state: true },
    _at: { state: true },
    _filterTypes: { state: true },
    _sensorValue: { state: true },
    _sensorHistory: { state: true },
    _sensorInterval: { state: true },
  };

  static styles = css`
    :host {
      display: flex;
      height: 100%;
      gap: var(--nala-space-md);
    }
    .sidebar {
      width: 18em;
      flex-shrink: 0;
      display: flex;
      flex-direction: column;
      gap: var(--nala-space-md);
      overflow-y: auto;
    }
    .graph-area {
      flex: 1;
      position: relative;
      background: var(--nala-surface);
      border: 1px solid var(--nala-border);
      border-radius: var(--nala-radius-md);
      overflow: hidden;
    }
    svg { width: 100%; height: 100%; }
    .node-circle {
      cursor: pointer;
      stroke: var(--nala-bg);
      stroke-width: 1.5;
    }
    .node-circle:hover { stroke: var(--nala-text); stroke-width: 2; }
    .link-line {
      stroke: var(--nala-border);
      stroke-opacity: 0.6;
    }
    .link-line:hover {
      stroke: var(--nala-text-muted);
      stroke-opacity: 1;
      stroke-width: 2;
    }
    .node-label {
      font-family: var(--nala-font-mono);
      font-size: 9px;
      fill: var(--nala-text-muted);
      pointer-events: none;
      text-anchor: middle;
    }
    .detail-panel {
      background: var(--nala-surface);
      border: 1px solid var(--nala-border);
      border-radius: var(--nala-radius-md);
      padding: var(--nala-space-md);
      font-size: 0.85em;
    }
    .detail-panel h3 {
      margin: 0 0 var(--nala-space-sm);
      color: var(--nala-primary);
      font-family: var(--nala-font-mono);
      font-size: 1em;
    }
    .detail-row {
      display: flex;
      justify-content: space-between;
      padding: var(--nala-space-xs) 0;
    }
    .detail-label { color: var(--nala-text-muted); }
    .detail-value { font-family: var(--nala-font-mono); }
    .error {
      padding: var(--nala-space-md);
      background: rgba(255, 107, 107, 0.1);
      border: 1px solid var(--nala-error);
      border-radius: var(--nala-radius-md);
      color: var(--nala-error);
      font-size: 0.85em;
    }
    .loading-overlay {
      position: absolute;
      inset: 0;
      display: flex;
      align-items: center;
      justify-content: center;
      background: rgba(26, 26, 46, 0.7);
      color: var(--nala-text-muted);
    }
    .time-row {
      padding: var(--nala-space-sm);
      background: var(--nala-surface);
      border: 1px solid var(--nala-border);
      border-radius: var(--nala-radius-md);
    }
    .time-label {
      font-size: 0.8em;
      color: var(--nala-text-muted);
      margin-bottom: var(--nala-space-xs);
    }
    marker { fill: var(--nala-border); }
    .sensor-value {
      font-size: 1.8em;
      font-family: var(--nala-font-mono);
      font-weight: bold;
      text-align: center;
      padding: var(--nala-space-sm) 0;
    }
    .sensor-status {
      text-align: center;
      font-size: 0.8em;
      padding: 2px 8px;
      border-radius: var(--nala-radius-sm);
      display: inline-block;
      margin: 0 auto;
    }
    .status-normal { background: rgba(0, 212, 170, 0.15); color: #00d4aa; }
    .status-warning { background: rgba(255, 217, 61, 0.15); color: #ffd93d; }
    .status-critical { background: rgba(255, 107, 107, 0.15); color: #ff6b6b; }
    .sparkline-container {
      height: 40px;
      padding: var(--nala-space-xs) 0;
    }
    .sparkline-container svg { width: 100%; height: 100%; }
    .sparkline-line { fill: none; stroke: var(--nala-primary); stroke-width: 1.5; }
    .detail-section {
      border-top: 1px solid var(--nala-border);
      margin-top: var(--nala-space-sm);
      padding-top: var(--nala-space-sm);
    }
    .detail-section-title {
      font-size: 0.8em;
      color: var(--nala-text-muted);
      margin-bottom: var(--nala-space-xs);
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }
  `;

  constructor() {
    super();
    this._nodes = [];
    this._links = [];
    this._nodeTypes = [];
    this._selected = null;
    this._loading = false;
    this._error = null;
    this._at = '';
    this._filterTypes = null;
    this._simulation = null;
    this._typeColorMap = {};
    this._sensorValue = null;
    this._sensorHistory = [];
    this._sensorInterval = null;
  }

  connectedCallback() {
    super.connectedCallback();
    this._loadGraph();
  }

  disconnectedCallback() {
    super.disconnectedCallback();
    if (this._simulation) this._simulation.stop();
    if (this._sensorInterval) clearInterval(this._sensorInterval);
  }

  async _loadGraph() {
    this._loading = true;
    this._error = null;
    try {
      const [nodesResp, edgesResp] = await Promise.all([
        fetchGraphNodes({ limit: 200, at: this._at || undefined }),
        fetchGraphEdges({ limit: 500, at: this._at || undefined }),
      ]);

      const nodeRows = nodesResp.rows || [];
      const edgeRows = edgesResp.rows || [];

      // Build node map — DESCRIBE returns all property key-value columns.
      const NODE_META_KEYS = new Set(['id', 'ID', 'type', 'Type', 'valid_from', 'ValidFrom', 'valid_to', 'ValidTo']);
      const nodeMap = new Map();
      const types = new Set();
      for (const r of nodeRows) {
        const id = r.id || r.ID;
        if (!id) continue;
        const type = r.type || r.Type || 'unknown';
        types.add(type);
        // Collect all extra columns as properties.
        const props = {};
        for (const [k, v] of Object.entries(r)) {
          if (!NODE_META_KEYS.has(k) && v !== '' && v != null) {
            props[k] = v;
          }
        }
        nodeMap.set(id, { id, type, props, validFrom: r.valid_from || r.ValidFrom, validTo: r.valid_to || r.ValidTo });
      }

      // Build links — DESCRIBE EDGES returns property key-value columns too.
      const EDGE_META_KEYS = new Set(['id', 'ID', 'from', 'From', 'source', 'to', 'To', 'target', 'relation', 'Relation', 'valid_from', 'ValidFrom', 'valid_to', 'ValidTo']);
      const links = [];
      for (const r of edgeRows) {
        const source = r.from || r.From || r.source;
        const target = r.to || r.To || r.target;
        const relation = r.relation || r.Relation || '';
        if (source && target && nodeMap.has(source) && nodeMap.has(target)) {
          const props = {};
          for (const [k, v] of Object.entries(r)) {
            if (!EDGE_META_KEYS.has(k) && v !== '' && v != null) {
              props[k] = v;
            }
          }
          links.push({ source, target, relation, props, id: r.id || r.ID });
        }
      }

      // Assign colors to types.
      const typeArr = [...types].sort();
      this._typeColorMap = {};
      typeArr.forEach((t, i) => {
        this._typeColorMap[t] = TYPE_COLORS[i % TYPE_COLORS.length];
      });

      this._nodes = [...nodeMap.values()];
      this._links = links;
      this._nodeTypes = typeArr;
    } catch (err) {
      this._error = err.message;
    } finally {
      this._loading = false;
    }
    await this.updateComplete;
    this._initSimulation();
  }

  _getVisibleNodes() {
    if (!this._filterTypes) return this._nodes;
    return this._nodes.filter(n => this._filterTypes.includes(n.type));
  }

  _getVisibleLinks() {
    const visible = new Set(this._getVisibleNodes().map(n => n.id));
    return this._links.filter(l => {
      const src = typeof l.source === 'object' ? l.source.id : l.source;
      const tgt = typeof l.target === 'object' ? l.target.id : l.target;
      return visible.has(src) && visible.has(tgt);
    });
  }

  _initSimulation() {
    const svg = this.renderRoot.querySelector('svg');
    if (!svg) return;

    const rect = svg.getBoundingClientRect();
    const w = rect.width || 800;
    const h = rect.height || 600;

    if (this._simulation) this._simulation.stop();

    const nodes = this._getVisibleNodes().map(n => ({ ...n }));
    const links = this._getVisibleLinks().map(l => ({
      ...l,
      source: typeof l.source === 'object' ? l.source.id : l.source,
      target: typeof l.target === 'object' ? l.target.id : l.target,
    }));

    const svgSel = d3Selection.select(svg);
    svgSel.selectAll('*').remove();

    // Arrow marker.
    svgSel.append('defs').append('marker')
      .attr('id', 'arrow')
      .attr('viewBox', '0 0 10 10')
      .attr('refX', 20).attr('refY', 5)
      .attr('markerWidth', 6).attr('markerHeight', 6)
      .attr('orient', 'auto')
      .append('path').attr('d', 'M0,0 L10,5 L0,10 Z');

    const g = svgSel.append('g');

    // Zoom.
    const zoom = d3Zoom.zoom()
      .scaleExtent([0.1, 8])
      .on('zoom', (event) => g.attr('transform', event.transform));
    svgSel.call(zoom);

    // Links.
    const link = g.append('g')
      .selectAll('line')
      .data(links)
      .join('line')
      .attr('class', 'link-line')
      .attr('marker-end', 'url(#arrow)')
      .style('cursor', 'pointer')
      .on('click', (event, d) => {
        if (this._sensorInterval) {
          clearInterval(this._sensorInterval);
          this._sensorInterval = null;
        }
        this._sensorValue = null;
        this._sensorHistory = [];
        this._selected = { ...d, _kind: 'edge' };
      });

    // Edge labels.
    const edgeLabel = g.append('g')
      .selectAll('text')
      .data(links)
      .join('text')
      .attr('class', 'node-label')
      .attr('font-size', '7px')
      .text(d => d.relation);

    // Nodes.
    const node = g.append('g')
      .selectAll('circle')
      .data(nodes)
      .join('circle')
      .attr('class', 'node-circle')
      .attr('r', 8)
      .attr('fill', d => this._typeColorMap[d.type] || '#888')
      .on('click', (event, d) => {
        this._selectNode(d);
      });

    // Labels — prefer a readable property (name, machineId, sensorId) over raw ID.
    const label = g.append('g')
      .selectAll('text')
      .data(nodes)
      .join('text')
      .attr('class', 'node-label')
      .attr('dy', '-0.8em')
      .text(d => {
        const p = d.props || {};
        const display = p.name || p.machineId || p.sensorId || p.zoneId || d.id;
        return display.length > 16 ? display.slice(0, 16) + '\u2026' : display;
      });

    // Drag.
    const drag = d3Drag.drag()
      .on('start', (event, d) => {
        if (!event.active) this._simulation.alphaTarget(0.3).restart();
        d.fx = d.x; d.fy = d.y;
      })
      .on('drag', (event, d) => {
        d.fx = event.x; d.fy = event.y;
      })
      .on('end', (event, d) => {
        if (!event.active) this._simulation.alphaTarget(0);
        d.fx = null; d.fy = null;
      });
    node.call(drag);

    // Force simulation.
    this._simulation = d3Force.forceSimulation(nodes)
      .force('link', d3Force.forceLink(links).id(d => d.id).distance(60))
      .force('charge', d3Force.forceManyBody().strength(-150))
      .force('center', d3Force.forceCenter(w / 2, h / 2))
      .force('collide', d3Force.forceCollide(15))
      .on('tick', () => {
        link
          .attr('x1', d => d.source.x).attr('y1', d => d.source.y)
          .attr('x2', d => d.target.x).attr('y2', d => d.target.y);
        node.attr('cx', d => d.x).attr('cy', d => d.y);
        label.attr('x', d => d.x).attr('y', d => d.y);
        edgeLabel
          .attr('x', d => (d.source.x + d.target.x) / 2)
          .attr('y', d => (d.source.y + d.target.y) / 2 - 4);
      });
  }

  render() {
    return html`
      <div class="sidebar">
        <div class="time-row">
          <div class="time-label">Time Travel</div>
          <nala-time-picker @value-changed=${this._onTimeChange}></nala-time-picker>
        </div>
        <nala-graph-controls
          .nodeTypes=${this._nodeTypes}
          @filter-changed=${this._onFilterChanged}
          @traverse=${this._onTraverse}
          @refresh=${() => this._loadGraph()}
        ></nala-graph-controls>
        ${this._selected ? this._renderDetail() : ''}
      </div>
      <div class="graph-area">
        ${this._error ? html`<div class="error">${this._error}</div>` : ''}
        <svg></svg>
        ${this._loading ? html`<div class="loading-overlay">Loading graph...</div>` : ''}
      </div>
    `;
  }

  _renderDetail() {
    const s = this._selected;
    if (s._kind === 'edge') {
      return this._renderEdgeDetail(s);
    }
    return this._renderNodeDetail(s);
  }

  _renderNodeDetail(n) {
    const props = n.props || {};
    const readingKey = props.readingKey;

    // Filter out readingKey from displayed props (it's shown in the live section)
    const displayProps = Object.entries(props).filter(([k]) => k !== 'readingKey');

    return html`
      <div class="detail-panel">
        <h3>${n.id}</h3>
        <div class="detail-row">
          <span class="detail-label">Type</span>
          <span class="detail-value">${n.type}</span>
        </div>
        ${displayProps.map(([k, v]) => html`
          <div class="detail-row">
            <span class="detail-label">${k}</span>
            <span class="detail-value">${v}</span>
          </div>
        `)}
        ${readingKey ? this._renderSensorLive(n, readingKey) : ''}
        <div class="detail-row">
          <span class="detail-label">Valid From</span>
          <span class="detail-value">${formatHLC(n.validFrom)}</span>
        </div>
        <div class="detail-row">
          <span class="detail-label">Valid To</span>
          <span class="detail-value">${formatHLC(n.validTo)}</span>
        </div>
      </div>
    `;
  }

  _renderEdgeDetail(e) {
    const props = e.props || {};
    const src = typeof e.source === 'object' ? e.source.id : e.source;
    const tgt = typeof e.target === 'object' ? e.target.id : e.target;
    return html`
      <div class="detail-panel">
        <h3>${e.relation}</h3>
        <div class="detail-row">
          <span class="detail-label">From</span>
          <span class="detail-value">${src}</span>
        </div>
        <div class="detail-row">
          <span class="detail-label">To</span>
          <span class="detail-value">${tgt}</span>
        </div>
        ${Object.entries(props).map(([k, v]) => html`
          <div class="detail-row">
            <span class="detail-label">${k}</span>
            <span class="detail-value">${v}</span>
          </div>
        `)}
      </div>
    `;
  }

  _renderSensorLive(n, readingKey) {
    const props = n.props || {};
    const value = this._sensorValue;
    const history = this._sensorHistory;
    const unit = props.unit || '';

    let statusClass = 'status-normal';
    let statusLabel = 'Normal';
    if (value != null) {
      const v = parseFloat(value);
      const critHigh = props.critHigh ? parseFloat(props.critHigh) : null;
      const warnHigh = props.warnHigh ? parseFloat(props.warnHigh) : null;
      const critLow = props.critLow ? parseFloat(props.critLow) : null;
      const warnLow = props.warnLow ? parseFloat(props.warnLow) : null;

      if ((critHigh != null && v >= critHigh) || (critLow != null && v <= critLow)) {
        statusClass = 'status-critical';
        statusLabel = 'Critical';
      } else if ((warnHigh != null && v >= warnHigh) || (warnLow != null && v <= warnLow)) {
        statusClass = 'status-warning';
        statusLabel = 'Warning';
      }
    }

    return html`
      <div class="detail-section">
        <div class="detail-section-title">Live Reading</div>
        ${value != null ? html`
          <div class="sensor-value">${parseFloat(value).toFixed(2)} ${unit}</div>
          <div style="text-align:center">
            <span class="sensor-status ${statusClass}">${statusLabel}</span>
          </div>
        ` : html`<div style="color:var(--nala-text-muted);text-align:center">No data</div>`}
        ${history.length > 1 ? this._renderSparkline(history) : ''}
        <div class="detail-row" style="margin-top:var(--nala-space-xs)">
          <span class="detail-label">Key</span>
          <span class="detail-value" style="font-size:0.75em">${readingKey}</span>
        </div>
      </div>
    `;
  }

  _renderSparkline(history) {
    const values = history.map(r => parseFloat(r.value)).filter(v => !isNaN(v));
    if (values.length < 2) return '';

    const w = 200, h = 36, pad = 2;
    const min = Math.min(...values);
    const max = Math.max(...values);
    const range = max - min || 1;

    const points = values.map((v, i) => {
      const x = pad + (i / (values.length - 1)) * (w - 2 * pad);
      const y = h - pad - ((v - min) / range) * (h - 2 * pad);
      return `${x},${y}`;
    }).join(' ');

    return html`
      <div class="sparkline-container">
        <svg viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">
          <polyline class="sparkline-line" points="${points}"/>
        </svg>
      </div>
    `;
  }

  async _selectNode(d) {
    // Clear previous sensor polling
    if (this._sensorInterval) {
      clearInterval(this._sensorInterval);
      this._sensorInterval = null;
    }
    this._sensorValue = null;
    this._sensorHistory = [];
    this._selected = d;

    const readingKey = d.props?.readingKey;
    if (readingKey) {
      await this._fetchSensorData(readingKey);
      // Poll every 2 seconds
      this._sensorInterval = setInterval(() => this._fetchSensorData(readingKey), 2000);
    }
  }

  async _fetchSensorData(key) {
    try {
      const [latest, history] = await Promise.all([
        fetchLatestValue(key),
        fetchSensorHistory(key, 30),
      ]);
      if (latest) {
        this._sensorValue = latest.value;
      }
      this._sensorHistory = history;
    } catch (e) {
      // Silently ignore fetch errors for live updates
    }
  }

  _onTimeChange(e) {
    this._at = e.detail.value;
    this._loadGraph();
  }

  _onFilterChanged(e) {
    this._filterTypes = e.detail.types;
    this._initSimulation();
  }

  async _onTraverse(e) {
    const { start, depth, direction } = e.detail;
    this._loading = true;
    this._error = null;
    try {
      const resp = await fetchGraphTraverse({
        start,
        depth,
        direction,
        at: this._at || undefined,
      });
      // Build nodes from traversal results.
      const nodeIds = new Set([start]);
      const links = [];
      for (const r of (resp.results || [])) {
        nodeIds.add(r.node_id);
        if (r.via_edge) {
          links.push({ source: start, target: r.node_id, relation: r.via_relation, id: r.via_edge });
        }
      }
      this._nodes = [...nodeIds].map(id => ({
        id,
        type: id === start ? 'start' : 'traversed',
      }));
      this._links = links;
      this._typeColorMap = { start: '#6c63ff', traversed: '#00d4aa' };
      this._nodeTypes = ['start', 'traversed'];
    } catch (err) {
      this._error = err.message;
    } finally {
      this._loading = false;
    }
    await this.updateComplete;
    this._initSimulation();
  }
}

customElements.define('nala-graph-explorer', NalaGraphExplorer);
