import React, { useState, useEffect, useRef, useCallback } from 'react';
import { api } from '../api.js';

// ── Constants ──────────────────────────────────────────────────────────

const COLORS = {
  bg: '#080C10',
  panel: '#0D1520',
  border: '#1A2840',
  text: '#C8D8E8',
  textDim: '#5A7080',
  accent: '#1A6EBF',
  red: '#D94040',
  green: '#2ECC71',
  yellow: '#F1C40F',
  orange: '#E67E22',
  purple: '#9B59B6',
};

const NODE_COLORS = {
  company: '#1A6EBF',
  member: '#9B59B6',
  bill: '#2ECC71',
  agency: '#E67E22',
};

const LINK_COLORS = {
  contribution: '#9B59B6',
  trade: '#D94040',
  lobbying: '#F1C40F',
  contract: '#2ECC71',
  vote: '#5DADE2',
};

const MONO = "'JetBrains Mono', monospace";
const SANS = "'IBM Plex Sans', sans-serif";

// ── Formatting ─────────────────────────────────────────────────────────

function fmtMoney(n) {
  if (!n || n === 0) return '$0';
  if (n >= 1e9) return `$${(n / 1e9).toFixed(1)}B`;
  if (n >= 1e6) return `$${(n / 1e6).toFixed(1)}M`;
  if (n >= 1e3) return `$${(n / 1e3).toFixed(0)}K`;
  return `$${n.toFixed(0)}`;
}

function suspicionColor(score) {
  if (score >= 0.7) return COLORS.red;
  if (score >= 0.4) return COLORS.orange;
  if (score >= 0.2) return COLORS.yellow;
  return COLORS.textDim;
}

// ── Simple Force Layout (no d3 dependency) ─────────────────────────────

function useForceLayout(nodes, links, width, height) {
  const [positions, setPositions] = useState({});
  const frameRef = useRef(null);
  const posRef = useRef({});

  useEffect(() => {
    if (!nodes.length) return;

    // Initialize positions: companies left, members middle, bills right
    const pos = {};
    const companies = nodes.filter(n => n.type === 'company');
    const members = nodes.filter(n => n.type === 'member');
    const bills = nodes.filter(n => n.type === 'bill');
    const agencies = nodes.filter(n => n.type === 'agency');

    const colX = { company: width * 0.15, member: width * 0.5, bill: width * 0.78, agency: width * 0.92 };

    [companies, members, bills, agencies].forEach((group, gi) => {
      const col = ['company', 'member', 'bill', 'agency'][gi];
      const x = colX[col];
      group.forEach((n, i) => {
        const spacing = Math.min(40, (height - 80) / Math.max(group.length, 1));
        const startY = Math.max(40, (height - group.length * spacing) / 2);
        pos[n.id] = {
          x: x + (Math.random() - 0.5) * 30,
          y: startY + i * spacing,
          vx: 0,
          vy: 0,
        };
      });
    });

    posRef.current = pos;

    // Build link lookup
    const linkMap = links.map(l => ({
      source: l.source,
      target: l.target,
    }));

    let iteration = 0;
    const maxIterations = 120;

    function tick() {
      const p = posRef.current;
      const alpha = Math.max(0.001, 1 - iteration / maxIterations);

      // Repulsion between all nodes
      const nodeIds = Object.keys(p);
      for (let i = 0; i < nodeIds.length; i++) {
        for (let j = i + 1; j < nodeIds.length; j++) {
          const a = p[nodeIds[i]];
          const b = p[nodeIds[j]];
          if (!a || !b) continue;
          let dx = b.x - a.x;
          let dy = b.y - a.y;
          const dist = Math.sqrt(dx * dx + dy * dy) || 1;
          const force = (300 * alpha) / (dist * dist);
          dx *= force;
          dy *= force;
          a.vx -= dx;
          a.vy -= dy;
          b.vx += dx;
          b.vy += dy;
        }
      }

      // Attraction along links
      for (const link of linkMap) {
        const a = p[link.source];
        const b = p[link.target];
        if (!a || !b) continue;
        const dx = b.x - a.x;
        const dy = b.y - a.y;
        const dist = Math.sqrt(dx * dx + dy * dy) || 1;
        const force = (dist - 150) * 0.005 * alpha;
        const fx = dx / dist * force;
        const fy = dy / dist * force;
        a.vx += fx;
        a.vy += fy;
        b.vx -= fx;
        b.vy -= fy;
      }

      // Column gravity (keep nodes in their lanes)
      for (const n of nodes) {
        const pp = p[n.id];
        if (!pp) continue;
        const targetX = colX[n.type] || width / 2;
        pp.vx += (targetX - pp.x) * 0.02 * alpha;
      }

      // Apply velocity with damping and bounds
      for (const id of nodeIds) {
        const pp = p[id];
        if (!pp) continue;
        pp.vx *= 0.7;
        pp.vy *= 0.7;
        pp.x = Math.max(20, Math.min(width - 20, pp.x + pp.vx));
        pp.y = Math.max(20, Math.min(height - 20, pp.y + pp.vy));
      }

      iteration++;
      setPositions({ ...p });

      if (iteration < maxIterations) {
        frameRef.current = requestAnimationFrame(tick);
      }
    }

    frameRef.current = requestAnimationFrame(tick);

    return () => {
      if (frameRef.current) cancelAnimationFrame(frameRef.current);
    };
  }, [nodes, links, width, height]);

  return positions;
}

// ── Graph SVG Component ────────────────────────────────────────────────

function InfluenceGraph({ data, width, height, onSelectNode }) {
  const nodes = data.nodes || [];
  const links = data.links || [];
  const positions = useForceLayout(nodes, links, width, height);
  const [hovered, setHovered] = useState(null);

  if (!nodes.length) {
    return (
      <div style={{ padding: 40, textAlign: 'center', color: COLORS.textDim, fontFamily: MONO, fontSize: 13 }}>
        No influence data available. Run data ingestion for lobbying, campaign finance, and congressional trades.
      </div>
    );
  }

  return (
    <svg width={width} height={height} style={{ background: COLORS.bg }}>
      {/* Column labels */}
      <text x={width * 0.15} y={18} textAnchor="middle" fill={COLORS.textDim} fontSize={11} fontFamily={MONO}>COMPANIES</text>
      <text x={width * 0.5} y={18} textAnchor="middle" fill={COLORS.textDim} fontSize={11} fontFamily={MONO}>MEMBERS</text>
      <text x={width * 0.78} y={18} textAnchor="middle" fill={COLORS.textDim} fontSize={11} fontFamily={MONO}>BILLS</text>
      <text x={width * 0.92} y={18} textAnchor="middle" fill={COLORS.textDim} fontSize={11} fontFamily={MONO}>AGENCIES</text>

      {/* Links */}
      {links.map((link, i) => {
        const s = positions[link.source];
        const t = positions[link.target];
        if (!s || !t) return null;
        const color = LINK_COLORS[link.type] || COLORS.textDim;
        const isHovered = hovered === link.source || hovered === link.target;
        return (
          <line
            key={i}
            x1={s.x} y1={s.y}
            x2={t.x} y2={t.y}
            stroke={color}
            strokeWidth={isHovered ? 2 : 0.8}
            strokeOpacity={isHovered ? 0.9 : 0.25}
          />
        );
      })}

      {/* Nodes */}
      {nodes.map((node) => {
        const p = positions[node.id];
        if (!p) return null;
        const color = NODE_COLORS[node.type] || COLORS.textDim;
        const r = node.type === 'company' ? 8 : node.type === 'member' ? 6 : 5;
        const isHovered = hovered === node.id;
        return (
          <g
            key={node.id}
            onMouseEnter={() => setHovered(node.id)}
            onMouseLeave={() => setHovered(null)}
            onClick={() => onSelectNode?.(node)}
            style={{ cursor: 'pointer' }}
          >
            <circle
              cx={p.x} cy={p.y} r={isHovered ? r + 3 : r}
              fill={color}
              fillOpacity={isHovered ? 1 : 0.8}
              stroke={isHovered ? '#fff' : 'none'}
              strokeWidth={1.5}
            />
            {(isHovered || node.type === 'company') && (
              <text
                x={p.x} y={p.y - r - 4}
                textAnchor="middle"
                fill={COLORS.text}
                fontSize={isHovered ? 11 : 9}
                fontFamily={MONO}
              >
                {node.ticker || node.label}
              </text>
            )}
          </g>
        );
      })}
    </svg>
  );
}

// ── Circular Flow Card ─────────────────────────────────────────────────

function LoopCard({ loop }) {
  const [expanded, setExpanded] = useState(false);
  return (
    <div
      style={{
        background: COLORS.panel,
        border: `1px solid ${COLORS.border}`,
        borderRadius: 8,
        padding: '12px 16px',
        marginBottom: 8,
        cursor: 'pointer',
        borderLeft: `3px solid ${suspicionColor(loop.suspicion_score)}`,
      }}
      onClick={() => setExpanded(!expanded)}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <div>
          <span style={{ fontFamily: MONO, fontSize: 14, color: COLORS.text, fontWeight: 600 }}>
            {loop.ticker}
          </span>
          <span style={{ fontFamily: SANS, fontSize: 13, color: COLORS.textDim, marginLeft: 8 }}>
            {loop.company}
          </span>
          {loop.circular_flow_detected && (
            <span style={{
              marginLeft: 8, padding: '2px 6px', borderRadius: 4,
              background: 'rgba(217, 64, 64, 0.2)', color: COLORS.red,
              fontFamily: MONO, fontSize: 10,
            }}>
              CIRCULAR
            </span>
          )}
        </div>
        <span style={{ fontFamily: MONO, fontSize: 13, color: suspicionColor(loop.suspicion_score) }}>
          {(loop.suspicion_score * 100).toFixed(0)}%
        </span>
      </div>

      <div style={{ display: 'flex', gap: 16, marginTop: 8, fontFamily: MONO, fontSize: 11, color: COLORS.textDim }}>
        <span>Lobby: {fmtMoney(loop.lobbying_spend)}</span>
        <span>PAC: {fmtMoney(loop.pac_contributions)}</span>
        <span>Contracts: {fmtMoney(loop.contracts_received)}</span>
        <span>Trades: {loop.member_trades?.length || 0}</span>
      </div>

      {expanded && (
        <div style={{ marginTop: 12, fontSize: 12, fontFamily: SANS }}>
          {loop.recipients?.length > 0 && (
            <div style={{ marginBottom: 8 }}>
              <div style={{ color: COLORS.accent, fontFamily: MONO, fontSize: 10, marginBottom: 4 }}>PAC RECIPIENTS</div>
              {loop.recipients.slice(0, 5).map((r, i) => (
                <div key={i} style={{ color: COLORS.textDim, fontSize: 11 }}>
                  {r.member} - {fmtMoney(r.amount)} ({r.committee})
                </div>
              ))}
            </div>
          )}
          {loop.member_trades?.length > 0 && (
            <div style={{ marginBottom: 8 }}>
              <div style={{ color: COLORS.red, fontFamily: MONO, fontSize: 10, marginBottom: 4 }}>MEMBER TRADES</div>
              {loop.member_trades.slice(0, 5).map((t, i) => (
                <div key={i} style={{ color: COLORS.textDim, fontSize: 11 }}>
                  {t.member} {t.action} {t.ticker} ({t.amount}) - {t.date}
                </div>
              ))}
            </div>
          )}
          {loop.legislation_affected?.length > 0 && (
            <div>
              <div style={{ color: COLORS.green, fontFamily: MONO, fontSize: 10, marginBottom: 4 }}>LEGISLATION</div>
              {loop.legislation_affected.slice(0, 5).map((l, i) => (
                <div key={i} style={{ color: COLORS.textDim, fontSize: 11 }}>
                  {l.bill} - {l.member_vote} ({l.company_impact})
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ── Hypocrisy Card ─────────────────────────────────────────────────────

function HypocrisyCard({ flag }) {
  return (
    <div style={{
      background: COLORS.panel,
      border: `1px solid ${COLORS.border}`,
      borderRadius: 8,
      padding: '10px 14px',
      marginBottom: 6,
      borderLeft: `3px solid ${COLORS.red}`,
    }}>
      <div style={{ display: 'flex', justifyContent: 'space-between' }}>
        <span style={{ fontFamily: MONO, fontSize: 13, color: COLORS.text }}>
          {flag.member}
        </span>
        <span style={{ fontFamily: MONO, fontSize: 11, color: COLORS.textDim }}>
          {flag.party}-{flag.state}
        </span>
      </div>
      <div style={{ fontSize: 11, fontFamily: SANS, color: COLORS.textDim, marginTop: 4 }}>
        Voted <span style={{ color: COLORS.yellow }}>{flag.vote}</span> on{' '}
        <span style={{ color: COLORS.green }}>{flag.bill_id}</span> but{' '}
        <span style={{ color: flag.trade_action === 'BUY' ? COLORS.green : COLORS.red }}>
          {flag.trade_action}
        </span>{' '}
        <span style={{ color: COLORS.accent }}>{flag.ticker}</span>
      </div>
      <div style={{ fontSize: 10, fontFamily: MONO, color: COLORS.textDim, marginTop: 4 }}>
        {flag.hypocrisy_type?.replace(/_/g, ' ')} | {flag.trade_date}
      </div>
    </div>
  );
}

// ── Node Detail Panel ──────────────────────────────────────────────────

function NodeDetail({ node, influenceData, onClose }) {
  if (!node) return null;

  return (
    <div style={{
      position: 'absolute', top: 0, right: 0, width: 360,
      height: '100%', background: COLORS.panel, borderLeft: `1px solid ${COLORS.border}`,
      padding: 16, overflowY: 'auto', zIndex: 10,
    }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
        <span style={{ fontFamily: MONO, fontSize: 16, color: COLORS.text, fontWeight: 600 }}>
          {node.ticker || node.label}
        </span>
        <span
          onClick={onClose}
          style={{ cursor: 'pointer', fontFamily: MONO, fontSize: 14, color: COLORS.textDim }}
        >
          X
        </span>
      </div>

      <div style={{ fontFamily: SANS, fontSize: 12, color: COLORS.textDim, marginBottom: 12 }}>
        {node.label} ({node.type})
      </div>

      {influenceData && (
        <>
          <div style={{ marginBottom: 16 }}>
            <div style={{ fontFamily: MONO, fontSize: 10, color: COLORS.accent, marginBottom: 6 }}>MONEY SUMMARY</div>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
              {[
                ['Lobbying', influenceData.lobbying?.total_spend],
                ['PAC', influenceData.pac_contributions?.total],
                ['Contracts', influenceData.contracts?.total_value],
                ['Total', influenceData.suspicion_summary?.total_money_in_system],
              ].map(([label, val]) => (
                <div key={label} style={{ background: COLORS.bg, padding: 8, borderRadius: 6 }}>
                  <div style={{ fontFamily: MONO, fontSize: 10, color: COLORS.textDim }}>{label}</div>
                  <div style={{ fontFamily: MONO, fontSize: 14, color: COLORS.text }}>{fmtMoney(val)}</div>
                </div>
              ))}
            </div>
          </div>

          {influenceData.circular_flow?.detected && (
            <div style={{
              background: 'rgba(217, 64, 64, 0.1)',
              border: `1px solid ${COLORS.red}`,
              borderRadius: 6, padding: 10, marginBottom: 12,
            }}>
              <div style={{ fontFamily: MONO, fontSize: 10, color: COLORS.red, marginBottom: 4 }}>
                CIRCULAR FLOW DETECTED
              </div>
              <div style={{ fontFamily: SANS, fontSize: 11, color: COLORS.textDim }}>
                {influenceData.circular_flow.overlap_members?.length || 0} members received PAC money AND traded this stock
              </div>
            </div>
          )}

          {influenceData.member_trades?.length > 0 && (
            <div style={{ marginBottom: 12 }}>
              <div style={{ fontFamily: MONO, fontSize: 10, color: COLORS.red, marginBottom: 6 }}>
                MEMBER TRADES ({influenceData.member_trades.length})
              </div>
              {influenceData.member_trades.slice(0, 8).map((t, i) => (
                <div key={i} style={{ fontSize: 11, fontFamily: MONO, color: COLORS.textDim, marginBottom: 2 }}>
                  {t.member} {t.action} {t.amount} ({t.date})
                </div>
              ))}
            </div>
          )}

          {influenceData.pac_contributions?.recipients?.length > 0 && (
            <div>
              <div style={{ fontFamily: MONO, fontSize: 10, color: COLORS.purple, marginBottom: 6 }}>
                PAC RECIPIENTS ({influenceData.pac_contributions.recipients.length})
              </div>
              {influenceData.pac_contributions.recipients.slice(0, 8).map((r, i) => (
                <div key={i} style={{ fontSize: 11, fontFamily: MONO, color: COLORS.textDim, marginBottom: 2 }}>
                  {r.member} - {fmtMoney(r.amount)}
                </div>
              ))}
            </div>
          )}
        </>
      )}
    </div>
  );
}

// ── Legend ──────────────────────────────────────────────────────────────

function Legend() {
  return (
    <div style={{
      display: 'flex', gap: 16, padding: '8px 16px',
      background: COLORS.panel, borderRadius: 8,
      border: `1px solid ${COLORS.border}`,
      fontFamily: MONO, fontSize: 10,
      flexWrap: 'wrap',
    }}>
      <span style={{ color: COLORS.textDim }}>NODES:</span>
      {Object.entries(NODE_COLORS).map(([type, color]) => (
        <span key={type} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
          <span style={{ width: 8, height: 8, borderRadius: '50%', background: color, display: 'inline-block' }} />
          <span style={{ color: COLORS.textDim }}>{type}</span>
        </span>
      ))}
      <span style={{ color: COLORS.textDim, marginLeft: 8 }}>LINKS:</span>
      {Object.entries(LINK_COLORS).map(([type, color]) => (
        <span key={type} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
          <span style={{ width: 12, height: 2, background: color, display: 'inline-block' }} />
          <span style={{ color: COLORS.textDim }}>{type}</span>
        </span>
      ))}
    </div>
  );
}

// ── Main View ──────────────────────────────────────────────────────────

export default function InfluenceNetwork() {
  const [graphData, setGraphData] = useState({ nodes: [], links: [], metadata: {} });
  const [loops, setLoops] = useState([]);
  const [hypocrisy, setHypocrisy] = useState([]);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState('graph');
  const [selectedNode, setSelectedNode] = useState(null);
  const [nodeDetail, setNodeDetail] = useState(null);
  const containerRef = useRef(null);
  const [dims, setDims] = useState({ w: 900, h: 600 });

  // Resize observer
  useEffect(() => {
    if (!containerRef.current) return;
    const ro = new ResizeObserver(entries => {
      for (const entry of entries) {
        setDims({ w: entry.contentRect.width, h: Math.max(500, entry.contentRect.height - 120) });
      }
    });
    ro.observe(containerRef.current);
    return () => ro.disconnect();
  }, []);

  // Fetch data
  useEffect(() => {
    async function load() {
      setLoading(true);
      try {
        const [graphRes, loopsRes, hypocrisyRes] = await Promise.all([
          api.get('/api/v1/intelligence/influence').catch(() => ({ nodes: [], links: [], metadata: {} })),
          api.get('/api/v1/intelligence/influence/circular-flows').catch(() => ({ loops: [] })),
          api.get('/api/v1/intelligence/influence/hypocrisy').catch(() => ({ flags: [] })),
        ]);
        setGraphData(graphRes);
        setLoops(loopsRes.loops || []);
        setHypocrisy(hypocrisyRes.flags || []);
      } catch (err) {
        console.error('Influence network load failed:', err);
      }
      setLoading(false);
    }
    load();
  }, []);

  // Load detail for selected company node
  const handleSelectNode = useCallback(async (node) => {
    setSelectedNode(node);
    if (node.type === 'company' && node.ticker) {
      try {
        const detail = await api.get(`/api/v1/intelligence/influence?ticker=${node.ticker}`);
        setNodeDetail(detail);
      } catch {
        setNodeDetail(null);
      }
    } else {
      setNodeDetail(null);
    }
  }, []);

  const tabs = [
    { id: 'graph', label: 'INFLUENCE GRAPH' },
    { id: 'loops', label: `CIRCULAR FLOWS (${loops.length})` },
    { id: 'hypocrisy', label: `HYPOCRISY (${hypocrisy.length})` },
  ];

  return (
    <div ref={containerRef} style={{ padding: '16px 20px', height: '100%', fontFamily: SANS }}>
      {/* Header */}
      <div style={{ marginBottom: 12 }}>
        <h2 style={{ fontFamily: MONO, fontSize: 18, color: COLORS.text, margin: 0 }}>
          Influence Network
        </h2>
        <p style={{ fontFamily: SANS, fontSize: 12, color: COLORS.textDim, margin: '4px 0 0' }}>
          Money-in-politics graph: Company --lobbies--&gt; Member --votes--&gt; Bill --funds--&gt; Company
        </p>
      </div>

      {/* Stats bar */}
      {graphData.metadata && (
        <div style={{
          display: 'flex', gap: 16, marginBottom: 12,
          fontFamily: MONO, fontSize: 11, color: COLORS.textDim,
        }}>
          <span>{graphData.metadata.companies_with_data || 0} companies</span>
          <span>{graphData.metadata.total_nodes || 0} nodes</span>
          <span>{graphData.metadata.total_links || 0} links</span>
          <span>Lobbying: {fmtMoney(graphData.metadata.total_lobbying)}</span>
          <span>PAC: {fmtMoney(graphData.metadata.total_pac)}</span>
          <span>Contracts: {fmtMoney(graphData.metadata.total_contracts)}</span>
        </div>
      )}

      {/* Tab bar */}
      <div style={{ display: 'flex', gap: 2, marginBottom: 12 }}>
        {tabs.map(tab => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            style={{
              background: activeTab === tab.id ? COLORS.accent : COLORS.panel,
              color: activeTab === tab.id ? '#fff' : COLORS.textDim,
              border: `1px solid ${COLORS.border}`,
              borderRadius: 6,
              padding: '6px 14px',
              fontFamily: MONO,
              fontSize: 11,
              cursor: 'pointer',
            }}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {loading ? (
        <div style={{ padding: 60, textAlign: 'center', color: COLORS.textDim, fontFamily: MONO, fontSize: 13 }}>
          Loading influence network...
        </div>
      ) : (
        <>
          {activeTab === 'graph' && (
            <div style={{ position: 'relative' }}>
              <Legend />
              <div style={{
                marginTop: 8, border: `1px solid ${COLORS.border}`,
                borderRadius: 8, overflow: 'hidden',
              }}>
                <InfluenceGraph
                  data={graphData}
                  width={dims.w - (selectedNode ? 360 : 0)}
                  height={dims.h}
                  onSelectNode={handleSelectNode}
                />
              </div>
              <NodeDetail
                node={selectedNode}
                influenceData={nodeDetail}
                onClose={() => { setSelectedNode(null); setNodeDetail(null); }}
              />
            </div>
          )}

          {activeTab === 'loops' && (
            <div style={{ maxHeight: dims.h, overflowY: 'auto' }}>
              {loops.length === 0 ? (
                <div style={{ padding: 40, textAlign: 'center', color: COLORS.textDim, fontFamily: MONO, fontSize: 12 }}>
                  No circular flows detected. Ingest lobbying, campaign finance, and contract data to enable detection.
                </div>
              ) : (
                loops.map((loop, i) => <LoopCard key={i} loop={loop} />)
              )}
            </div>
          )}

          {activeTab === 'hypocrisy' && (
            <div style={{ maxHeight: dims.h, overflowY: 'auto' }}>
              {hypocrisy.length === 0 ? (
                <div style={{ padding: 40, textAlign: 'center', color: COLORS.textDim, fontFamily: MONO, fontSize: 12 }}>
                  No vote/trade hypocrisy detected. Ingest congressional trade and legislation data.
                </div>
              ) : (
                hypocrisy.map((flag, i) => <HypocrisyCard key={i} flag={flag} />)
              )}
            </div>
          )}
        </>
      )}
    </div>
  );
}
