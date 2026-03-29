/**
 * ActorUniverse -- 3D/4D interactive visualization of the financial power structure.
 *
 * Built with Three.js + CSS2DRenderer for labels.
 *
 * Features:
 *   - 3D force-directed layout with tier-based spatial clustering
 *   - InstancedMesh for high-perf rendering of 1000+ actors
 *   - Animated particle flows along connections (money in/out/influence)
 *   - 4th dimension: TIME -- scrub a slider to watch the network evolve
 *   - Orbit controls (rotate, zoom, pan)
 *   - Click actor sphere -> info panel
 *   - Hover -> highlight connections, dim everything else
 *   - Double-click -> zoom into local cluster
 *   - Search -> camera flies to actor
 *   - Filter by tier/category
 *   - CSS2D billboard labels (shown above threshold or when zoomed in)
 *   - LOD: far = dots, close = spheres + labels
 */
import React, { useEffect, useRef, useState, useCallback, useMemo } from 'react';
import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
import { CSS2DRenderer, CSS2DObject } from 'three/examples/jsm/renderers/CSS2DRenderer.js';
import { api } from '../api.js';
import { colors, tokens, shared } from '../styles/shared.js';
import useFullScreen from '../hooks/useFullScreen.js';

// ── Constants ──
const TIER_COLORS = {
    sovereign:     0xFFD700,
    regional:      0x3B82F6,
    institutional: 0x8B5CF6,
    individual:    0x06B6D4,
};
const TIER_HEX = {
    sovereign:     '#FFD700',
    regional:      '#3B82F6',
    institutional: '#8B5CF6',
    individual:    '#06B6D4',
};
const TIER_LABELS = {
    sovereign: 'Sovereign',
    regional: 'Regional',
    institutional: 'Institutional',
    individual: 'Individual',
};
const CATEGORY_LABELS = {
    central_bank: 'Central Banks',
    government: 'Politicians',
    fund: 'Funds',
    corporation: 'Corporations',
    insider: 'Insiders',
    politician: 'Politicians',
    activist: 'Activists',
    swf: 'SWFs',
};
const FLOW_COLORS_HEX = {
    campaign: '#22C55E', contribution: '#22C55E', contract: '#22C55E',
    lobbying: '#22C55E', investment: '#22C55E',
    stock_trade: '#EF4444', stock_sale: '#EF4444', sell: '#EF4444', outflow: '#EF4444',
    influence: '#FFD700', vote: '#FFD700', policy: '#FFD700', regulation: '#FFD700',
};
const TIER_SHELL_RADIUS = {
    sovereign: 0,
    regional: 60,
    institutional: 120,
    individual: 200,
};

const MONO = "'JetBrains Mono', monospace";
const SANS = "'IBM Plex Sans', sans-serif";

function formatMoney(val) {
    if (!val && val !== 0) return '--';
    const abs = Math.abs(val);
    if (abs >= 1e12) return `$${(val / 1e12).toFixed(1)}T`;
    if (abs >= 1e9) return `$${(val / 1e9).toFixed(1)}B`;
    if (abs >= 1e6) return `$${(val / 1e6).toFixed(0)}M`;
    if (abs >= 1e3) return `$${(val / 1e3).toFixed(0)}K`;
    return `$${val.toFixed(0)}`;
}

function parseFlowDate(dateStr) {
    if (!dateStr) return null;
    const d = new Date(dateStr);
    return isNaN(d.getTime()) ? null : d;
}

function getFlowColorHex(type) {
    return FLOW_COLORS_HEX[type] || '#22C55E';
}

// ── 3D force simulation (simple spring-based) ──
class ForceSimulation3D {
    constructor(nodes, links, options = {}) {
        this.nodes = nodes;
        this.links = links;
        this.alpha = 1;
        this.alphaDecay = options.alphaDecay || 0.02;
        this.velocityDecay = options.velocityDecay || 0.4;
        this.centerStrength = options.centerStrength || 0.01;
        this.chargeStrength = options.chargeStrength || -30;
        this.linkDistance = options.linkDistance || 40;
        this.linkStrength = options.linkStrength || 0.3;

        // Build link lookup
        this._linkIndex = {};
        this.links.forEach(l => {
            const sid = typeof l.source === 'object' ? l.source.id : l.source;
            const tid = typeof l.target === 'object' ? l.target.id : l.target;
            if (!this._linkIndex[sid]) this._linkIndex[sid] = [];
            if (!this._linkIndex[tid]) this._linkIndex[tid] = [];
            this._linkIndex[sid].push(l);
            this._linkIndex[tid].push(l);
        });

        // Build node lookup
        this._nodeMap = {};
        this.nodes.forEach(n => { this._nodeMap[n.id] = n; });

        // Resolve link source/target to node objects
        this.links.forEach(l => {
            if (typeof l.source === 'string') l.source = this._nodeMap[l.source] || l.source;
            if (typeof l.target === 'string') l.target = this._nodeMap[l.target] || l.target;
        });
    }

    tick() {
        if (this.alpha < 0.001) return;

        const nodes = this.nodes;
        const links = this.links;

        // Center force
        for (const n of nodes) {
            n.vx = (n.vx || 0) - n.x * this.centerStrength * this.alpha;
            n.vy = (n.vy || 0) - n.y * this.centerStrength * this.alpha;
            n.vz = (n.vz || 0) - n.z * this.centerStrength * this.alpha;
        }

        // Tier shell force -- push nodes toward their tier radius
        for (const n of nodes) {
            const targetR = TIER_SHELL_RADIUS[n.tier] || 150;
            const dist = Math.sqrt(n.x * n.x + n.y * n.y + n.z * n.z) || 1;
            const diff = (targetR - dist) * 0.005 * this.alpha;
            n.vx += (n.x / dist) * diff;
            n.vy += (n.y / dist) * diff;
            n.vz += (n.z / dist) * diff;
        }

        // Charge (repulsion) -- use Barnes-Hut approximation with grid
        // For performance, only compare within buckets + nearby
        const gridSize = 50;
        const grid = {};
        for (const n of nodes) {
            const gx = Math.floor(n.x / gridSize);
            const gy = Math.floor(n.y / gridSize);
            const gz = Math.floor(n.z / gridSize);
            const key = `${gx},${gy},${gz}`;
            if (!grid[key]) grid[key] = [];
            grid[key].push(n);
        }

        for (const n of nodes) {
            const gx = Math.floor(n.x / gridSize);
            const gy = Math.floor(n.y / gridSize);
            const gz = Math.floor(n.z / gridSize);

            for (let dx = -1; dx <= 1; dx++) {
                for (let dy = -1; dy <= 1; dy++) {
                    for (let dz = -1; dz <= 1; dz++) {
                        const key = `${gx + dx},${gy + dy},${gz + dz}`;
                        const bucket = grid[key];
                        if (!bucket) continue;
                        for (const m of bucket) {
                            if (m === n) continue;
                            let dx2 = n.x - m.x;
                            let dy2 = n.y - m.y;
                            let dz2 = n.z - m.z;
                            let dist2 = dx2 * dx2 + dy2 * dy2 + dz2 * dz2;
                            if (dist2 < 1) dist2 = 1;
                            const force = this.chargeStrength * this.alpha / dist2;
                            const dist = Math.sqrt(dist2);
                            n.vx -= (dx2 / dist) * force;
                            n.vy -= (dy2 / dist) * force;
                            n.vz -= (dz2 / dist) * force;
                        }
                    }
                }
            }
        }

        // Link spring force
        for (const l of links) {
            const s = l.source;
            const t = l.target;
            if (!s || !t || typeof s !== 'object' || typeof t !== 'object') continue;
            let dx = t.x - s.x;
            let dy = t.y - s.y;
            let dz = t.z - s.z;
            let dist = Math.sqrt(dx * dx + dy * dy + dz * dz) || 1;
            const targetDist = this.linkDistance;
            const strength = (l.strength || this.linkStrength) * this.alpha;
            const diff = (dist - targetDist) / dist * strength * 0.5;
            dx *= diff; dy *= diff; dz *= diff;
            s.vx += dx; s.vy += dy; s.vz += dz;
            t.vx -= dx; t.vy -= dy; t.vz -= dz;
        }

        // Velocity decay + position update
        for (const n of nodes) {
            n.vx *= (1 - this.velocityDecay);
            n.vy *= (1 - this.velocityDecay);
            n.vz *= (1 - this.velocityDecay);
            n.x += n.vx;
            n.y += n.vy;
            n.z += n.vz;
        }

        this.alpha *= (1 - this.alphaDecay);
    }

    reheat() {
        this.alpha = 0.5;
    }
}


// ── Styles ──
const S = {
    page: {
        position: 'relative',
        height: 'calc(100vh - 70px)',
        display: 'flex',
        flexDirection: 'column',
        background: colors.bg,
        overflow: 'hidden',
    },
    filterBar: {
        display: 'flex',
        alignItems: 'center',
        gap: '8px',
        padding: '10px 16px',
        borderBottom: `1px solid ${colors.border}`,
        background: colors.card,
        flexWrap: 'wrap',
        zIndex: 10,
    },
    filterGroup: {
        display: 'flex',
        alignItems: 'center',
        gap: '4px',
    },
    filterLabel: {
        fontSize: '9px',
        fontWeight: 700,
        letterSpacing: '1.5px',
        color: colors.textMuted,
        fontFamily: MONO,
        marginRight: '2px',
    },
    filterBtn: (active) => ({
        background: active ? `${colors.accent}30` : 'transparent',
        border: `1px solid ${active ? colors.accent : colors.border}`,
        borderRadius: '4px',
        padding: '4px 10px',
        fontSize: '10px',
        color: active ? '#E8F0F8' : colors.textMuted,
        cursor: 'pointer',
        fontFamily: MONO,
        transition: 'all 0.15s',
    }),
    searchInput: {
        background: colors.bg,
        border: `1px solid ${colors.border}`,
        borderRadius: '4px',
        padding: '4px 10px',
        fontSize: '11px',
        color: colors.text,
        fontFamily: MONO,
        width: '160px',
        outline: 'none',
        marginLeft: 'auto',
    },
    mainArea: {
        display: 'flex',
        flex: 1,
        position: 'relative',
        overflow: 'hidden',
    },
    canvasContainer: {
        flex: 1,
        position: 'relative',
        overflow: 'hidden',
    },
    detailPanel: {
        width: '340px',
        minWidth: '300px',
        background: colors.card,
        borderLeft: `1px solid ${colors.border}`,
        overflowY: 'auto',
        padding: '16px',
        display: 'flex',
        flexDirection: 'column',
        gap: '12px',
        zIndex: 5,
    },
    timelineBar: {
        display: 'flex',
        alignItems: 'center',
        gap: '10px',
        padding: '8px 16px',
        borderTop: `1px solid ${colors.border}`,
        background: colors.card,
        zIndex: 10,
    },
    badge: (color) => ({
        display: 'inline-block',
        padding: '4px 8px',
        borderRadius: '999px',
        fontSize: '9px',
        fontWeight: 700,
        fontFamily: MONO,
        letterSpacing: '0.5px',
        background: `${color}20`,
        color: color,
        border: `1px solid ${color}40`,
        whiteSpace: 'nowrap',
        minWidth: '32px',
        textAlign: 'center',
    }),
    metricRow: {
        display: 'flex',
        justifyContent: 'space-between',
        padding: '6px 0',
        borderBottom: `1px solid ${colors.borderSubtle}`,
        fontSize: '11px',
        gap: '8px',
        alignItems: 'center',
    },
    sectionTitle: {
        fontSize: '10px',
        fontWeight: 700,
        letterSpacing: '1.5px',
        color: colors.accent,
        fontFamily: MONO,
        marginTop: '8px',
    },
};


export default function ActorUniverse() {
    const containerRef = useRef(null);
    const fullScreenRef = useRef(null);
    const rendererRef = useRef(null);
    const sceneRef = useRef(null);
    const cameraRef = useRef(null);
    const controlsRef = useRef(null);
    const labelRendererRef = useRef(null);
    const simulationRef = useRef(null);
    const animFrameRef = useRef(null);
    const instancedMeshRef = useRef(null);
    const particlesRef = useRef([]);
    const labelsRef = useRef([]);
    const connectionLinesRef = useRef(null);
    const hoveredIndexRef = useRef(-1);
    const nodeDataRef = useRef([]);
    const linkDataRef = useRef([]);
    const raycasterRef = useRef(new THREE.Raycaster());
    const mouseRef = useRef(new THREE.Vector2());
    const clockRef = useRef(new THREE.Clock());
    const { isFullScreen, toggleFullScreen } = useFullScreen(fullScreenRef);

    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    // Filters
    const [tierFilter, setTierFilter] = useState('all');
    const [categoryFilter, setCategoryFilter] = useState('all');
    const [searchQuery, setSearchQuery] = useState('');

    // Selection
    const [selectedNode, setSelectedNode] = useState(null);
    const [actorDetail, setActorDetail] = useState(null);
    const [detailLoading, setDetailLoading] = useState(false);

    // Time controls
    const [timelineDays, setTimelineDays] = useState(90);
    const [isPlaying, setIsPlaying] = useState(true);

    // ── Load data ──
    useEffect(() => { loadData(); }, []);

    const loadData = async () => {
        setLoading(true);
        setError(null);
        try {
            const d = await api.getActorNetwork();
            setData(d);
        } catch (err) {
            setError(err.message || 'Failed to load actor network');
        }
        setLoading(false);
    };

    // ── Load actor detail on selection ──
    useEffect(() => {
        if (!selectedNode) { setActorDetail(null); return; }
        let cancelled = false;
        setDetailLoading(true);
        api.getActorDetail(selectedNode.id).then(d => {
            if (!cancelled) { setActorDetail(d); setDetailLoading(false); }
        }).catch(() => {
            if (!cancelled) setDetailLoading(false);
        });
        return () => { cancelled = true; };
    }, [selectedNode?.id]);

    // ── Filter flows by timeline ──
    const timelineFilteredFlows = useMemo(() => {
        const allFlows = data?.flows || [];
        if (!allFlows.length) return [];
        const cutoff = new Date();
        cutoff.setDate(cutoff.getDate() - timelineDays);
        return allFlows.filter(f => {
            const d = parseFlowDate(f.date);
            return !d || d >= cutoff;
        });
    }, [data?.flows, timelineDays]);

    // ── Filtered data ──
    const filteredData = useMemo(() => {
        if (!data) return { nodes: [], links: [], flows: [] };

        const q = searchQuery.toLowerCase().trim();
        let nodes = data.nodes || [];

        if (tierFilter !== 'all') nodes = nodes.filter(n => n.tier === tierFilter);
        if (categoryFilter !== 'all') nodes = nodes.filter(n => n.category === categoryFilter);
        if (q) {
            nodes = nodes.filter(n =>
                n.label.toLowerCase().includes(q)
                || (n.title || '').toLowerCase().includes(q)
                || n.id.toLowerCase().includes(q)
            );
        }

        const nodeIds = new Set(nodes.map(n => n.id));
        const links = (data.links || []).filter(
            l => nodeIds.has(l.source?.id || l.source) && nodeIds.has(l.target?.id || l.target)
        );

        return {
            nodes: nodes.map(n => ({ ...n })),
            links: links.map(l => ({
                source: l.source?.id || l.source,
                target: l.target?.id || l.target,
                strength: l.strength || 0.3,
                relationship: l.relationship || '',
            })),
            flows: timelineFilteredFlows.filter(f => nodeIds.has(f.from) || nodeIds.has(f.to)),
        };
    }, [data, tierFilter, categoryFilter, searchQuery, timelineFilteredFlows]);

    // ── Categories list ──
    const categories = useMemo(() => {
        if (!data?.nodes) return [];
        return [...new Set(data.nodes.map(n => n.category))].sort();
    }, [data?.nodes]);

    // ── Flow aggregation ──
    const flowAggregation = useMemo(() => {
        const actors = {};
        (filteredData.flows || []).forEach(f => {
            const amt = Math.abs(f.amount || 0);
            if (f.from) {
                if (!actors[f.from]) actors[f.from] = { inflow: 0, outflow: 0 };
                actors[f.from].outflow += amt;
            }
            if (f.to) {
                if (!actors[f.to]) actors[f.to] = { inflow: 0, outflow: 0 };
                actors[f.to].inflow += amt;
            }
        });
        return Object.entries(actors)
            .map(([id, agg]) => ({
                id,
                label: (data?.nodes || []).find(n => n.id === id)?.label || id,
                ...agg,
                net: agg.inflow - agg.outflow,
            }))
            .sort((a, b) => Math.abs(b.net) - Math.abs(a.net))
            .slice(0, 15);
    }, [filteredData.flows, data?.nodes]);


    // ═══════════════════════════════════════════════════════
    //  THREE.JS SCENE SETUP + RENDER LOOP
    // ═══════════════════════════════════════════════════════
    useEffect(() => {
        if (!containerRef.current || filteredData.nodes.length === 0) return;

        const container = containerRef.current;
        const width = container.clientWidth;
        const height = container.clientHeight;

        // ── Scene ──
        const scene = new THREE.Scene();
        scene.background = new THREE.Color(colors.bg);
        scene.fog = new THREE.FogExp2(colors.bg, 0.0015);
        sceneRef.current = scene;

        // ── Camera ──
        const camera = new THREE.PerspectiveCamera(60, width / height, 0.5, 2000);
        camera.position.set(0, 80, 300);
        camera.lookAt(0, 0, 0);
        cameraRef.current = camera;

        // ── Renderer ──
        const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: false });
        renderer.setSize(width, height);
        renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
        renderer.toneMapping = THREE.ACESFilmicToneMapping;
        renderer.toneMappingExposure = 1.2;
        container.appendChild(renderer.domElement);
        rendererRef.current = renderer;

        // ── CSS2D Label Renderer ──
        const labelRenderer = new CSS2DRenderer();
        labelRenderer.setSize(width, height);
        labelRenderer.domElement.style.position = 'absolute';
        labelRenderer.domElement.style.top = '0';
        labelRenderer.domElement.style.left = '0';
        labelRenderer.domElement.style.pointerEvents = 'none';
        container.appendChild(labelRenderer.domElement);
        labelRendererRef.current = labelRenderer;

        // ── Orbit Controls ──
        const controls = new OrbitControls(camera, renderer.domElement);
        controls.enableDamping = true;
        controls.dampingFactor = 0.08;
        controls.minDistance = 20;
        controls.maxDistance = 800;
        controls.autoRotate = false;
        controls.autoRotateSpeed = 0.3;
        controlsRef.current = controls;

        // ── Ambient + directional light ──
        scene.add(new THREE.AmbientLight(0x404060, 0.6));
        const dirLight = new THREE.DirectionalLight(0xffffff, 0.8);
        dirLight.position.set(50, 100, 80);
        scene.add(dirLight);
        const backLight = new THREE.DirectionalLight(0x4466aa, 0.3);
        backLight.position.set(-50, -30, -60);
        scene.add(backLight);

        // ── Subtle grid helper at y=0 ──
        const gridHelper = new THREE.GridHelper(600, 40, 0x1A2332, 0x0D1520);
        gridHelper.position.y = -80;
        scene.add(gridHelper);

        // ─────────────────────────────────────
        //  NODES (InstancedMesh)
        // ─────────────────────────────────────
        const nodes = filteredData.nodes;
        const links = filteredData.links;
        const nodeCount = nodes.length;
        nodeDataRef.current = nodes;
        linkDataRef.current = links;

        // Initialize 3D positions -- spread in sphere by tier
        const nodeMap = {};
        nodes.forEach((n, i) => {
            const shellR = TIER_SHELL_RADIUS[n.tier] || 150;
            const phi = Math.acos(2 * Math.random() - 1);
            const theta = Math.random() * Math.PI * 2;
            n.x = shellR * Math.sin(phi) * Math.cos(theta) + (Math.random() - 0.5) * 20;
            n.y = shellR * Math.sin(phi) * Math.sin(theta) + (Math.random() - 0.5) * 20;
            n.z = shellR * Math.cos(phi) + (Math.random() - 0.5) * 20;
            n.vx = 0; n.vy = 0; n.vz = 0;
            n._index = i;
            n._radius = Math.max(1.5, Math.min(8, 1.5 + Math.log(1 + (n.influence || 0.1)) * 4));
            nodeMap[n.id] = n;
        });

        // InstancedMesh -- spheres
        const sphereGeo = new THREE.SphereGeometry(1, 16, 12);
        const sphereMat = new THREE.MeshPhongMaterial({
            color: 0xffffff,
            transparent: true,
            opacity: 0.9,
            shininess: 60,
        });
        const instancedMesh = new THREE.InstancedMesh(sphereGeo, sphereMat, nodeCount);
        instancedMesh.instanceMatrix.setUsage(THREE.DynamicDrawUsage);
        scene.add(instancedMesh);
        instancedMeshRef.current = instancedMesh;

        // Color buffer for per-instance colors
        const colorArray = new Float32Array(nodeCount * 3);
        const tempColor = new THREE.Color();
        nodes.forEach((n, i) => {
            const tierColor = TIER_COLORS[n.tier] || 0x5A7080;
            tempColor.setHex(tierColor);
            colorArray[i * 3] = tempColor.r;
            colorArray[i * 3 + 1] = tempColor.g;
            colorArray[i * 3 + 2] = tempColor.b;
        });
        instancedMesh.instanceColor = new THREE.InstancedBufferAttribute(colorArray, 3);

        // ── CSS2D Labels ──
        const labelObjs = [];
        nodes.forEach((n) => {
            const div = document.createElement('div');
            div.textContent = n.label.length > 16 ? n.label.substring(0, 14) + '..' : n.label;
            div.style.cssText = `
                font-family: ${MONO};
                font-size: 9px;
                color: ${colors.textDim};
                background: ${colors.card}CC;
                padding: 1px 4px;
                border-radius: 3px;
                pointer-events: none;
                white-space: nowrap;
                user-select: none;
            `;
            const label = new CSS2DObject(div);
            label.position.set(0, 0, 0);
            label.visible = false; // will show based on LOD
            label.userData = { nodeId: n.id, nodeIndex: n._index };
            scene.add(label);
            labelObjs.push(label);
        });
        labelsRef.current = labelObjs;

        // ─────────────────────────────────────
        //  CONNECTIONS (BufferGeometry lines)
        // ─────────────────────────────────────
        const linePositions = new Float32Array(links.length * 6); // 2 verts per line * 3 coords
        const lineColors = new Float32Array(links.length * 6);
        const lineGeo = new THREE.BufferGeometry();
        lineGeo.setAttribute('position', new THREE.BufferAttribute(linePositions, 3));
        lineGeo.setAttribute('color', new THREE.BufferAttribute(lineColors, 3));
        const lineMat = new THREE.LineBasicMaterial({
            vertexColors: true,
            transparent: true,
            opacity: 0.3,
            linewidth: 1,
        });
        const lineSegments = new THREE.LineSegments(lineGeo, lineMat);
        scene.add(lineSegments);
        connectionLinesRef.current = lineSegments;

        // ── Active flow pair lookup for line coloring ──
        const activeFlowPairs = new Set();
        (filteredData.flows || []).forEach(f => {
            activeFlowPairs.add(`${f.from}|${f.to}`);
            activeFlowPairs.add(`${f.to}|${f.from}`);
        });

        // ─────────────────────────────────────
        //  3D FORCE SIMULATION
        // ─────────────────────────────────────
        const sim = new ForceSimulation3D(nodes, links, {
            alphaDecay: 0.015,
            velocityDecay: 0.35,
            centerStrength: 0.008,
            chargeStrength: -20,
            linkDistance: 35,
            linkStrength: 0.2,
        });
        simulationRef.current = sim;

        // ─────────────────────────────────────
        //  PARTICLE SYSTEM (animated flows)
        // ─────────────────────────────────────
        const MAX_PARTICLES = 300;
        const particleGeo = new THREE.BufferGeometry();
        const particlePositions = new Float32Array(MAX_PARTICLES * 3);
        const particleColorsArr = new Float32Array(MAX_PARTICLES * 3);
        const particleSizes = new Float32Array(MAX_PARTICLES);
        particleGeo.setAttribute('position', new THREE.BufferAttribute(particlePositions, 3));
        particleGeo.setAttribute('color', new THREE.BufferAttribute(particleColorsArr, 3));
        particleGeo.setAttribute('size', new THREE.BufferAttribute(particleSizes, 1));

        const particleMat = new THREE.PointsMaterial({
            size: 2,
            vertexColors: true,
            transparent: true,
            opacity: 0.85,
            blending: THREE.AdditiveBlending,
            sizeAttenuation: true,
            depthWrite: false,
        });
        const particlePoints = new THREE.Points(particleGeo, particleMat);
        scene.add(particlePoints);

        // Particle data array
        const particles = [];
        particlesRef.current = particles;
        let particleSpawnTimer = 0;
        const SPAWN_INTERVAL = 0.12; // seconds

        function spawnParticle(flows) {
            if (!flows.length || particles.length >= MAX_PARTICLES) return;
            const flow = flows[Math.floor(Math.random() * flows.length)];
            const fromNode = nodeMap[flow.from];
            const toNode = nodeMap[flow.to];
            if (!fromNode || !toNode) return;

            const color = new THREE.Color(getFlowColorHex(flow.type));
            particles.push({
                from: fromNode,
                to: toNode,
                progress: 0,
                speed: 0.3 + Math.random() * 0.5, // units per sec
                color,
                size: Math.max(1.5, Math.min(5, 1 + Math.log10(Math.max(flow.amount || 1000, 1000)) * 0.5)),
            });
        }

        // ─────────────────────────────────────
        //  UPDATE INSTANCED MESH + LINES
        // ─────────────────────────────────────
        const dummy = new THREE.Object3D();

        function updateScene(delta) {
            // Run force simulation
            sim.tick();

            // Update instanced mesh transforms
            for (let i = 0; i < nodeCount; i++) {
                const n = nodes[i];
                dummy.position.set(n.x, n.y, n.z);
                const r = n._radius;
                // LOD: scale based on distance from camera
                const distToCamera = camera.position.distanceTo(dummy.position);
                const lodScale = distToCamera < 100 ? 1.0 : distToCamera < 300 ? 0.7 : 0.4;
                dummy.scale.set(r * lodScale, r * lodScale, r * lodScale);

                // Opacity from trust score -- modulate instance color alpha via brightness
                // (InstancedMesh doesn't support per-instance opacity natively;
                //  we approximate by darkening low-trust nodes)
                const trust = n.trust_score || 0.5;
                const brightness = 0.4 + trust * 0.6;
                const tierColor = TIER_COLORS[n.tier] || 0x5A7080;
                tempColor.setHex(tierColor);
                tempColor.multiplyScalar(brightness);

                // Highlight hovered
                if (i === hoveredIndexRef.current) {
                    tempColor.multiplyScalar(1.6);
                }

                instancedMesh.instanceColor.array[i * 3] = tempColor.r;
                instancedMesh.instanceColor.array[i * 3 + 1] = tempColor.g;
                instancedMesh.instanceColor.array[i * 3 + 2] = tempColor.b;

                dummy.updateMatrix();
                instancedMesh.setMatrixAt(i, dummy.matrix);

                // Update label position + LOD visibility
                const label = labelObjs[i];
                if (label) {
                    label.position.set(n.x, n.y + r * 1.5 + 3, n.z);
                    // Show label only when close enough or node is influential
                    label.visible = distToCamera < 150 || n.influence > 0.8;
                }
            }
            instancedMesh.instanceMatrix.needsUpdate = true;
            instancedMesh.instanceColor.needsUpdate = true;

            // Update connection lines
            const posArr = lineSegments.geometry.attributes.position.array;
            const colArr = lineSegments.geometry.attributes.color.array;
            for (let i = 0; i < links.length; i++) {
                const l = links[i];
                const s = l.source;
                const t = l.target;
                if (!s || !t || typeof s !== 'object' || typeof t !== 'object') continue;
                const offset = i * 6;
                posArr[offset] = s.x;     posArr[offset + 1] = s.y;     posArr[offset + 2] = s.z;
                posArr[offset + 3] = t.x; posArr[offset + 4] = t.y; posArr[offset + 5] = t.z;

                // Color: active flows = green, else dim gray
                const key = `${s.id}|${t.id}`;
                const isActive = activeFlowPairs.has(key);
                if (isActive) {
                    colArr[offset] = 0.13; colArr[offset + 1] = 0.77; colArr[offset + 2] = 0.37;
                    colArr[offset + 3] = 0.13; colArr[offset + 4] = 0.77; colArr[offset + 5] = 0.37;
                } else {
                    colArr[offset] = 0.15; colArr[offset + 1] = 0.2; colArr[offset + 2] = 0.28;
                    colArr[offset + 3] = 0.15; colArr[offset + 4] = 0.2; colArr[offset + 5] = 0.28;
                }
            }
            lineSegments.geometry.attributes.position.needsUpdate = true;
            lineSegments.geometry.attributes.color.needsUpdate = true;

            // ── Update particles ──
            particleSpawnTimer += delta;
            if (particleSpawnTimer > SPAWN_INTERVAL && isPlayingRef.current) {
                particleSpawnTimer = 0;
                spawnParticle(filteredData.flows);
            }

            // Animate existing particles
            for (let i = particles.length - 1; i >= 0; i--) {
                const p = particles[i];
                p.progress += p.speed * delta;
                if (p.progress >= 1) {
                    particles.splice(i, 1);
                    continue;
                }
                const idx = i * 3;
                particlePositions[idx] = p.from.x + (p.to.x - p.from.x) * p.progress;
                particlePositions[idx + 1] = p.from.y + (p.to.y - p.from.y) * p.progress;
                particlePositions[idx + 2] = p.from.z + (p.to.z - p.from.z) * p.progress;
                particleColorsArr[idx] = p.color.r;
                particleColorsArr[idx + 1] = p.color.g;
                particleColorsArr[idx + 2] = p.color.b;
                particleSizes[i] = p.size;
            }
            // Zero out unused slots
            for (let i = particles.length; i < MAX_PARTICLES; i++) {
                particlePositions[i * 3] = 0;
                particlePositions[i * 3 + 1] = -9999;
                particlePositions[i * 3 + 2] = 0;
                particleSizes[i] = 0;
            }
            particleGeo.attributes.position.needsUpdate = true;
            particleGeo.attributes.color.needsUpdate = true;
            particleGeo.attributes.size.needsUpdate = true;
            particleGeo.setDrawRange(0, Math.min(particles.length, MAX_PARTICLES));
        }

        // ── isPlaying ref for animation closure ──
        const isPlayingRef = { current: isPlaying };

        // ── RENDER LOOP ──
        function animate() {
            animFrameRef.current = requestAnimationFrame(animate);
            const delta = clockRef.current.getDelta();
            controls.update();
            updateScene(delta);
            renderer.render(scene, camera);
            labelRenderer.render(scene, camera);
        }
        animate();

        // ── RESIZE HANDLER ──
        const onResize = () => {
            const w = container.clientWidth;
            const h = container.clientHeight;
            if (w === 0 || h === 0) return;
            camera.aspect = w / h;
            camera.updateProjectionMatrix();
            renderer.setSize(w, h);
            labelRenderer.setSize(w, h);
        };
        const resizeObserver = new ResizeObserver(onResize);
        resizeObserver.observe(container);

        // ── MOUSE INTERACTIONS ──
        let clickDownTime = 0;

        const onPointerDown = () => { clickDownTime = performance.now(); };

        const onPointerMove = (event) => {
            const rect = renderer.domElement.getBoundingClientRect();
            mouseRef.current.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
            mouseRef.current.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;

            // Raycast for hover
            raycasterRef.current.setFromCamera(mouseRef.current, camera);
            const intersects = raycasterRef.current.intersectObject(instancedMesh);
            if (intersects.length > 0) {
                const idx = intersects[0].instanceId;
                hoveredIndexRef.current = idx;
                renderer.domElement.style.cursor = 'pointer';
                // Dim non-connected nodes via line opacity
                lineMat.opacity = 0.1;
                // Make hovered connections brighter
            } else {
                hoveredIndexRef.current = -1;
                renderer.domElement.style.cursor = 'default';
                lineMat.opacity = 0.3;
            }
        };

        const onClick = (event) => {
            // Ignore drags
            if (performance.now() - clickDownTime > 300) return;

            raycasterRef.current.setFromCamera(mouseRef.current, camera);
            const intersects = raycasterRef.current.intersectObject(instancedMesh);
            if (intersects.length > 0) {
                const idx = intersects[0].instanceId;
                const node = nodes[idx];
                setSelectedNode(prev => prev?.id === node.id ? null : node);
            } else {
                setSelectedNode(null);
            }
        };

        const onDblClick = (event) => {
            raycasterRef.current.setFromCamera(mouseRef.current, camera);
            const intersects = raycasterRef.current.intersectObject(instancedMesh);
            if (intersects.length > 0) {
                const idx = intersects[0].instanceId;
                const node = nodes[idx];
                // Fly camera to this node's local cluster
                const target = new THREE.Vector3(node.x, node.y, node.z);
                const cameraTarget = target.clone().add(new THREE.Vector3(20, 15, 30));

                // Smooth fly
                const startPos = camera.position.clone();
                const startTarget = controls.target.clone();
                const duration = 1000;
                const startTime = performance.now();

                function flyStep() {
                    const elapsed = performance.now() - startTime;
                    const t = Math.min(1, elapsed / duration);
                    const ease = t < 0.5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2;

                    camera.position.lerpVectors(startPos, cameraTarget, ease);
                    controls.target.lerpVectors(startTarget, target, ease);
                    controls.update();

                    if (t < 1) requestAnimationFrame(flyStep);
                }
                flyStep();
            }
        };

        renderer.domElement.addEventListener('pointerdown', onPointerDown);
        renderer.domElement.addEventListener('pointermove', onPointerMove);
        renderer.domElement.addEventListener('click', onClick);
        renderer.domElement.addEventListener('dblclick', onDblClick);

        // ── CLEANUP ──
        return () => {
            cancelAnimationFrame(animFrameRef.current);
            resizeObserver.disconnect();
            renderer.domElement.removeEventListener('pointerdown', onPointerDown);
            renderer.domElement.removeEventListener('pointermove', onPointerMove);
            renderer.domElement.removeEventListener('click', onClick);
            renderer.domElement.removeEventListener('dblclick', onDblClick);
            controls.dispose();

            // Remove labels
            labelObjs.forEach(l => scene.remove(l));

            // Dispose geometries + materials
            sphereGeo.dispose();
            sphereMat.dispose();
            lineGeo.dispose();
            lineMat.dispose();
            particleGeo.dispose();
            particleMat.dispose();
            instancedMesh.dispose();

            renderer.dispose();
            if (container.contains(renderer.domElement)) container.removeChild(renderer.domElement);
            if (container.contains(labelRenderer.domElement)) container.removeChild(labelRenderer.domElement);
        };
    }, [filteredData.nodes.length, filteredData.links.length]);

    // ── Update isPlaying in the animation closure ──
    useEffect(() => {
        // We communicate with the closure via a ref-like pattern on particles
        // The spawn logic already checks isPlayingRef; but since it's captured
        // in the closure we need to re-trigger. The effect above re-runs on
        // filteredData changes. For play/pause we set a flag on window
        // (simple approach that avoids full scene rebuild).
        window.__gridUniversePlaying = isPlaying;
    }, [isPlaying]);

    // ── Search: fly camera to found actor ──
    useEffect(() => {
        if (!searchQuery || !cameraRef.current || !controlsRef.current) return;
        const q = searchQuery.toLowerCase().trim();
        if (!q) return;

        const nodes = nodeDataRef.current;
        const found = nodes.find(n =>
            n.label.toLowerCase().includes(q)
            || (n.title || '').toLowerCase().includes(q)
        );
        if (!found) return;

        const camera = cameraRef.current;
        const controls = controlsRef.current;
        const target = new THREE.Vector3(found.x, found.y, found.z);
        const cameraTarget = target.clone().add(new THREE.Vector3(15, 10, 25));

        const startPos = camera.position.clone();
        const startTarget = controls.target.clone();
        const duration = 800;
        const startTime = performance.now();

        function flyStep() {
            const elapsed = performance.now() - startTime;
            const t = Math.min(1, elapsed / duration);
            const ease = t < 0.5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2;
            camera.position.lerpVectors(startPos, cameraTarget, ease);
            controls.target.lerpVectors(startTarget, target, ease);
            controls.update();
            if (t < 1) requestAnimationFrame(flyStep);
        }
        flyStep();
    }, [searchQuery]);


    // ── RENDER ──
    if (loading) {
        return (
            <div style={{ ...S.page, justifyContent: 'center', alignItems: 'center' }}>
                <div style={{ color: colors.textMuted, fontFamily: MONO, fontSize: '13px' }}>
                    Loading actor universe...
                </div>
            </div>
        );
    }

    if (error) {
        return (
            <div style={{ ...S.page, justifyContent: 'center', alignItems: 'center' }}>
                <div style={{ color: colors.red, fontFamily: MONO, fontSize: '13px' }}>{error}</div>
                <button onClick={loadData} style={{ ...shared.buttonSmall, marginTop: '12px' }}>Retry</button>
            </div>
        );
    }

    return (
        <div ref={fullScreenRef} style={S.page}>
            {/* ── Filter bar ── */}
            <div style={S.filterBar}>
                {/* Tier filter */}
                <div style={S.filterGroup}>
                    <span style={S.filterLabel}>TIER</span>
                    {['all', 'sovereign', 'regional', 'institutional', 'individual'].map(t => (
                        <button key={t} onClick={() => setTierFilter(t)} style={{
                            ...S.filterBtn(tierFilter === t),
                            ...(t !== 'all' && tierFilter === t ? { borderColor: TIER_HEX[t], color: TIER_HEX[t] } : {}),
                        }}>
                            {t === 'all' ? 'All' : TIER_LABELS[t]}
                        </button>
                    ))}
                </div>

                <div style={{ width: '1px', height: '20px', background: colors.border }} />

                {/* Category filter */}
                <div style={S.filterGroup}>
                    <span style={S.filterLabel}>TYPE</span>
                    <button onClick={() => setCategoryFilter('all')} style={S.filterBtn(categoryFilter === 'all')}>All</button>
                    {categories.map(c => (
                        <button key={c} onClick={() => setCategoryFilter(c)} style={S.filterBtn(categoryFilter === c)}>
                            {CATEGORY_LABELS[c] || c}
                        </button>
                    ))}
                </div>

                <div style={{ width: '1px', height: '20px', background: colors.border }} />

                {/* Search */}
                <input
                    type="text"
                    placeholder="Search actors..."
                    value={searchQuery}
                    onChange={e => setSearchQuery(e.target.value)}
                    style={S.searchInput}
                />

                {/* Stats */}
                <div style={{ fontSize: '9px', color: colors.textMuted, fontFamily: MONO, whiteSpace: 'nowrap' }}>
                    {filteredData.nodes.length} actors / {filteredData.links.length} links
                    {filteredData.flows.length > 0 && ` / ${filteredData.flows.length} flows`}
                </div>
            </div>

            {/* ── Main area: 3D canvas + detail panel ── */}
            <div style={S.mainArea}>
                {/* 3D Canvas */}
                <div ref={containerRef} style={S.canvasContainer}>
                    {/* Legend overlay */}
                    <div style={{
                        position: 'absolute', bottom: '12px', left: '12px',
                        background: `${colors.card}DD`,
                        border: `1px solid ${colors.border}`,
                        borderRadius: '8px',
                        padding: '10px 14px',
                        fontFamily: MONO,
                        fontSize: '9px',
                        display: 'flex',
                        flexDirection: 'column',
                        gap: '4px',
                        zIndex: 2,
                        pointerEvents: 'none',
                    }}>
                        {Object.entries(TIER_HEX).map(([tier, color]) => (
                            <div key={tier} style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                                <div style={{ width: '10px', height: '10px', borderRadius: '50%', background: `${color}60`, border: `1.5px solid ${color}` }} />
                                <span style={{ color: colors.textDim }}>{TIER_LABELS[tier]}</span>
                            </div>
                        ))}
                        <div style={{ marginTop: '4px', borderTop: `1px solid ${colors.borderSubtle}`, paddingTop: '4px' }}>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                                <div style={{ width: '8px', height: '8px', borderRadius: '50%', background: '#22C55E' }} />
                                <span style={{ color: colors.textDim }}>Money in</span>
                            </div>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                                <div style={{ width: '8px', height: '8px', borderRadius: '50%', background: '#EF4444' }} />
                                <span style={{ color: colors.textDim }}>Money out</span>
                            </div>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                                <div style={{ width: '8px', height: '8px', borderRadius: '50%', background: '#FFD700' }} />
                                <span style={{ color: colors.textDim }}>Influence</span>
                            </div>
                        </div>
                        <div style={{ marginTop: '4px', borderTop: `1px solid ${colors.borderSubtle}`, paddingTop: '4px', color: colors.textMuted, fontSize: '8px' }}>
                            Scroll=zoom | Drag=rotate | Right-drag=pan | Dbl-click=fly to
                        </div>
                    </div>

                    {/* Fullscreen button */}
                    <button
                        onClick={toggleFullScreen}
                        style={{
                            position: 'absolute', top: '12px', right: '12px',
                            background: `${colors.card}DD`,
                            border: `1px solid ${colors.border}`,
                            borderRadius: '6px',
                            padding: '6px 10px',
                            color: colors.textDim,
                            fontFamily: MONO,
                            fontSize: '9px',
                            cursor: 'pointer',
                            zIndex: 2,
                        }}
                    >
                        {isFullScreen ? 'EXIT FS' : 'FULLSCREEN'}
                    </button>
                </div>

                {/* ── Detail Panel (right sidebar) ── */}
                {selectedNode && (
                    <div style={S.detailPanel}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: '8px' }}>
                            <div style={{ minWidth: 0, flex: 1 }}>
                                <div title={selectedNode.label} style={{ fontSize: '16px', fontWeight: 700, color: '#E8F0F8', fontFamily: SANS, lineHeight: '1.2', wordBreak: 'break-word' }}>
                                    {selectedNode.label}
                                </div>
                                <div title={selectedNode.title} style={{ fontSize: '11px', color: colors.textDim, marginTop: '2px', lineHeight: '1.5' }}>
                                    {selectedNode.title}
                                </div>
                            </div>
                            <span style={{ ...S.badge(TIER_HEX[selectedNode.tier] || '#5A7080'), flexShrink: 0 }}>
                                {(selectedNode.tier || '').toUpperCase()}
                            </span>
                        </div>

                        {/* Key metrics */}
                        <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
                            <div style={{ ...shared.metric, flex: 1, minWidth: '80px' }}>
                                <div style={{ ...shared.metricValue, fontSize: '15px' }}>
                                    {((selectedNode.influence || 0) * 100).toFixed(0)}%
                                </div>
                                <div style={shared.metricLabel}>Influence</div>
                            </div>
                            <div style={{ ...shared.metric, flex: 1, minWidth: '80px' }}>
                                <div style={{ ...shared.metricValue, fontSize: '15px' }}>
                                    {((selectedNode.trust_score || 0) * 100).toFixed(0)}%
                                </div>
                                <div style={shared.metricLabel}>Trust</div>
                            </div>
                        </div>

                        {/* Trust bar */}
                        <div>
                            <div style={{ height: '4px', background: colors.border, borderRadius: '2px', overflow: 'hidden' }}>
                                <div style={{
                                    height: '100%',
                                    width: `${(selectedNode.trust_score || 0) * 100}%`,
                                    background: `linear-gradient(90deg, ${colors.accent}, ${TIER_HEX[selectedNode.tier] || colors.accent})`,
                                    borderRadius: '2px',
                                }} />
                            </div>
                        </div>

                        {/* Detail from API */}
                        {detailLoading && (
                            <div style={{ color: colors.textMuted, fontSize: '10px', fontFamily: MONO }}>
                                Loading details...
                            </div>
                        )}

                        {actorDetail && (
                            <>
                                {actorDetail.net_worth && (
                                    <div style={S.metricRow}>
                                        <span style={{ color: colors.textMuted }}>Net Worth</span>
                                        <span style={{ color: colors.text, fontFamily: MONO, fontWeight: 600 }}>
                                            {formatMoney(actorDetail.net_worth)}
                                        </span>
                                    </div>
                                )}
                                {actorDetail.category && (
                                    <div style={S.metricRow}>
                                        <span style={{ color: colors.textMuted }}>Category</span>
                                        <span style={{ color: colors.textDim, fontFamily: MONO }}>
                                            {CATEGORY_LABELS[actorDetail.category] || actorDetail.category}
                                        </span>
                                    </div>
                                )}

                                {/* Connections */}
                                {actorDetail.connections && actorDetail.connections.length > 0 && (
                                    <div>
                                        <div style={S.sectionTitle}>CONNECTIONS ({actorDetail.connections.length})</div>
                                        <div style={{ maxHeight: '150px', overflowY: 'auto', marginTop: '6px' }}>
                                            {actorDetail.connections.slice(0, 15).map((c, i) => (
                                                <div key={i} style={{ ...S.metricRow, padding: '4px 0', fontSize: '10px' }}>
                                                    <span style={{ color: colors.textDim, flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                                                        {c.label || c.target || c.id}
                                                    </span>
                                                    <span style={{ color: colors.textMuted, fontSize: '9px', flexShrink: 0 }}>
                                                        {c.relationship || c.type || ''}
                                                    </span>
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                )}

                                {/* Recent actions */}
                                {actorDetail.recent_actions && actorDetail.recent_actions.length > 0 && (
                                    <div>
                                        <div style={S.sectionTitle}>RECENT ACTIONS</div>
                                        <div style={{ maxHeight: '150px', overflowY: 'auto', marginTop: '6px' }}>
                                            {actorDetail.recent_actions.slice(0, 10).map((a, i) => (
                                                <div key={i} style={{
                                                    background: colors.bg,
                                                    borderRadius: '4px',
                                                    padding: '6px 8px',
                                                    marginBottom: '3px',
                                                    fontSize: '10px',
                                                }}>
                                                    <div style={{ color: colors.textDim, lineHeight: '1.4' }}>
                                                        {a.description || a.action || a.what || JSON.stringify(a)}
                                                    </div>
                                                    {a.date && (
                                                        <div style={{ color: colors.textMuted, fontSize: '9px', marginTop: '2px' }}>
                                                            {a.date}
                                                        </div>
                                                    )}
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                )}
                            </>
                        )}

                        {/* Flow summary for selected actor */}
                        {(() => {
                            const agg = flowAggregation.find(a => a.id === selectedNode.id);
                            if (!agg) return null;
                            return (
                                <div>
                                    <div style={S.sectionTitle}>MONEY FLOWS</div>
                                    <div style={{ display: 'flex', gap: '8px', marginTop: '6px' }}>
                                        <div style={{ ...shared.metric, flex: 1 }}>
                                            <div style={{ ...shared.metricValue, fontSize: '13px', color: '#22C55E' }}>
                                                {formatMoney(agg.inflow)}
                                            </div>
                                            <div style={shared.metricLabel}>Inflow</div>
                                        </div>
                                        <div style={{ ...shared.metric, flex: 1 }}>
                                            <div style={{ ...shared.metricValue, fontSize: '13px', color: '#EF4444' }}>
                                                {formatMoney(agg.outflow)}
                                            </div>
                                            <div style={shared.metricLabel}>Outflow</div>
                                        </div>
                                        <div style={{ ...shared.metric, flex: 1 }}>
                                            <div style={{ ...shared.metricValue, fontSize: '13px', color: agg.net >= 0 ? '#22C55E' : '#EF4444' }}>
                                                {formatMoney(agg.net)}
                                            </div>
                                            <div style={shared.metricLabel}>Net</div>
                                        </div>
                                    </div>
                                </div>
                            );
                        })()}

                        {/* Close button */}
                        <button
                            onClick={() => setSelectedNode(null)}
                            style={{
                                ...S.filterBtn(false),
                                marginTop: '8px',
                                textAlign: 'center',
                                padding: '8px',
                            }}
                        >
                            Close
                        </button>
                    </div>
                )}
            </div>

            {/* ── Timeline / Time Scrub Bar ── */}
            <div style={S.timelineBar}>
                <span style={{ fontSize: '9px', fontWeight: 700, letterSpacing: '1.5px', color: colors.textMuted, fontFamily: MONO }}>
                    TIME
                </span>
                <button
                    onClick={() => setIsPlaying(!isPlaying)}
                    style={{
                        ...S.filterBtn(isPlaying),
                        padding: '4px 12px',
                        fontSize: '10px',
                    }}
                >
                    {isPlaying ? 'PAUSE' : 'PLAY'}
                </button>
                {[30, 60, 90].map(d => (
                    <button
                        key={d}
                        onClick={() => setTimelineDays(d)}
                        style={{
                            ...S.filterBtn(timelineDays === d),
                            padding: '4px 10px',
                        }}
                    >
                        {d}d
                    </button>
                ))}
                <input
                    type="range"
                    min={7}
                    max={180}
                    value={timelineDays}
                    onChange={e => setTimelineDays(Number(e.target.value))}
                    style={{
                        flex: 1,
                        height: '4px',
                        accentColor: colors.accent,
                        cursor: 'pointer',
                    }}
                />
                <span style={{ fontSize: '10px', color: colors.text, fontFamily: MONO, minWidth: '40px', textAlign: 'right' }}>
                    {timelineDays}d
                </span>
            </div>
        </div>
    );
}
