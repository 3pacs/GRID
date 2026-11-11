import React, { useMemo, useState } from 'react';

const NY = 'America/New_York';
const timeLabel = time => new Date(time).toLocaleTimeString('en-US', { timeZone: NY, hour: 'numeric', minute: '2-digit' });
const nyDate = value => Number.isFinite(Date.parse(value)) && new Intl.DateTimeFormat('en-CA', {
    timeZone: NY, year: 'numeric', month: '2-digit', day: '2-digit',
}).format(new Date(value));
const price = value => value.toLocaleString('en-US', { maximumFractionDigits: 2 });
const positive = 'var(--gb-positive)', negative = 'var(--gb-negative)', neutral = 'var(--gb-muted)';
const tone = v => v === 0 ? neutral : v > 0 ? positive : negative;

export default function GammaIntradayChart({ snapshot: s, chain }) {
    const [selected, setSelected] = useState(null);
    const bars = s.bars;
    const chart = useMemo(() => {
        const start = Date.parse(s.session_start), duration = Date.parse(s.session_end) - start;
        const base = bars[0].open;
        let low = Math.min(base, ...bars.map(b => b.close)), high = Math.max(base, ...bars.map(b => b.close));
        const pad = Math.max(.01, (high - low) * .07);
        low -= pad; high += pad;
        const y = value => 97 - (value - low) / (high - low) * 88;
        const x = t => 3 + Math.min(1, Math.max(0, (t - start) / duration)) * 281;
        const points = bars.map(b => ({ x:x(Date.parse(b.time) + 60000), y:y(b.close), t:Date.parse(b.time) }));
        // Begin a new path after every missing minute. Isolated points keep a visible dot.
        let path = '';
        const islands = [];
        points.forEach((p, i) => {
            const disconnected = !i || p.t - points[i - 1].t !== 60000;
            path += (disconnected ? 'M' : 'L') + p.x.toFixed(2) + ',' + p.y.toFixed(2);
            if (disconnected && (!points[i + 1] || points[i + 1].t - p.t !== 60000)) islands.push(p);
        });
        const volumes = bars.filter(b => Number.isFinite(b.volume) && b.volume >= 0).map(b => b.volume);
        return { low, high, y, points, path, islands, base, duration,
            hasVolume: s.symbol !== 'SPX' && volumes.length > 0, maxVol: Math.max(1, ...volumes) };
    }, [s, bars]);
    const last = bars[bars.length - 1], delta = last.close - chart.base;
    const deltaPct = chart.base ? delta / chart.base * 100 : null;
    const walls = chain && nyDate(chain.as_of) === s.session_date
        ? [['Call', chain.call_wall], ['Put', chain.put_wall]].filter(([, v]) => Number.isFinite(v)) : [];
    const i = selected == null ? bars.length - 1 : bars.findIndex(b => b.time === selected);
    const selectedIndex = i < 0 ? bars.length - 1 : i, bar = bars[selectedIndex], point = chart.points[selectedIndex];
    const openLabel = timeLabel(s.session_start), closeLabel = timeLabel(s.session_end);
    const chartHeight = chart.hasVolume ? 147 : 116;
    const sourceTime = s.as_of ? timeLabel(s.as_of) + ' ET' : 'Unavailable';
    const lineColor = tone(delta);
    return <div className="gb-price-chart">
        <div className="gb-price-head"><strong>{price(last.close)}</strong>
            {Number.isFinite(deltaPct) && <span className={delta > 0 ? 'gb-positive' : delta < 0 ? 'gb-negative' : ''}>
                {delta > 0 ? '+' : ''}{deltaPct.toFixed(2)}% since first bar</span>}
        </div>
        <p>{s.session_date} · Through {sourceTime} · {s.source_label || 'Price source unavailable'}</p>
        <svg viewBox={'0 0 340 ' + chartHeight} role="img" aria-label={s.symbol + ' intraday one-minute closing prices'}
            onClick={e => {
                const box = e.currentTarget.getBoundingClientRect();
                const target = (e.clientX - box.left) / box.width * 340;
                const nearest = chart.points.reduce((best, pt, j) => Math.abs(pt.x - target) < Math.abs(chart.points[best].x - target) ? j : best, 0);
                setSelected(bars[nearest].time);
            }}>
            {[chart.high, chart.low].map((v, index) => <g key={index}><line x1="3" x2="284" y1={chart.y(v)} y2={chart.y(v)} className="gb-price-grid" />
                <text x="289" y={chart.y(v) + 3}>{price(v)}</text></g>)}
            <line x1="3" x2="284" y1={chart.y(chart.base)} y2={chart.y(chart.base)} className="gb-price-grid" strokeDasharray="2 3" />
            <path d={chart.path} fill="none" stroke={lineColor} strokeWidth="1.7" vectorEffect="non-scaling-stroke" />
            {chart.islands.map(p => <circle key={p.t} cx={p.x} cy={p.y} r="1.15" fill={lineColor} />)}
            {walls.map(([label, v]) => <g key={label} aria-label={label+' wall reference '+price(v)}>
                <title>{label} wall {price(v)} · {timeLabel(chain.as_of)} ET chain</title>
                <line x1="274" x2="284" y1={Math.max(9, Math.min(97, chart.y(v)))} y2={Math.max(9, Math.min(97, chart.y(v)))}
                    stroke={label === 'Call' ? positive : negative} strokeWidth="2" />
            </g>)}
            <circle cx={point.x} cy={point.y} r="2.4" fill={lineColor} stroke="var(--gb-card)" strokeWidth="1" />
            {chart.hasVolume && <g aria-label={s.symbol+' minute volume'}>
                {bars.map((b, j) => Number.isFinite(b.volume) && b.volume > 0 && <line key={b.time}
                    x1={chart.points[j].x} x2={chart.points[j].x} y1="130" y2={130 - 24 * (b.volume / chart.maxVol)} stroke={tone(b.close - b.open)}
                    strokeWidth={Math.min(2, 281 / (chart.duration / 60000))} />)}
                <text x="289" y="124">Vol.</text>
            </g>}
            <text x="3" y={chartHeight - 4}>{openLabel} ET</text><text textAnchor="end" x="284" y={chartHeight - 4}>{closeLabel} ET</text>
        </svg>
        {walls.length > 0 && <p className="gb-wallref"><b>Wall reference · {timeLabel(chain.as_of)} ET snapshot</b><br />
            {walls.map(([label, v]) => label+' '+price(v)+(v > chart.high ? ' ↑ above plot' : v < chart.low ? ' ↓ below plot' : '')).join('  ·  ')}</p>}
        <label className="gb-minute"><span>Inspect minute · {timeLabel(bar.time)} ET</span>
            <input type="range" min="0" max={bars.length - 1} value={selectedIndex} aria-label={s.symbol+' bar minute'}
                aria-valuetext={timeLabel(bar.time)+' ET'} onChange={e => { const k = Number(e.target.value); setSelected(k === bars.length-1 ? null : bars[k].time); }}/></label>
        <p>O {price(bar.open)} · H {price(bar.high)} · L {price(bar.low)} · C {price(bar.close)}
            {chart.hasVolume && ' · Vol '+(Number.isFinite(bar.volume) ? bar.volume.toLocaleString('en-US') : 'unavailable')}</p>
        <p>{s.symbol === 'SPX' ? 'Index volume unavailable. ' : !chart.hasVolume ? 'Volume unavailable. ' : ''}
            {Number.isInteger(s.missing_bars) && s.missing_bars > 0 && s.missing_bars+' missing minute'+(s.missing_bars === 1 ? '.' : 's.')}</p>
    </div>;
}
