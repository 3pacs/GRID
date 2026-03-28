import React, { useEffect, useMemo, useState } from 'react';
import { Canvas } from '@react-three/fiber';
import { Html, Line, OrbitControls, Stars } from '@react-three/drei';
import { eclipticTo3D } from '../lib/aspects.js';
import { tokens } from '../styles/tokens.js';

const PLANET_ORDER = ['Mercury', 'Venus', 'Moon', 'Mars', 'Jupiter', 'Saturn', 'Uranus', 'Neptune', 'Pluto'];
const ORBIT_RADII = {
    Mercury: 1.35,
    Venus: 1.8,
    Moon: 2.2,
    Mars: 2.8,
    Jupiter: 3.6,
    Saturn: 4.25,
    Uranus: 4.9,
    Neptune: 5.45,
    Pluto: 5.95,
};
const PLANET_COLORS = {
    Mercury: tokens.purple,
    Venus: '#DB2777',
    Moon: tokens.textBright,
    Mars: tokens.red,
    Jupiter: tokens.gold,
    Saturn: '#FCD34D',
    Uranus: '#67E8F9',
    Neptune: tokens.accent,
    Pluto: '#94A3B8',
};

function useViewportProfile() {
    const [profile, setProfile] = useState({
        compact: true,
        reducedMotion: false,
        webgl: true,
    });

    useEffect(() => {
        if (typeof window === 'undefined') return undefined;

        const compactQuery = window.matchMedia('(max-width: 720px)');
        const motionQuery = window.matchMedia('(prefers-reduced-motion: reduce)');
        const supportsWebGL = (() => {
            try {
                const canvas = document.createElement('canvas');
                return Boolean(canvas.getContext('webgl2') || canvas.getContext('webgl'));
            } catch {
                return false;
            }
        })();

        const sync = () => {
            setProfile({
                compact: compactQuery.matches,
                reducedMotion: motionQuery.matches,
                webgl: supportsWebGL,
            });
        };

        sync();

        const attach = (query) => {
            if (query.addEventListener) {
                query.addEventListener('change', sync);
                return () => query.removeEventListener('change', sync);
            }
            query.addListener(sync);
            return () => query.removeListener(sync);
        };

        const detachCompact = attach(compactQuery);
        const detachMotion = attach(motionQuery);

        return () => {
            detachCompact();
            detachMotion();
        };
    }, []);

    return profile;
}

function OrbitRing({ radius }) {
    return (
        <mesh rotation={[-Math.PI / 2, 0, 0]}>
            <ringGeometry args={[radius - 0.01, radius + 0.01, 128]} />
            <meshBasicMaterial color="#1E365A" transparent opacity={0.42} side={2} />
        </mesh>
    );
}

function PlanetMarker({ body, radius, compact }) {
    const point = eclipticTo3D(body.geocentric_longitude, radius);
    const size = compact ? (body.planet === 'Moon' ? 0.09 : 0.12) : (body.planet === 'Moon' ? 0.12 : 0.15);

    return (
        <group position={[point.x, 0, point.z]}>
            <mesh>
                <sphereGeometry args={[size, 24, 24]} />
                <meshStandardMaterial
                    color={PLANET_COLORS[body.planet] || tokens.accent}
                    emissive={body.is_retrograde ? '#7C3AED' : '#0A1628'}
                    emissiveIntensity={body.is_retrograde ? 0.8 : 0.35}
                />
            </mesh>
            <Html distanceFactor={compact ? 10 : 12} center>
                <div style={{
                    padding: '4px 8px',
                    borderRadius: '999px',
                    background: 'rgba(11, 13, 26, 0.86)',
                    border: `1px solid ${body.is_retrograde ? tokens.purple : tokens.cardBorder}`,
                    color: tokens.textBright,
                    fontSize: compact ? '9px' : '10px',
                    lineHeight: 1.2,
                    whiteSpace: 'nowrap',
                    fontFamily: tokens.fontMono,
                    transform: 'translate3d(-50%, -150%, 0)',
                    pointerEvents: 'none',
                }}>
                    {body.planet} {body.is_retrograde ? 'Rx' : ''}
                </div>
            </Html>
        </group>
    );
}

function AspectLayer({ positions, aspects, compact }) {
    const lookup = useMemo(() => {
        const map = {};
        for (const body of positions) {
            map[body.planet] = eclipticTo3D(body.geocentric_longitude, ORBIT_RADII[body.planet] || 2.5);
        }
        return map;
    }, [positions]);

    const visibleAspects = (aspects || []).filter((aspect) => aspect.orb_used <= 4).slice(0, compact ? 10 : 16);

    return visibleAspects.map((aspect) => {
        const from = lookup[aspect.planet1];
        const to = lookup[aspect.planet2];
        if (!from || !to) return null;

        const color = aspect.aspect_type === 'trine' || aspect.aspect_type === 'sextile'
            ? '#22C55E'
            : aspect.aspect_type === 'conjunction'
                ? '#F59E0B'
                : '#EF4444';

        return (
            <Line
                key={`${aspect.planet1}-${aspect.planet2}-${aspect.aspect_type}`}
                points={[
                    [from.x, 0.02, from.z],
                    [(from.x + to.x) / 2, compact ? 0.24 : 0.35, (from.z + to.z) / 2],
                    [to.x, 0.02, to.z],
                ]}
                color={color}
                lineWidth={aspect.applying ? 1.5 : 1}
                transparent
                opacity={aspect.applying ? 0.75 : 0.45}
            />
        );
    });
}

function OrreryScene({ positions, aspects, showAspectLines, autoRotate, compact, reducedMotion }) {
    const starCount = compact ? 900 : 2200;
    const starFactor = compact ? 2.2 : 3.2;

    return (
        <>
            <color attach="background" args={[tokens.bg]} />
            <ambientLight intensity={0.65} />
            <pointLight position={[0, 0, 0]} intensity={2.8} color={tokens.accent} />
            <pointLight position={[0, 6, 0]} intensity={0.5} color={tokens.purple} />
            {!reducedMotion && <Stars radius={60} depth={32} count={starCount} factor={starFactor} saturation={0} fade speed={0.45} />}

            <mesh>
                <sphereGeometry args={[0.42, 32, 32]} />
                <meshStandardMaterial color={tokens.gold} emissive="#C77612" emissiveIntensity={1.35} />
            </mesh>

            {PLANET_ORDER.map((planet) => (
                <OrbitRing key={`orbit-${planet}`} radius={ORBIT_RADII[planet]} />
            ))}

            {showAspectLines ? <AspectLayer positions={positions} aspects={aspects} compact={compact} /> : null}

            {positions.map((body) => (
                <PlanetMarker
                    key={body.planet}
                    body={body}
                    radius={ORBIT_RADII[body.planet] || 2.5}
                    compact={compact}
                />
            ))}

            <OrbitControls
                enablePan={false}
                enableDamping={!reducedMotion}
                dampingFactor={0.08}
                autoRotate={autoRotate && !reducedMotion}
                autoRotateSpeed={0.3}
                minDistance={compact ? 5.2 : 6}
                maxDistance={compact ? 12 : 14}
                maxPolarAngle={Math.PI / 2.08}
                minPolarAngle={Math.PI / 3.25}
            />
        </>
    );
}

export default function PlanetaryOrrery({
    positions = [],
    aspects = [],
    showAspectLines = true,
    autoRotate = true,
}) {
    const profile = useViewportProfile();

    if (!profile.webgl) {
        return (
            <div style={{
                minHeight: '320px',
                width: '100%',
                borderRadius: tokens.radius.xl,
                overflow: 'hidden',
                border: `1px solid ${tokens.cardBorder}`,
                background: 'linear-gradient(180deg, rgba(11, 13, 26, 0.98) 0%, rgba(17, 24, 39, 0.96) 100%)',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                padding: tokens.spacing.lg,
                color: tokens.textMuted,
                fontFamily: tokens.fontMono,
                textAlign: 'center',
            }}>
                3D Orrery unavailable in this browser. The date, aspect, and telemetry panels still work normally.
            </div>
        );
    }

    return (
        <div style={{
            height: 'clamp(300px, 72vw, 520px)',
            minHeight: '300px',
            width: '100%',
            borderRadius: tokens.radius.xl,
            overflow: 'hidden',
            border: `1px solid ${tokens.cardBorder}`,
            background: 'radial-gradient(circle at 50% 50%, rgba(74, 144, 217, 0.18) 0%, rgba(11, 13, 26, 1) 68%)',
        }}>
            <Canvas
                camera={{ position: profile.compact ? [0, 5.4, 8.8] : [0, 6.25, 7.8], fov: profile.compact ? 48 : 42 }}
                dpr={[1, profile.compact ? 1.35 : 1.75]}
                frameloop={profile.reducedMotion ? 'demand' : 'always'}
                gl={{
                    powerPreference: 'high-performance',
                    antialias: true,
                    alpha: false,
                }}
            >
                <OrreryScene
                    positions={positions}
                    aspects={aspects}
                    showAspectLines={showAspectLines}
                    autoRotate={autoRotate}
                    compact={profile.compact}
                    reducedMotion={profile.reducedMotion}
                />
            </Canvas>
        </div>
    );
}
