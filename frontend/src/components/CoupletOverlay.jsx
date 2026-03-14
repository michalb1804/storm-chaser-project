// components/CoupletOverlay.jsx
// Wizualizacja punktów pomiarowych Velocity Couplet i wyniku
import { useEffect, useState, useCallback, useRef } from 'react'
import styles from './CoupletOverlay.module.css'

export default function CoupletOverlay({ points, result, map, isActive, onClear }) {
  const [projected, setProjected] = useState([])
  const [mapSize, setMapSize] = useState({ w: 0, h: 0 })

  // Projekcja punktów na współrzędne ekranu
  const project = useCallback(() => {
    if (!map || !points?.length) {
      setProjected([])
      return
    }
    const size = map.getSize()
    setMapSize({ w: size.x, h: size.y })
    setProjected(
      points.map(p => ({
        ...p,
        px: map.latLngToContainerPoint([p.lat, p.lon])
      }))
    )
  }, [map, points])

  useEffect(() => {
    project()
    if (!map) return
    map.on('move zoom viewreset resize', project)
    return () => map.off('move zoom viewreset resize', project)
  }, [map, project])

  if (!isActive || projected.length === 0) return null

  const p1 = projected[0]
  const p2 = projected[1]

  return (
    <>
      {/* SVG layer: punkty i linia */}
      <svg
        style={{
          position: 'absolute',
          top: 0,
          left: 0,
          width: mapSize.w,
          height: mapSize.h,
          pointerEvents: 'none',
          zIndex: 450,
          overflow: 'visible',
        }}
      >
        <defs>
          {/* Gradient dla linii łączącej */}
          <linearGradient id="couplet-gradient" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stopColor="#60a8ff" />
            <stop offset="100%" stopColor="#ff6060" />
          </linearGradient>
        </defs>

        {/* Punkt 1 - niebieski (inbound/negative velocity) */}
        <g>
          <circle
            cx={p1.px.x}
            cy={p1.px.y}
            r={12}
            fill="rgba(96, 168, 255, 0.3)"
            stroke="#60a8ff"
            strokeWidth={2}
          />
          <text
            x={p1.px.x}
            y={p1.px.y + 4}
            textAnchor="middle"
            fill="#60a8ff"
            fontSize="12"
            fontWeight="bold"
            fontFamily="'Share Tech Mono', monospace"
          >
            {p1.loading ? '...' : (p1.value != null ? p1.value.toFixed(1) : '?')}
          </text>
          <text
            x={p1.px.x}
            y={p1.px.y - 18}
            textAnchor="middle"
            fill="#60a8ff"
            fontSize="10"
            fontFamily="'Share Tech Mono', monospace"
          >
            P1
          </text>
        </g>

        {/* Punkt 2 - czerwony (outbound/positive velocity) - jeśli istnieje */}
        {p2 && (
          <>
            {/* Linia łącząca */}
            <line
              x1={p1.px.x}
              y1={p1.px.y}
              x2={p2.px.x}
              y2={p2.px.y}
              stroke="url(#couplet-gradient)"
              strokeWidth={2}
              strokeDasharray="6,3"
            />

            {/* Punkt 2 */}
            <g>
              <circle
                cx={p2.px.x}
                cy={p2.px.y}
                r={12}
                fill="rgba(255, 96, 96, 0.3)"
                stroke="#ff6060"
                strokeWidth={2}
              />
              <text
                x={p2.px.x}
                y={p2.px.y + 4}
                textAnchor="middle"
                fill="#ff6060"
                fontSize="12"
                fontWeight="bold"
                fontFamily="'Share Tech Mono', monospace"
              >
                {p2.loading ? '...' : (p2.value != null ? p2.value.toFixed(1) : '?')}
              </text>
              <text
                x={p2.px.x}
                y={p2.px.y - 18}
                textAnchor="middle"
                fill="#ff6060"
                fontSize="10"
                fontFamily="'Share Tech Mono', monospace"
              >
                P2
              </text>
            </g>

            {/* Odległość na środku linii */}
            {!p2.loading && !p1.loading && result?.distanceKm != null && (
              <text
                x={(p1.px.x + p2.px.x) / 2}
                y={(p1.px.y + p2.px.y) / 2 - 8}
                textAnchor="middle"
                fill="rgba(255, 255, 255, 0.7)"
                fontSize="9"
                fontFamily="'Share Tech Mono', monospace"
              >
                {result.distanceKm.toFixed(1)} km
              </text>
            )}
          </>
        )}
      </svg>

      {/* Panel z wynikiem */}
      {result && (
        <div className={styles.resultPanel}>
          <div className={styles.header}>
            <span className={styles.title}>VELOCITY COUPLET</span>
            <button className={styles.closeBtn} onClick={onClear}>✕</button>
          </div>

          {result.error ? (
            <div className={styles.error}>{result.error}</div>
          ) : (
            <>
              <div className={styles.deltaRow}>
                <span className={styles.deltaLabel}>Δ Velocity</span>
                <span
                  className={styles.deltaValue}
                  style={{ color: result.interpretation.color }}
                >
                  {result.deltaVelocity > 0 ? '+' : ''}
                  {result.deltaVelocity.toFixed(1)} m/s
                </span>
              </div>

              <div className={styles.pointsRow}>
                <div className={styles.pointInfo}>
                  <span className={styles.pointLabel} style={{ color: '#60a8ff' }}>P1</span>
                  <span className={styles.pointValue}>
                    {result.point1.value?.toFixed(1) ?? '—'} m/s
                  </span>
                </div>
                <div className={styles.pointInfo}>
                  <span className={styles.pointLabel} style={{ color: '#ff6060' }}>P2</span>
                  <span className={styles.pointValue}>
                    {result.point2.value?.toFixed(1) ?? '—'} m/s
                  </span>
                </div>
              </div>

              <div className={styles.distanceRow}>
                <span className={styles.distanceLabel}>Odległość</span>
                <span className={styles.distanceValue}>{result.distanceKm.toFixed(2)} km</span>
              </div>

              <div
                className={styles.interpretation}
                style={{ borderColor: result.interpretation.color }}
              >
                <span
                  className={styles.interpretationLabel}
                  style={{ color: result.interpretation.color }}
                >
                  {result.interpretation.label}
                </span>
                <span className={styles.interpretationDesc}>
                  {result.interpretation.description}
                </span>
              </div>
            </>
          )}
        </div>
      )}

      {/* Instrukcja gdy brak drugiego punktu */}
      {projected.length === 1 && !result && (
        <div className={styles.instruction}>
          Dotknij drugi punkt na mapie
        </div>
      )}
    </>
  )
}