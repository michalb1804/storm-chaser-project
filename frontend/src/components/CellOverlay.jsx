import { useEffect, useCallback, useRef, useState } from 'react'
import styles from './CellOverlay.module.css'

// NWS-aligned colors by max dBZ
function cellColor(dbz) {
  if (dbz >= 65) return { fill: 'rgba(248,0,253,0.25)', stroke: '#F800FD' }
  if (dbz >= 55) return { fill: 'rgba(253,0,0,0.25)',   stroke: '#FD0000' }
  if (dbz >= 45) return { fill: 'rgba(253,149,0,0.25)', stroke: '#FD9500' }
  return           { fill: 'rgba(253,238,0,0.25)',       stroke: '#FDEE02' }
}

function cellRadius(area_km2) {
  return Math.min(Math.max(Math.sqrt(area_km2) * 1.8, 10), 40)
}

function trendArrow(history) {
  if (!history || history.length < 2) return '→'
  const vals = history.map(h => h.max_dbz).filter(v => v != null)
  if (vals.length < 2) return '→'
  const diff = vals[vals.length - 1] - vals[0]
  if (diff > 2)  return '↑'
  if (diff < -2) return '↓'
  return '→'
}

function DbzBar({ history }) {
  if (!history?.length) return null
  const vals = history.map(h => h.max_dbz ?? 0)
  const maxVal = Math.max(...vals, 1)
  return (
    <div className={styles.dbzBar}>
      {vals.map((v, i) => {
        const { stroke } = cellColor(v)
        return (
          <div key={i} className={styles.dbzBarCol}>
            <div
              className={styles.dbzBarFill}
              style={{ height: `${Math.round((v / maxVal) * 100)}%`, background: stroke }}
            />
            <div className={styles.dbzBarLabel}>{Math.round(v)}</div>
          </div>
        )
      })}
    </div>
  )
}

export default function CellOverlay({ cells, map }) {
  const [projected, setProjected]   = useState([])
  const [mapSize, setMapSize]       = useState({ w: 0, h: 0 })
  const [selectedId, setSelectedId] = useState(null)
  const selectedIdRef               = useRef(selectedId)

  useEffect(() => { selectedIdRef.current = selectedId }, [selectedId])

  const project = useCallback(() => {
    if (!map || !cells?.length) {
      setProjected([])
      return
    }
    const size = map.getSize()
    setMapSize({ w: size.x, h: size.y })
    setProjected(
      cells.map(cell => ({
        ...cell,
        px: map.latLngToContainerPoint([cell.lat, cell.lon]),
        forecast_px: (cell.forecast ?? []).map(f =>
          map.latLngToContainerPoint([f.lat, f.lon])
        ),
      }))
    )
  }, [map, cells])

  useEffect(() => {
    project()
    if (!map) return
    map.on('move zoom viewreset resize', project)
    return () => map.off('move zoom viewreset resize', project)
  }, [map, project])

  // Deselect when clicking the map background (not on a cell)
  useEffect(() => {
    if (!map) return
    const onMapClick = (e) => {
      if (e.originalEvent.target instanceof SVGElement) return
      setSelectedId(null)
    }
    map.on('click', onMapClick)
    return () => map.off('click', onMapClick)
  }, [map])

  const selectedCell = projected.find(c => c.id === selectedId) ?? null

  return (
    <>
      {/* SVG layer: circles + selected trajectory */}
      <svg
        style={{
          position:      'absolute',
          top:           0,
          left:          0,
          width:         projected.length ? mapSize.w : 0,
          height:        projected.length ? mapSize.h : 0,
          pointerEvents: 'none',
          zIndex:        400,
          overflow:      'visible',
        }}
      >
        <defs>
          <marker id="cell-arrow" markerWidth="6" markerHeight="6"
            refX="5" refY="3" orient="auto">
            <path d="M0,0 L0,6 L6,3 z" fill="rgba(255,255,255,0.8)" />
          </marker>
        </defs>

        {/* Trajectory + forecast for selected cell only */}
        {selectedCell && (() => {
          const { px, forecast_px, forecast } = selectedCell
          if (!forecast_px?.length) return null
          const trajPoints = [px, ...forecast_px].map(p => `${p.x},${p.y}`).join(' ')
          return (
            <g>
              <polyline
                points={trajPoints}
                fill="none"
                stroke="rgba(255,255,255,0.55)"
                strokeWidth="1.5"
                strokeDasharray="5,4"
              />
              {forecast_px.map((fp, i) => (
                <circle key={i} cx={fp.x} cy={fp.y} r={4}
                  fill="white" opacity={0.8 - i * 0.22} />
              ))}
              {forecast_px.map((fp, i) => (
                <text key={`t${i}`}
                  x={fp.x + 6} y={fp.y - 4}
                  fill="rgba(255,255,255,0.7)"
                  fontSize="9"
                  fontFamily="'Share Tech Mono', monospace"
                >
                  +{forecast[i].minutes}′
                </text>
              ))}
              {forecast_px.length >= 2 && (() => {
                const prev = forecast_px[forecast_px.length - 2]
                const last = forecast_px[forecast_px.length - 1]
                return (
                  <line
                    x1={prev.x} y1={prev.y}
                    x2={last.x} y2={last.y}
                    stroke="rgba(255,255,255,0.8)"
                    strokeWidth="1.5"
                    markerEnd="url(#cell-arrow)"
                  />
                )
              })()}
            </g>
          )
        })()}

        {/* All cell circles — pointer-events: auto for click */}
        {projected.map(cell => {
          const { px, max_dbz, area_km2, id } = cell
          const { fill, stroke } = cellColor(max_dbz)
          const r = cellRadius(area_km2)
          const isSelected = id === selectedId
          return (
            <g key={id}
              style={{ pointerEvents: 'auto', cursor: 'pointer' }}
              onClick={() => setSelectedId(prev => prev === id ? null : id)}
            >
              <circle
                cx={px.x} cy={px.y} r={r}
                fill={fill}
                stroke={stroke}
                strokeWidth={isSelected ? 2.5 : 1.5}
              />
              <text
                x={px.x} y={px.y - r - 4}
                textAnchor="middle"
                fill={stroke}
                fontSize="11"
                fontWeight="bold"
                fontFamily="'Share Tech Mono', monospace"
                style={{ textShadow: '0 0 3px #000', pointerEvents: 'none' }}
              >
                {Math.round(max_dbz)}
              </text>
            </g>
          )
        })}
      </svg>

      {/* Info panel for selected cell */}
      {selectedCell && (
        <div className={styles.infoPanel}>
          <div className={styles.infoPanelHeader}>
            <span style={{ color: cellColor(selectedCell.max_dbz).stroke }}>
              {Math.round(selectedCell.max_dbz)} dBZ
            </span>
            <span className={styles.trend}>{trendArrow(selectedCell.dbz_history)}</span>
            <button
              className={styles.closeBtn}
              onClick={() => setSelectedId(null)}
            >✕</button>
          </div>

          <div className={styles.infoRow}>
            <span className={styles.label}>Obszar</span>
            <span>{selectedCell.area_km2} km²</span>
          </div>

          {selectedCell.eht_km != null && (
            <div className={styles.infoRow}>
              <span className={styles.label}>Wierzchołek echa</span>
              <span>{selectedCell.eht_km} km</span>
            </div>
          )}

          <div className={styles.historyLabel}>Historia dBZ (5 skanów)</div>
          <DbzBar history={selectedCell.dbz_history} />
        </div>
      )}
    </>
  )
}
