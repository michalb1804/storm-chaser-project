// App.jsx
import { useState, useCallback, useMemo } from 'react'
import RadarMap    from './components/RadarMap.jsx'
import Sidebar     from './components/Sidebar.jsx'
import TopBar      from './components/TopBar.jsx'
import TimeSlider  from './components/TimeSlider.jsx'
import { useRadarHistory }  from './hooks/useRadarHistory.js'
import { usePointValue }    from './hooks/useRadar.js'
import styles from './App.module.css'

export default function App() {
  const [product, setProduct] = useState('COMPO_CMAX')

  const {
    scans,
    selectedTs,
    setSelectedTs,
    scanTime,
    loading: histLoading,
    live,
    refresh,
  } = useRadarHistory(product, 5)

  const { value: pointValue, query: queryPoint } = usePointValue(product)

  const handleMapClick = useCallback((lat, lon) => {
    queryPoint(lat, lon)
  }, [queryPoint])

  const topScan    = scans[0] ?? null
  const pseudoMeta = topScan ? {
    scan_time:   topScan.scan_time,
    cache_age_s: topScan.age_s,
  } : null

  const popupContent = useMemo(() => {
    if (!pointValue) return null
    const { lat, lon, value, no_signal, quantity } = pointValue
    const val = no_signal || value == null
      ? '<span style="color:#506070">brak sygnału</span>'
      : `<span style="color:#00e5a0;font-size:18px">${value.toFixed(1)}</span> <span style="color:#506070">${quantity || 'dBZ'}</span>`
    return {
      lat, lon,
      content: `
        <div style="font-family:'Share Tech Mono',monospace;padding:4px 0">
          <div style="color:#506070;font-size:10px;margin-bottom:6px">
            ${lat.toFixed(4)}°N  ${lon.toFixed(4)}°E
          </div>
          <div>${val}</div>
          <div style="color:#506070;font-size:10px;margin-top:4px">${product}</div>
        </div>
      `
    }
  }, [pointValue, product])

  const noData = !histLoading && scans.length === 0

  return (
    <div className={styles.app}>
      <Sidebar
        product={product}
        onProductChange={setProduct}
        meta={pseudoMeta}
        loading={histLoading}
        error={noData ? 'Brak danych — odśwież' : null}
        lastUpdate={topScan ? new Date(topScan.scan_time) : null}
        onRefresh={refresh}
        pointValue={pointValue ?? null}
      />

      <div className={styles.main}>
        <TopBar
          product={product}
          meta={pseudoMeta}
          loading={histLoading}
          scanTime={scanTime}
          live={live}
        />
        <div className={styles.mapWrapper}>
          <RadarMap
            product={product}
            selectedTs={selectedTs}
            onMapClick={handleMapClick}
            popupContent={popupContent}
          />

          <TimeSlider
            scans={scans}
            selectedTs={selectedTs}
            onSelect={setSelectedTs}
            loading={histLoading}
          />

          {noData && (
            <div className={styles.errorOverlay}>
              <span className={styles.errorIcon}>⚠</span>
              <span className={styles.errorText}>Brak danych w cache</span>
              <button className={styles.retryBtn} onClick={refresh}>PONÓW</button>
            </div>
          )}

          <div className={styles.watermark}>IMGW-PIB · NOAA GFS</div>
        </div>
      </div>
    </div>
  )
}
