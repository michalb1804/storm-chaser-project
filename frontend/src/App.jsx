// App.jsx
import { useState, useCallback, useMemo } from 'react'
import RadarMap    from './components/RadarMap.jsx'
import Sidebar     from './components/Sidebar.jsx'
import TopBar      from './components/TopBar.jsx'
import { useRadarImage, useRadarBounds, usePointValue } from './hooks/useRadar.js'
import styles from './App.module.css'

export default function App() {
  const [product, setProduct] = useState('COMPO_CMAX')

  // bounds pobierane per-produkt — każdy radar ma własną siatkę
  const bounds = useRadarBounds(product)

  const { imageUrl, meta, loading, error, lastUpdate, refresh } =
    useRadarImage(product, 60_000)

  const { value: pointValue, query: queryPoint } = usePointValue(product)

  const handleMapClick = useCallback((lat, lon) => {
    queryPoint(lat, lon)
  }, [queryPoint])

  // Popup HTML dla klikniętego punktu
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

  return (
    <div className={styles.app}>
      <Sidebar
        product={product}
        onProductChange={setProduct}
        meta={meta}
        loading={loading}
        error={error}
        lastUpdate={lastUpdate}
        onRefresh={refresh}
        pointValue={pointValue
          ? { ...pointValue, lat: pointValue.lat, lon: pointValue.lon }
          : null}
      />

      <div className={styles.main}>
        <TopBar product={product} meta={meta} loading={loading} />
        <div className={styles.mapWrapper}>
          <RadarMap
            imageUrl={imageUrl}
            bounds={bounds}
            onMapClick={handleMapClick}
            popupContent={popupContent}
          />

          {/* Overlay błędu */}
          {error && (
            <div className={styles.errorOverlay}>
              <span className={styles.errorIcon}>⚠</span>
              <span className={styles.errorText}>API niedostępne</span>
              <span className={styles.errorSub}>{error}</span>
              <button className={styles.retryBtn} onClick={refresh}>
                PONÓW
              </button>
            </div>
          )}

          {/* Watermark */}
          <div className={styles.watermark}>
            IMGW-PIB · NOAA GFS
          </div>
        </div>
      </div>
    </div>
  )
}
