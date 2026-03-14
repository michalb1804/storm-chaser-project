// components/Sidebar.jsx
import styles from './Sidebar.module.css'
import { RADAR_PRODUCTS } from '../hooks/useRadar'

function fmt(val, units) {
  if (val == null) return '—'
  const n = typeof val === 'number' ? val.toFixed(1) : val
  return units ? `${n} ${units}` : `${n}`
}

function fmtTime(isoStr) {
  if (!isoStr) return '—'
  try {
    const d = new Date(isoStr)
    return d.toUTCString().replace('GMT', 'UTC').slice(0, -4)
  } catch { return isoStr }
}

export default function Sidebar({
  product, onProductChange,
  meta, loading, error, lastUpdate, onRefresh,
  pointValue,
}) {
  return (
    <aside className={styles.sidebar}>

      {/* Header */}
      <div className={styles.header}>
        <span className={styles.logo}>⬡ STORM</span>
        <span className={styles.version}>v0.1</span>
      </div>

      {/* Status */}
      <div className={styles.statusBar}>
        <span className={`${styles.dot} ${loading ? styles.dotPulse : styles.dotOk}`} />
        <span className={styles.statusText}>
          {loading ? 'POBIERANIE...' : error ? 'BŁĄD' : 'LIVE'}
        </span>
        <span className={styles.updateTime}>
          {lastUpdate ? lastUpdate.toLocaleTimeString('pl', { hour12: false }) : '—'}
        </span>
        <button className={styles.refreshBtn} onClick={onRefresh} title="Odśwież">↺</button>
      </div>

      {/* Wybór produktu */}
      <section className={styles.section}>
        <div className={styles.sectionTitle}>PRODUKT</div>
        <div className={styles.productList}>
          {Object.entries(RADAR_PRODUCTS).map(([key, p]) => (
            <button
              key={key}
              className={`${styles.productBtn} ${product === key ? styles.productBtnActive : ''}`}
              onClick={() => onProductChange(key)}
              style={{ '--accent-color': p.color }}
            >
              <span className={styles.productLabel}>{p.label}</span>
              <span className={styles.productDesc}>{p.desc}</span>
            </button>
          ))}
        </div>
      </section>

      {/* Metadane skanu */}
      <section className={styles.section}>
        <div className={styles.sectionTitle}>SKAN</div>
        <div className={styles.metaGrid}>
          <span className={styles.metaKey}>CZAS</span>
          <span className={styles.metaVal}>{fmtTime(meta?.scan_time)}</span>

          <span className={styles.metaKey}>PRODUKT</span>
          <span className={styles.metaVal}>{meta?.quantity || '—'}</span>

          <span className={styles.metaKey}>MAX</span>
          <span className={styles.metaVal}>{fmt(meta?.val_max, meta?.quantity === 'DBZH' ? 'dBZ' : '')}</span>

          <span className={styles.metaKey}>NaN%</span>
          <span className={styles.metaVal}>{meta?.nan_pct != null ? `${meta.nan_pct.toFixed(1)}%` : '—'}</span>

          <span className={styles.metaKey}>CACHE</span>
          <span className={styles.metaVal}>{meta?.cache_age_s != null ? `${meta.cache_age_s.toFixed(0)}s` : '—'}</span>
        </div>
      </section>

      {/* Wartość w punkcie */}
      <section className={styles.section}>
        <div className={styles.sectionTitle}>KLIKNIJ NA MAPĘ</div>
        {pointValue ? (
          <div className={styles.pointBox}>
            <div className={styles.pointCoords}>
              {pointValue.lat?.toFixed(4)}°N &nbsp; {pointValue.lon?.toFixed(4)}°E
            </div>
            <div className={styles.pointValue}>
              {pointValue.no_signal
                ? <span className={styles.noSignal}>BRAK SYGNAŁU</span>
                : <span className={styles.signal}>
                    {fmt(pointValue.value, pointValue.quantity || 'dBZ')}
                  </span>
              }
            </div>
          </div>
        ) : (
          <div className={styles.pointHint}>Kliknij dowolny punkt na mapie</div>
        )}
      </section>

      {/* Georef info */}
      {meta?.georef && (
        <section className={styles.section}>
          <div className={styles.sectionTitle}>SIATKA</div>
          <div className={styles.metaGrid}>
            <span className={styles.metaKey}>ROZM</span>
            <span className={styles.metaVal}>{meta.georef.shape?.join('×')}</span>
            <span className={styles.metaKey}>PROJ</span>
            <span className={styles.metaVal}>{meta.georef.projection?.toUpperCase()}</span>
            <span className={styles.metaKey}>LAT</span>
            <span className={styles.metaVal}>{meta.georef.lat_min?.toFixed(2)}–{meta.georef.lat_max?.toFixed(2)}</span>
            <span className={styles.metaKey}>LON</span>
            <span className={styles.metaVal}>{meta.georef.lon_min?.toFixed(2)}–{meta.georef.lon_max?.toFixed(2)}</span>
          </div>
        </section>
      )}

      {/* Stopka */}
      <div className={styles.footer}>
        Dane: IMGW-PIB
      </div>
    </aside>
  )
}
