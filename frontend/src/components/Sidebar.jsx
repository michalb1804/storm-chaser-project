// components/Sidebar.jsx
// Pasek boczny: tytuł, ProductPicker, meta, wartość w punkcie, georef.
import { useState } from 'react'
import ProductPicker from './ProductPicker.jsx'
import styles from './Sidebar.module.css'

function fmt(value, quantity) {
  if (value == null) return '—'
  if (quantity === 'dBZ' || quantity === 'DBZH') return `${value.toFixed(1)} dBZ`
  if (quantity === 'Height' || quantity === 'EHT')  return `${value.toFixed(1)} km`
  if (quantity === 'SRI' || quantity === 'mm/h')    return `${value.toFixed(2)} mm/h`
  if (quantity === 'KDP')  return `${value.toFixed(2)} °/km`
  if (quantity === 'ZDR')  return `${value.toFixed(2)} dB`
  return `${value.toFixed(2)}`
}

function formatScanTime(isoStr) {
  if (!isoStr) return '—'
  try {
    const d = new Date(isoStr)
    const pad = n => String(n).padStart(2, '0')
    return `${pad(d.getUTCDate())}.${pad(d.getUTCMonth()+1)} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}z`
  } catch { return '—' }
}

export default function Sidebar({
  product, onProductChange,
  meta, loading, error, lastUpdate, onRefresh,
  pointValue,
}) {
  const [pickerOpen, setPickerOpen] = useState(false)

  return (
    <aside className={styles.sidebar}>
      {/* Nagłówek */}
      <div className={styles.header}>
        <span className={styles.logo}>ATMOS</span>
        <span className={styles.logoSub}>Storm Radar Pro</span>
      </div>

      {/* Aktywny produkt + toggle pickera */}
      <section className={styles.section}>
        <div className={styles.sectionTitle}>PRODUKT</div>
        <button
          className={styles.productToggle}
          onClick={() => setPickerOpen(o => !o)}
          title="Zmień produkt radarowy"
        >
          <span className={styles.productKey}>{product}</span>
          <span className={styles.productArrow}>{pickerOpen ? '▲' : '▼'}</span>
        </button>

        {pickerOpen && (
          <div className={styles.pickerWrap}>
            <ProductPicker
              value={product}
              onChange={key => { onProductChange(key); setPickerOpen(false) }}
            />
          </div>
        )}
      </section>

      {/* Status / meta */}
      <section className={styles.section}>
        <div className={styles.sectionTitle}>STATUS</div>
        <div className={styles.metaGrid}>
          <span className={styles.metaKey}>SKAN</span>
          <span className={styles.metaVal}>
            {loading ? '…' : formatScanTime(meta?.scan_time)}
          </span>

          <span className={styles.metaKey}>CACHE</span>
          <span className={styles.metaVal}>
            {meta?.cache_age_s != null ? `${meta.cache_age_s.toFixed(0)}s` : '—'}
          </span>
        </div>

        <button
          className={styles.refreshBtn}
          onClick={onRefresh}
          disabled={loading}
        >
          {loading ? 'ŁADOWANIE…' : '⟳ ODŚWIEŻ'}
        </button>

        {error && (
          <div className={styles.errorMsg}>{error}</div>
        )}
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

      {/* Stopka */}
      <div className={styles.footer}>
        Dane: IMGW-PIB
      </div>
    </aside>
  )
}
