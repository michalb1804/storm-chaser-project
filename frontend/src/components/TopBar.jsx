// components/TopBar.jsx
import styles from './TopBar.module.css'

function fmtScanTime(iso) {
  if (!iso) return '—'
  try {
    const d = new Date(iso)
    const date = d.toISOString().slice(0, 10)
    const time = d.toISOString().slice(11, 16)
    return `${date}  ${time} UTC`
  } catch { return iso }
}

export default function TopBar({ product, meta, loading }) {
  return (
    <div className={styles.topbar}>
      <div className={styles.left}>
        <span className={styles.productName}>{product}</span>
        <span className={styles.sep}>·</span>
        <span className={styles.scanTime}>{fmtScanTime(meta?.scan_time)}</span>
      </div>

      <div className={styles.right}>
        {loading && <span className={styles.spinner}>⟳</span>}
        {meta?.val_max != null && (
          <span className={styles.stat}>
            MAX <span className={styles.statVal}>{meta.val_max.toFixed(1)}</span>
          </span>
        )}
        {meta?.nan_pct != null && (
          <span className={styles.stat}>
            COV <span className={styles.statVal}>{(100 - meta.nan_pct).toFixed(0)}%</span>
          </span>
        )}
        <span className={styles.cacheAge}>
          {meta?.cache_age_s != null
            ? `cache ${meta.cache_age_s.toFixed(0)}s`
            : ''}
        </span>
      </div>
    </div>
  )
}
