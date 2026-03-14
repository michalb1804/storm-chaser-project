// components/TimeSlider.jsx
// Sortuje scans rosnąco po scan_time niezależnie od kolejności z API.
// Lewo = najstarszy, prawo = najnowszy = LIVE.
// Sterowanie przez timestamp (null = live).
import styles from './TimeSlider.module.css'

function fmtUTC(isoStr) {
  if (!isoStr) return '—'
  try {
    const d = new Date(isoStr)
    return (
      String(d.getUTCHours()).padStart(2, '0') + ':' +
      String(d.getUTCMinutes()).padStart(2, '0') + 'z'
    )
  } catch { return '—' }
}

export default function TimeSlider({ scans, selectedTs, onSelect, loading }) {
  if (!scans || scans.length === 0) {
    return (
      <div className={styles.wrap}>
        <div className={styles.panel}>
          <span className={styles.empty}>
            {loading ? 'POBIERANIE…' : 'BRAK SKANÓW'}
          </span>
        </div>
      </div>
    )
  }

  // Posortuj rosnąco — lewo=najstarszy, prawo=najnowszy — niezależnie od API
  const sorted = [...scans].sort((a, b) =>
    (a.scan_time ?? '').localeCompare(b.scan_time ?? '')
  )
  const n       = sorted.length
  const newest  = sorted[n - 1]   // prawy koniec
  const oldest  = sorted[0]       // lewy koniec
  const isLive  = selectedTs === null

  // Pozycja suwaka: 0=najstarszy(lewo), N-1=najnowszy(prawo)
  const sliderPos = isLive
    ? n - 1
    : Math.max(0, sorted.findIndex(s => s.timestamp === selectedTs))

  function handleSlider(e) {
    const pos = Number(e.target.value)
    onSelect(pos === n - 1 ? null : sorted[pos].timestamp)
  }

  return (
    <div className={styles.wrap}>
      <div className={styles.panel}>

        {/* Ticki: sorted[0]=lewo=najstarszy, sorted[N-1]=prawo=najnowszy */}
        <div className={styles.ticks}>
          {sorted.map((scan, i) => {
            const isNewest = i === n - 1
            const isActive = isNewest ? isLive : scan.timestamp === selectedTs
            return (
              <div
                key={scan.timestamp || i}
                className={[
                  styles.tick,
                  isActive ? styles.active : '',
                  isNewest ? styles.latest : '',
                ].join(' ')}
                onClick={() => onSelect(isNewest ? null : scan.timestamp)}
                title={scan.scan_time || ''}
              >
                <div className={styles.tickDot} />
                <span className={styles.tickLabel}>{fmtUTC(scan.scan_time)}</span>
              </div>
            )
          })}
        </div>

        {/* Suwak */}
        <div className={styles.sliderRow}>
          <span className={styles.endLabel}>{fmtUTC(oldest?.scan_time)}</span>
          <input
            type="range"
            className={styles.slider}
            min={0}
            max={n - 1}
            step={1}
            value={sliderPos}
            onChange={handleSlider}
          />
          <button
            className={[styles.liveBtn, isLive ? styles.liveBtnActive : ''].join(' ')}
            onClick={() => onSelect(null)}
          >
            ● LIVE
          </button>
        </div>

      </div>
    </div>
  )
}
