// components/ColorLegend.jsx
import styles from './ColorLegend.module.css'

// ── Helpers ─────────────────────────────────────────────────────────────────
function scalePct(v, vmin, vmax) {
  return ((v - vmin) / (vmax - vmin) * 100).toFixed(2) + '%'
}

// ── 1. Paleta NWS dBZ (ta sama co w shaderze) ────────────────────────────
const DBZ_STOPS = [
  [-10,   4, 233, 231],
  [  0,   1, 159, 244],
  [  5,   3,   0, 244],
  [ 10,   2, 253,   2],
  [ 15,   1, 197,   1],
  [ 20,   0, 142,   0],
  [ 25, 253, 248,   2],
  [ 30, 229, 188,   0],
  [ 35, 253, 149,   0],
  [ 40, 253,   0,   0],
  [ 45, 212,   0,   0],
  [ 50, 188,   0,   0],
  [ 55, 248,   0, 253],
  [ 60, 152,  84, 198],
  [ 70, 255, 255, 255],
]
const DBZ_MIN = -10
const DBZ_MAX = 70

const nwsGradient =
  'linear-gradient(to top, ' +
  DBZ_STOPS.map(([v, r, g, b]) => `rgb(${r},${g},${b}) ${scalePct(v, DBZ_MIN, DBZ_MAX)}`).join(', ') +
  ')'

const DBZ_LABELS = [-10, 0, 10, 20, 30, 40, 50, 60, 70]

// ── 2. Paleta wysokości (turbo-like, 0–20 km) ────────────────────────────
const HEIGHT_STOPS = [
  [  0,  48,  18,  59],
  [  2,  70, 120, 210],
  [  4,  55, 191, 178],
  [  6,  47, 225,  96],
  [  8, 167, 238,  46],
  [ 10, 253, 188,  48],
  [ 13, 234,  84,  28],
  [ 16, 162,  17,  10],
  [ 20, 122,   4,   3],
]

function buildHeightGradient(vmin, vmax) {
  const stops = HEIGHT_STOPS
    .map(([v, r, g, b]) => [Math.max(vmin, Math.min(vmax, v)), r, g, b])
    .filter(([v], i, arr) => i === 0 || v !== arr[i - 1][0])
  return (
    'linear-gradient(to top, ' +
    stops.map(([v, r, g, b]) => `rgb(${r},${g},${b}) ${scalePct(v, vmin, vmax)}`).join(', ') +
    ')'
  )
}

function buildHeightLabels(vmin, vmax) {
  const step = vmax <= 5 ? 1 : vmax <= 10 ? 2 : 3
  const labels = []
  for (let v = Math.ceil(vmin); v <= vmax; v += step) labels.push(v)
  if (labels[labels.length - 1] !== Math.round(vmax)) labels.push(Math.round(vmax))
  return labels
}

// ── 3. Paleta prędkości radialnej (dywergująca niebieski→biały→czerwony) ──
// Niebieski = zbliżanie (ujemne), czerwony = oddalanie (dodatnie)
// Musi być identyczna z buildVelocityPalette() w RadarWebGL.jsx
function velocityColor(v, vmin, vmax) {
  let r, g, b
  if (v < 0) {
    const s = vmin !== 0 ? v / vmin : 0
    r = Math.round(240 * (1 - s))
    g = Math.round(240 * (1 - s))
    b = Math.round(240 + (178 - 240) * (1 - s))
  } else {
    const s = vmax > 0 ? v / vmax : 0
    r = Math.round(240 + (140 - 240) * s)
    g = Math.round(240 * (1 - s))
    b = Math.round(240 * (1 - s))
  }
  return `rgb(${r},${g},${b})`
}

function buildVelocityGradient(vmin, vmax) {
  const steps = 16
  const stops = []
  for (let i = 0; i <= steps; i++) {
    const v = vmin + (i / steps) * (vmax - vmin)
    stops.push(`${velocityColor(v, vmin, vmax)} ${scalePct(v, vmin, vmax)}`)
  }
  return `linear-gradient(to top, ${stops.join(', ')})`
}

function buildVelocityLabels(vmin, vmax) {
  // Etykiety symetryczne co 5 lub 10 m/s
  const absMax = Math.max(Math.abs(vmin), Math.abs(vmax))
  const step = absMax <= 15 ? 5 : 10
  const labels = []
  for (let v = Math.ceil(vmin / step) * step; v <= vmax; v += step) {
    labels.push(Math.round(v))
  }
  return labels
}

// ── Detekcja typu ────────────────────────────────────────────────────────────
function detectType(quantity, product) {
  const q = quantity.toUpperCase()
  if (q === 'HGHT' || q === 'HEIGHT' || q === 'H') return 'height'
  if (q === 'VRAD' || q === 'V' || q === 'VRADDH' || q === 'VRADS') return 'velocity'
  // Fallback po nazwie produktu gdy quantity nieznane lub niespójne
  const p = (product ?? '').toUpperCase()
  if (p.endsWith('_EHT')) return 'height'
  if (p.endsWith('_CAPPI_V')) return 'velocity'
  return 'dbz'
}

// ── Komponent ────────────────────────────────────────────────────────────────
export default function ColorLegend({ proj, product }) {
  const quantity = proj?.quantity ?? ''
  const type = detectType(quantity, product)

  if (type === 'height') {
    const vmin = proj?.vmin ?? 0
    const vmax = proj?.vmax ?? 20
    return (
      <LegendBar
        title="km"
        gradient={buildHeightGradient(vmin, vmax)}
        labels={buildHeightLabels(vmin, vmax)}
        vmin={vmin}
        vmax={vmax}
      />
    )
  }

  if (type === 'velocity') {
    const vmin = proj?.vmin ?? -30
    const vmax = proj?.vmax ?? 30
    return (
      <LegendBar
        title="m/s"
        gradient={buildVelocityGradient(vmin, vmax)}
        labels={buildVelocityLabels(vmin, vmax)}
        vmin={vmin}
        vmax={vmax}
      />
    )
  }

  // Domyślnie: dBZ
  return (
    <LegendBar
      title="dBZ"
      gradient={nwsGradient}
      labels={DBZ_LABELS}
      vmin={DBZ_MIN}
      vmax={DBZ_MAX}
    />
  )
}

function LegendBar({ title, gradient, labels, vmin, vmax }) {
  return (
    <div className={styles.legend}>
      <div className={styles.title}>{title}</div>
      <div className={styles.barWrapper}>
        <div className={styles.bar} style={{ background: gradient }} />
        <div className={styles.labels}>
          {labels.map(v => (
            <span
              key={v}
              className={styles.label}
              style={{ bottom: scalePct(v, vmin, vmax) }}
            >
              {v}
            </span>
          ))}
        </div>
      </div>
    </div>
  )
}
