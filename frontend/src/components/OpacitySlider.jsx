// components/OpacitySlider.jsx
// Slider do regulacji przezroczystości danych radarowych
import styles from './OpacitySlider.module.css'

export default function OpacitySlider({ opacity, onChange }) {
  // Konwersja 0-1 na procenty 0-100
  const percent = Math.round(opacity * 100)

  function handleChange(e) {
    const value = Number(e.target.value) / 100
    onChange(value)
  }

  return (
    <div className={styles.wrap}>
      <div className={styles.panel}>
        <span className={styles.label}>PRZEZROCZYSTOŚĆ</span>
        <input
          type="range"
          className={styles.slider}
          min={0}
          max={100}
          step={5}
          value={percent}
          onChange={handleChange}
        />
        <span className={styles.value}>{percent}%</span>
      </div>
    </div>
  )
}