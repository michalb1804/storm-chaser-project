// components/CoupletButton.jsx
// Przycisk do włączania trybu Velocity Couplet Measurement
import styles from './CoupletButton.module.css'

export default function CoupletButton({ isActive, onToggle, disabled, isVelocityProduct }) {
  return (
    <div className={styles.wrap}>
      <button
        className={[
          styles.btn,
          isActive ? styles.active : '',
          disabled ? styles.disabled : ''
        ].join(' ')}
        onClick={onToggle}
        disabled={disabled}
        title={
          disabled
            ? 'Dostępny tylko dla produktów velocity (CAPPI_V)'
            : isActive
              ? 'Wyłącz tryb pomiaru couplet'
              : 'Włącz tryb pomiaru Velocity Couplet'
        }
      >
        <span className={styles.icon}>⟳</span>
        <span className={styles.label}>
          {isActive ? 'COUPLET ●' : 'COUPLET'}
        </span>
      </button>
    </div>
  )
}