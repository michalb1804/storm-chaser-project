// components/ProductPicker.jsx
// Selektor produktu radarowego — tylko typy CMAX, CAPPI, PPI, EHT.
// Pogrupowany po stacji radarowej.
import styles from './ProductPicker.module.css'

// Stacje radarowe z dostępnymi typami produktów
const RADARS = [
  {
    id:    'COMPO',
    label: 'Kompozyt (PL)',
    items: [
      { key: 'COMPO_CMAX',  label: 'CMAX',  desc: 'Max. odbicia', units: 'dBZ' },
      { key: 'COMPO_CAPPI', label: 'CAPPI', desc: '1 km CAPPI',   units: 'dBZ' },
      { key: 'COMPO_EHT',   label: 'EHT',   desc: 'Wys. echa',    units: 'km'  },
    ],
  },
  {
    id:    'LEG',
    label: 'Legionowo',
    items: [
      { key: 'LEG_PPI',   label: 'PPI',   desc: 'Skan 0.5°', units: 'dBZ' },
      { key: 'LEG_CAPPI', label: 'CAPPI', desc: 'CAPPI',     units: 'dBZ' },
      { key: 'LEG_EHT',   label: 'EHT',   desc: 'Wys. echa', units: 'km'  },
    ],
  },
  {
    id:    'BRZ',
    label: 'Brzuchania',
    items: [
      { key: 'BRZ_PPI',   label: 'PPI',   desc: 'Skan 0.5°', units: 'dBZ' },
      { key: 'BRZ_CAPPI', label: 'CAPPI', desc: 'CAPPI',     units: 'dBZ' },
      { key: 'BRZ_EHT',   label: 'EHT',   desc: 'Wys. echa', units: 'km'  },
    ],
  },
  {
    id:    'GDY',
    label: 'Gdynia',
    items: [
      { key: 'GDY_PPI',   label: 'PPI',   desc: 'Skan 0.5°', units: 'dBZ' },
      { key: 'GDY_CAPPI', label: 'CAPPI', desc: 'CAPPI',     units: 'dBZ' },
      { key: 'GDY_EHT',   label: 'EHT',   desc: 'Wys. echa', units: 'km'  },
    ],
  },
  {
    id:    'GSA',
    label: 'Góra Św. Anny',
    items: [
      { key: 'GSA_PPI',   label: 'PPI',   desc: 'Skan 0.5°', units: 'dBZ' },
      { key: 'GSA_CAPPI', label: 'CAPPI', desc: 'CAPPI',     units: 'dBZ' },
      { key: 'GSA_EHT',   label: 'EHT',   desc: 'Wys. echa', units: 'km'  },
    ],
  },
  {
    id:    'PAS',
    label: 'Pastewnik',
    items: [
      { key: 'PAS_PPI',   label: 'PPI',   desc: 'Skan 0.5°', units: 'dBZ' },
      { key: 'PAS_CAPPI', label: 'CAPPI', desc: 'CAPPI',     units: 'dBZ' },
      { key: 'PAS_EHT',   label: 'EHT',   desc: 'Wys. echa', units: 'km'  },
    ],
  },
  {
    id:    'POZ',
    label: 'Poznań',
    items: [
      { key: 'POZ_PPI',   label: 'PPI',   desc: 'Skan 0.5°', units: 'dBZ' },
      { key: 'POZ_CAPPI', label: 'CAPPI', desc: 'CAPPI',     units: 'dBZ' },
      { key: 'POZ_EHT',   label: 'EHT',   desc: 'Wys. echa', units: 'km'  },
    ],
  },
  {
    id:    'RAM',
    label: 'Ramża',
    items: [
      { key: 'RAM_PPI',   label: 'PPI',   desc: 'Skan 0.5°', units: 'dBZ' },
      { key: 'RAM_CAPPI', label: 'CAPPI', desc: 'CAPPI',     units: 'dBZ' },
      { key: 'RAM_EHT',   label: 'EHT',   desc: 'Wys. echa', units: 'km'  },
    ],
  },
  {
    id:    'RZE',
    label: 'Rzeszów',
    items: [
      { key: 'RZE_PPI',   label: 'PPI',   desc: 'Skan 0.5°', units: 'dBZ' },
      { key: 'RZE_CAPPI', label: 'CAPPI', desc: 'CAPPI',     units: 'dBZ' },
      { key: 'RZE_EHT',   label: 'EHT',   desc: 'Wys. echa', units: 'km'  },
    ],
  },
  {
    id:    'SWI',
    label: 'Świdwin',
    items: [
      { key: 'SWI_PPI',   label: 'PPI',   desc: 'Skan 0.5°', units: 'dBZ' },
      { key: 'SWI_CAPPI', label: 'CAPPI', desc: 'CAPPI',     units: 'dBZ' },
      { key: 'SWI_EHT',   label: 'EHT',   desc: 'Wys. echa', units: 'km'  },
    ],
  },
  {
    id:    'UZR',
    label: 'Użranki',
    items: [
      { key: 'UZR_PPI',   label: 'PPI',   desc: 'Skan 0.5°', units: 'dBZ' },
      { key: 'UZR_CAPPI', label: 'CAPPI', desc: 'CAPPI',     units: 'dBZ' },
      { key: 'UZR_EHT',   label: 'EHT',   desc: 'Wys. echa', units: 'km'  },
    ],
  },
]

export const PRODUCT_GROUPS = RADARS

export function findProduct(key) {
  for (const g of RADARS) {
    const item = g.items.find(i => i.key === key)
    if (item) return { ...item, group: g.label }
  }
  return null
}

export default function ProductPicker({ value, onChange }) {
  return (
    <div className={styles.picker}>
      {RADARS.map(radar => (
        <div key={radar.id} className={styles.group}>
          <div className={styles.groupLabel}>{radar.label}</div>
          <div className={styles.items}>
            {radar.items.map(item => (
              <button
                key={item.key}
                className={[
                  styles.item,
                  item.key === value ? styles.active : '',
                ].join(' ')}
                onClick={() => onChange(item.key)}
                title={`${item.desc} [${item.units}]`}
              >
                <span className={styles.itemLabel}>{item.label}</span>
                <span className={styles.itemUnits}>{item.units}</span>
              </button>
            ))}
          </div>
        </div>
      ))}
    </div>
  )
}
