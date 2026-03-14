import { useState, useEffect, useCallback, useRef } from 'react'

const API = ''

// Products for which cell tracking makes sense (reflectivity only)
const REFLECTIVITY_PRODUCTS = new Set([
  'COMPO_CMAX', 'COMPO_CAPPI',
  'LEG_PPI', 'LEG_CAPPI', 'BRZ_PPI', 'BRZ_CAPPI',
  'GDY_PPI', 'GDY_CAPPI', 'GSA_PPI', 'GSA_CAPPI',
  'PAS_PPI', 'PAS_CAPPI', 'POZ_PPI', 'POZ_CAPPI',
  'RAM_PPI', 'RAM_CAPPI', 'RZE_PPI', 'RZE_CAPPI',
  'SWI_PPI', 'SWI_CAPPI', 'UZR_PPI', 'UZR_CAPPI',
])

export function useCells(product) {
  const [data, setData] = useState({ cells: [], motion_kmh: 0, motion_deg: 0 })
  const enabled    = REFLECTIVITY_PRODUCTS.has(product)
  const retryRef   = useRef(null)

  const fetch_ = useCallback(async () => {
    if (!enabled) return
    try {
      const res = await fetch(`${API}/api/radar/${product}/cells`)
      if (!res.ok) return
      const json = await res.json()
      setData(json)
      // Jeśli wykryto komórki ale brak wektora ruchu (za mało historii w cache),
      // ponów za 15s — backfill prawdopodobnie jeszcze trwa
      if (json.cells?.length > 0 && json.motion_kmh === 0) {
        clearTimeout(retryRef.current)
        retryRef.current = setTimeout(fetch_, 15_000)
      }
    } catch { /* silent — cells are supplementary */ }
  }, [product, enabled])

  // Reset when product changes
  useEffect(() => {
    setData({ cells: [], motion_kmh: 0, motion_deg: 0 })
    clearTimeout(retryRef.current)
  }, [product])

  useEffect(() => {
    if (!enabled) return
    fetch_()
    const id = setInterval(fetch_, 60_000)
    return () => { clearInterval(id); clearTimeout(retryRef.current) }
  }, [fetch_, enabled])

  return data
}
