// hooks/useRadar.js
import { useState, useEffect, useCallback, useRef } from 'react'

const API = ''  // Vite proxy → localhost:8000

export const RADAR_PRODUCTS = {
  COMPO_CMAX:  { label: 'CMAX',   desc: 'Max. odbicia (cała Polska)', units: 'dBZ',    color: '#00e5a0' },
  COMPO_EHT:   { label: 'EHT',    desc: 'Wys. wierzchołka burzy',     units: 'km',     color: '#f5a623' },
  COMPO_SRI:   { label: 'SRI',    desc: 'Opad powierzchniowy',         units: 'mm/h',   color: '#60a8ff' },
  COMPO_DSPRI: { label: 'DSPRI',  desc: 'Opad akumulowany',            units: 'mm',     color: '#a060ff' },
  LEG_KDP:     { label: 'KDP',    desc: 'Legionowo — KDP (polaryz.)',  units: 'deg/km', color: '#ff6060' },
  LEG_ZDR:     { label: 'ZDR',    desc: 'Legionowo — ZDR (polaryz.)',  units: 'dB',     color: '#ffd060' },
  LEG_RHOHV:   { label: 'RhoHV',  desc: 'Legionowo — korelacja',      units: '',        color: '#60ffd0' },
}

// Fallback bounds jeśli API niedostępne
const FALLBACK_BOUNDS = {
  COMPO: [[48.13, 11.81], [56.19, 26.37]],
  LEG:   [[50.10, 17.46], [54.59, 24.47]],
  BRZ:   [[47.89, 16.26], [52.90, 23.87]],
  GDY:   [[52.08, 14.72], [57.09, 22.33]],
  GSA:   [[47.95, 14.33], [52.96, 21.94]],
  PAS:   [[47.93, 12.99], [52.94, 20.59]],
  POZ:   [[49.91, 13.00], [54.92, 20.59]],
  RAM:   [[50.89, 18.35], [55.90, 25.96]],
  RZE:   [[47.61, 18.24], [52.62, 25.85]],
  SWI:   [[48.72, 18.91], [53.72, 26.51]],
  UZR:   [[51.36, 17.11], [56.37, 24.72]],
}

function getFallbackBounds(product) {
  const prefix = product.split('_')[0]
  return FALLBACK_BOUNDS[prefix] || FALLBACK_BOUNDS.COMPO
}

export function useRadarBounds(product) {
  const [bounds, setBounds] = useState(() => getFallbackBounds(product))

  useEffect(() => {
    setBounds(getFallbackBounds(product))
    fetch(`${API}/api/radar/${product}/bounds`)
      .then(r => r.json())
      .then(d => {
        if (d.bounds) {
          setBounds([
            [d.bounds.south, d.bounds.west],
            [d.bounds.north, d.bounds.east],
          ])
        }
      })
      .catch(() => {})
  }, [product])

  return bounds
}

export function useRadarImage(product, refreshInterval = 60000) {
  const [imageUrl, setImageUrl]     = useState(null)
  const [meta, setMeta]             = useState(null)
  const [loading, setLoading]       = useState(false)
  const [error, setError]           = useState(null)
  const [lastUpdate, setLastUpdate] = useState(null)
  const abortRef = useRef(null)

  const refresh = useCallback(async () => {
    if (!product) return
    if (abortRef.current) abortRef.current.abort()
    abortRef.current = new AbortController()
    setLoading(true)
    setError(null)
    try {
      const metaRes = await fetch(`${API}/api/radar/${product}/meta`,
        { signal: abortRef.current.signal })
      if (!metaRes.ok) throw new Error(`HTTP ${metaRes.status}`)
      const metaData = await metaRes.json()
      setMeta(metaData)
      const ts = metaData.scan_time || Date.now()
      setImageUrl(`${API}/api/radar/${product}?t=${encodeURIComponent(ts)}&width=900&height=900`)
      setLastUpdate(new Date())
    } catch (e) {
      if (e.name !== 'AbortError') setError(e.message)
    } finally {
      setLoading(false)
    }
  }, [product])

  useEffect(() => { refresh() }, [refresh])
  useEffect(() => {
    const id = setInterval(refresh, refreshInterval)
    return () => clearInterval(id)
  }, [refresh, refreshInterval])

  return { imageUrl, meta, loading, error, lastUpdate, refresh }
}

export function usePointValue(product) {
  const [value, setValue]     = useState(null)
  const [loading, setLoading] = useState(false)

  const query = useCallback(async (lat, lon) => {
    if (!product) return
    setLoading(true)
    try {
      const res = await fetch(
        `${API}/api/radar/${product}/point?lat=${lat.toFixed(5)}&lon=${lon.toFixed(5)}`)
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const data = await res.json()
      setValue({ ...data, lat, lon })
    } catch (e) {
      setValue({ lat, lon, error: e.message })
    } finally {
      setLoading(false)
    }
  }, [product])

  const clear = useCallback(() => setValue(null), [])
  return { value, loading, query, clear }
}
