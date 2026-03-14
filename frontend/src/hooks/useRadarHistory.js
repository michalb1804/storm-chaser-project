// hooks/useRadarHistory.js
import { useState, useEffect, useCallback, useRef } from 'react'

const API = ''

// Flush cache raz na sesję przeglądarkową (nie przy każdym hot-reload w dev)
let _flushed = false
async function flushCacheOnce() {
  if (_flushed) return
  _flushed = true
  try {
    await fetch(`${API}/api/cache/flush`, { method: 'DELETE' })
  } catch (e) {
    console.warn('Cache flush failed:', e.message)
  }
}

export function useRadarHistory(product, limit = 5) {
  const [scans,        setScans]        = useState([])   // od najnowszego
  const [selectedTs,   setSelectedTs]   = useState(null) // wybrany timestamp (null = live)
  const [loading,      setLoading]      = useState(false)
  const abortRef = useRef(null)

  const fetchHistory = useCallback(async () => {
    if (!product) return
    if (abortRef.current) abortRef.current.abort()
    abortRef.current = new AbortController()
    setLoading(true)
    try {
      const res = await fetch(
        `${API}/api/radar/${product}/history?limit=${limit}`,
        { signal: abortRef.current.signal }
      )
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const data = await res.json()
      const newScans = data.scans || []
      setScans(prev => {
        // Nowy skan pojawił się → wróć do live
        if (newScans[0]?.timestamp !== prev[0]?.timestamp) {
          setSelectedTs(null)
        }
        return newScans
      })
    } catch (e) {
      if (e.name !== 'AbortError') console.warn('history fetch:', e.message)
    } finally {
      setLoading(false)
    }
  }, [product, limit])

  useEffect(() => {
    flushCacheOnce().then(() => fetchHistory())
    const id = setInterval(fetchHistory, 60_000)
    return () => { clearInterval(id); abortRef.current?.abort() }
  }, [fetchHistory])

  // Zmiana produktu → resetuj do live
  useEffect(() => {
    setSelectedTs(null)
    setScans([])
  }, [product])

  // null = live (najnowszy), string = konkretny timestamp
  const live     = selectedTs === null
  const selected = live ? scans[0] : scans.find(s => s.timestamp === selectedTs) ?? scans[0]

  const imageUrl = selected
    ? live
      ? `${API}/api/radar/${product}?width=900&height=900&t=${encodeURIComponent(selected.scan_time || '')}`
      : `${API}/api/radar/${product}/scan/${selected.timestamp}?width=900&height=900`
    : null

  return {
    scans,
    selectedTs,
    setSelectedTs,
    scanTime:  selected?.scan_time ?? null,
    imageUrl,
    loading,
    live,
    refresh: fetchHistory,
  }
}

