// hooks/useVelocityCouplet.js
// Hook do pomiaru Velocity Couplet - różnicy prędkości radialnej między dwoma punktami
import { useState, useCallback, useEffect, useRef } from 'react'

const API = ''  // Vite proxy → localhost:8000

// Interpretacja siły coupletu (na podstawie delta velocity)
function interpretCouplet(deltaMs) {
  const absDelta = Math.abs(deltaMs)
  if (absDelta < 15) {
    return {
      level: 'weak',
      label: 'Słaby',
      color: '#60a8ff',
      description: 'Brak wyraźnej rotacji'
    }
  } else if (absDelta < 30) {
    return {
      level: 'moderate',
      label: 'Umiarkowany',
      color: '#f5a623',
      description: 'Możliwa słaba rotacja'
    }
  } else if (absDelta < 45) {
    return {
      level: 'strong',
      label: 'Silny',
      color: '#ff8040',
      description: 'Prawdopodobny mezocyklon'
    }
  } else {
    return {
      level: 'severe',
      label: 'Bardzo silny',
      color: '#ff4040',
      description: 'Wysokie ryzyko trąby powietrznej!'
    }
  }
}

// Oblicz odległość między dwoma punktami (w km) - wzór Haversine
function distanceKm(lat1, lon1, lat2, lon2) {
  const R = 6371 // km
  const dLat = (lat2 - lat1) * Math.PI / 180
  const dLon = (lon2 - lon1) * Math.PI / 180
  const a = Math.sin(dLat/2) * Math.sin(dLat/2) +
            Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
            Math.sin(dLon/2) * Math.sin(dLon/2)
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a))
  return R * c
}

export function useVelocityCouplet(product, selectedTs) {
  const [points, setPoints] = useState([]) // [{lat, lon, value, loading}, ...]
  const [result, setResult] = useState(null)
  const [isActive, setIsActive] = useState(false)
  const abortRefs = useRef([])

  // Sprawdź czy produkt to velocity
  const isVelocityProduct = useCallback(() => {
    const p = (product || '').toUpperCase()
    return p.includes('CAPPI_V') || p.includes('VRAD') || p.endsWith('_V')
  }, [product])

  // Pobierz wartość velocity w punkcie
  const fetchPointValue = useCallback(async (lat, lon, pointIndex) => {
    if (!product) return null

    // Anuluj poprzednie zapytanie dla tego punktu
    if (abortRefs.current[pointIndex]) {
      abortRefs.current[pointIndex].abort()
    }
    abortRefs.current[pointIndex] = new AbortController()

    try {
      const qs = selectedTs ? `&scan_time=${encodeURIComponent(selectedTs)}` : ''
      const res = await fetch(
        `${API}/api/radar/${product}/point?lat=${lat.toFixed(5)}&lon=${lon.toFixed(5)}${qs}`,
        { signal: abortRefs.current[pointIndex].signal }
      )
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const data = await res.json()
      return data
    } catch (e) {
      if (e.name !== 'AbortError') {
        console.error('Couplet point fetch error:', e.message)
      }
      return null
    }
  }, [product, selectedTs])

  // Dodaj punkt
  const addPoint = useCallback(async (lat, lon) => {
    if (!isActive) return
    if (points.length >= 2) {
      // Reset jeśli już mamy 2 punkty
      setPoints([{ lat, lon, value: null, loading: true }])
      setResult(null)
    } else {
      setPoints(prev => [...prev, { lat, lon, value: null, loading: true }])
    }
  }, [isActive, points.length])

  // Pobierz wartości dla punktów
  useEffect(() => {
    if (!isActive || points.length === 0) return

    points.forEach(async (point, index) => {
      if (point.value !== null || !point.loading) return

      const data = await fetchPointValue(point.lat, point.lon, index)
      if (data) {
        setPoints(prev => {
          const newPoints = [...prev]
          newPoints[index] = {
            ...newPoints[index],
            value: data.value,
            no_signal: data.no_signal,
            quantity: data.quantity,
            loading: false
          }
          return newPoints
        })
      }
    })
  }, [points, isActive, fetchPointValue])

  // Oblicz wynik gdy mamy 2 punkty z wartościami
  useEffect(() => {
    if (points.length !== 2) {
      setResult(null)
      return
    }

    const [p1, p2] = points
    if (p1.loading || p2.loading || p1.value == null || p2.value == null) {
      setResult(null)
      return
    }

    // Jeśli brak sygnału w którymś punkcie
    if (p1.no_signal || p2.no_signal) {
      setResult({
        error: 'Brak sygnału w jednym z punktów',
        points: points
      })
      return
    }

    const delta = p2.value - p1.value
    const absDelta = Math.abs(delta)
    const dist = distanceKm(p1.lat, p1.lon, p2.lat, p2.lon)
    const interpretation = interpretCouplet(delta)

    setResult({
      point1: { ...p1 },
      point2: { ...p2 },
      deltaVelocity: delta,
      absDeltaVelocity: absDelta,
      distanceKm: dist,
      interpretation,
      points
    })
  }, [points])

  // Wyczyść
  const clear = useCallback(() => {
    setPoints([])
    setResult(null)
  }, [])

  // Toggle tryb
  const toggleActive = useCallback(() => {
    setIsActive(prev => {
      if (prev) {
        // Wyłączamy - czyścimy
        setPoints([])
        setResult(null)
      }
      return !prev
    })
  }, [])

  // Cleanup
  useEffect(() => {
    return () => {
      abortRefs.current.forEach(abort => abort?.abort())
    }
  }, [])

  return {
    isActive,
    toggleActive,
    points,
    result,
    addPoint,
    clear,
    isVelocityProduct: isVelocityProduct()
  }
}