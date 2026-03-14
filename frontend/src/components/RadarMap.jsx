// components/RadarMap.jsx
import { useEffect, useRef, useState } from 'react'
import L from 'leaflet'
import 'leaflet/dist/leaflet.css'
import RadarWebGL from './RadarWebGL.jsx'

export default function RadarMap({
  product,
  selectedTs,
  onMapClick,
  popupContent,
  opacity = 0.85,
}) {
  const containerRef = useRef(null)
  const mapRef       = useRef(null)
  const popupRef     = useRef(null)
  const [mapReady, setMapReady] = useState(false)

  useEffect(() => {
    if (!containerRef.current || mapRef.current) return

    const map = L.map(containerRef.current, {
      center:           [52.1, 19.5],
      zoom:             6,
      zoomControl:      true,
      attributionControl: true,
    })

    L.tileLayer(
      'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
      {
        attribution: '© OpenStreetMap © CARTO',
        subdomains:  'abcd',
        maxZoom:     19,
        opacity:     0.85,
      }
    ).addTo(map)

    popupRef.current = L.popup({ maxWidth: 320, className: 'radar-popup' })

    map.on('click', e => {
      if (onMapClick) onMapClick(e.latlng.lat, e.latlng.lng)
    })

    mapRef.current = map
    setMapReady(true)

    return () => {
      map.remove()
      mapRef.current = null
      setMapReady(false)
    }
  }, [])

  useEffect(() => {
    const map = mapRef.current
    if (!map || !popupContent || !popupRef.current) return
    const { lat, lon, content } = popupContent
    if (lat == null || lon == null) return
    popupRef.current.setLatLng([lat, lon]).setContent(content).openOn(map)
  }, [popupContent])

  return (
    <div ref={containerRef} style={{ width: '100%', height: '100%' }}>
      {mapReady && mapRef.current && (
        <RadarWebGL
          product={product}
          selectedTs={selectedTs}
          map={mapRef.current}
          opacity={opacity}
        />
      )}
    </div>
  )
}
