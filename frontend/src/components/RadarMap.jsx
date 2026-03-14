// components/RadarMap.jsx
import { useEffect, useRef, useCallback } from 'react'
import L from 'leaflet'
import 'leaflet/dist/leaflet.css'

export default function RadarMap({ imageUrl, bounds, onMapClick, popupContent }) {
  const containerRef = useRef(null)
  const mapRef       = useRef(null)
  const layerRef     = useRef(null)
  const popupRef     = useRef(null)

  // Init mapy
  useEffect(() => {
    if (!containerRef.current || mapRef.current) return

    const map = L.map(containerRef.current, {
      center: [52.1, 19.5],
      zoom:   6,
      zoomControl: true,
      attributionControl: true,
    })

    // Ciemna warstwa bazowa — CartoDB Dark Matter
    L.tileLayer(
      'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
      {
        attribution: '© OpenStreetMap © CARTO',
        subdomains:  'abcd',
        maxZoom:     19,
        opacity:     0.85,
      }
    ).addTo(map)

    // Popup do kliknięć
    popupRef.current = L.popup({ maxWidth: 320, className: 'radar-popup' })

    // Kliknięcie na mapę
    map.on('click', (e) => {
      if (onMapClick) onMapClick(e.latlng.lat, e.latlng.lng)
    })

    mapRef.current = map
    return () => { map.remove(); mapRef.current = null }
  }, [])

  // Aktualizacja warstwy radarowej
  useEffect(() => {
    const map = mapRef.current
    if (!map || !imageUrl || !bounds) return

    // Usuń starą warstwę
    if (layerRef.current) {
      map.removeLayer(layerRef.current)
    }

    const layer = L.imageOverlay(imageUrl, bounds, {
      opacity:      0.85,
      interactive:  false,
      crossOrigin:  true,
    })

    layer.on('load',  () => { layer.setOpacity(0.85) })
    layer.on('error', () => console.warn('Błąd ładowania obrazu radarowego'))

    layer.addTo(map)
    layerRef.current = layer
  }, [imageUrl, bounds])

  // Popup przy kliknięciu
  useEffect(() => {
    const map = mapRef.current
    if (!map || !popupContent || !popupRef.current) return

    const { lat, lon, content } = popupContent
    if (lat == null || lon == null) return

    popupRef.current
      .setLatLng([lat, lon])
      .setContent(content)
      .openOn(map)
  }, [popupContent])

  return (
    <div
      ref={containerRef}
      style={{ width: '100%', height: '100%' }}
    />
  )
}
