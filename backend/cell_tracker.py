"""
Storm cell detection, tracking and motion forecast for IMGW radar data.
"""
import numpy as np
from datetime import datetime

try:
    from scipy import ndimage as _ndi
    def _label(binary):
        return _ndi.label(binary)
    def _closing(binary):
        struct = _ndi.generate_binary_structure(2, 2)
        return _ndi.binary_closing(binary, structure=struct, iterations=2)
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False
    def _label(binary):
        # Simple 2-pass connected components without scipy
        # (fallback, slower)
        from collections import deque
        labeled = np.zeros_like(binary, dtype=np.int32)
        n = 0
        for r in range(binary.shape[0]):
            for c in range(binary.shape[1]):
                if binary[r, c] and not labeled[r, c]:
                    n += 1
                    q = deque([(r, c)])
                    labeled[r, c] = n
                    while q:
                        rr, cc = q.popleft()
                        for dr, dc in ((-1,0),(1,0),(0,-1),(0,1),(-1,-1),(-1,1),(1,-1),(1,1)):
                            nr, nc = rr+dr, cc+dc
                            if 0<=nr<binary.shape[0] and 0<=nc<binary.shape[1]:
                                if binary[nr, nc] and not labeled[nr, nc]:
                                    labeled[nr, nc] = n
                                    q.append((nr, nc))
        return labeled, n
    def _closing(binary):
        return binary  # skip morphological closing without scipy


THRESHOLD_DBZ = 35.0
MIN_PIXELS    = 9     # ~7 km² at 1 km/pixel
MAX_PIXELS    = 50000 # ignore huge noise blobs
FORECAST_MIN  = [15, 30, 45]
MAX_MATCH_DEG = 1.5   # ~150 km max displacement between scans


def _is_reflectivity(quantity: str) -> bool:
    q = quantity.upper()
    return any(k in q for k in ('DBZ', 'TH', 'TV', 'DBZH', 'DBZV'))


def _latlon_at(r: float, c: float, georef: dict):
    """Bilinear interpolation: fractional pixel (r, c) → (lat, lon)."""
    lat_g = georef['lat_grid']
    lon_g = georef['lon_grid']
    h, w  = lat_g.shape
    r = float(np.clip(r, 0, h - 1.001))
    c = float(np.clip(c, 0, w - 1.001))
    r0, c0   = int(r), int(c)
    dr, dc   = r - r0, c - c0
    w00 = (1-dr)*(1-dc); w10 = dr*(1-dc); w01 = (1-dr)*dc; w11 = dr*dc
    lat = lat_g[r0,c0]*w00 + lat_g[r0+1,c0]*w10 + lat_g[r0,c0+1]*w01 + lat_g[r0+1,c0+1]*w11
    lon = lon_g[r0,c0]*w00 + lon_g[r0+1,c0]*w10 + lon_g[r0,c0+1]*w01 + lon_g[r0+1,c0+1]*w11
    return float(lat), float(lon)


def detect_cells(data: np.ndarray, georef: dict) -> list[dict]:
    """Detect convective cells via threshold + connected-component labelling."""
    binary = np.zeros_like(data, dtype=bool)
    binary[np.isfinite(data) & (data >= THRESHOLD_DBZ)] = True

    binary = _closing(binary)
    labeled, n_cells = _label(binary)

    pixel_area_km2 = abs(georef.get('xscale', 1000) * georef.get('yscale', 1000)) / 1e6

    cells = []
    for cid in range(1, n_cells + 1):
        mask  = labeled == cid
        n_px  = int(mask.sum())
        if n_px < MIN_PIXELS or n_px > MAX_PIXELS:
            continue

        rows, cols = np.where(mask)
        vals = data[mask]
        # intensity-weighted centroid
        w = np.maximum(vals - THRESHOLD_DBZ, 0.1)
        r_c = float(np.average(rows, weights=w))
        c_c = float(np.average(cols, weights=w))

        lat, lon = _latlon_at(r_c, c_c, georef)
        cells.append({
            'id':      cid,
            'lat':     lat,
            'lon':     lon,
            'max_dbz': float(np.nanmax(vals)),
            'area_km2': round(n_px * pixel_area_km2, 1),
        })

    return cells


def _match_cells(prev: list[dict], curr: list[dict]) -> list[tuple]:
    """Nearest-neighbour cell matching. Returns list of (prev_cell, curr_cell)."""
    if not prev or not curr:
        return []
    used  = set()
    pairs = []
    for c in sorted(curr, key=lambda x: -x['max_dbz']):
        best_d, best_p = MAX_MATCH_DEG, None
        for p in prev:
            if id(p) in used:
                continue
            d = ((c['lat']-p['lat'])**2 + (c['lon']-p['lon'])**2) ** 0.5
            if d < best_d:
                best_d, best_p = d, p
        if best_p is not None:
            used.add(id(best_p))
            pairs.append((best_p, c))
    return pairs


def _max_dbz_near(data: np.ndarray, georef: dict, lat: float, lon: float, radius_px: int = 20):
    """Max dBZ within radius_px pixels of the given lat/lon."""
    lat_g = georef['lat_grid']
    lon_g = georef['lon_grid']
    dist  = (lat_g - lat) ** 2 + (lon_g - lon) ** 2
    r0, c0 = np.unravel_index(dist.argmin(), dist.shape)
    r1 = max(0, r0 - radius_px);  r2 = min(data.shape[0], r0 + radius_px + 1)
    c1 = max(0, c0 - radius_px);  c2 = min(data.shape[1], c0 + radius_px + 1)
    patch  = data[r1:r2, c1:c2]
    finite = patch[np.isfinite(patch)]
    return float(finite.max()) if len(finite) else None


def _query_latlon(data: np.ndarray, georef: dict, lat: float, lon: float):
    """Nearest-pixel value at lat/lon."""
    lat_g = georef['lat_grid']
    lon_g = georef['lon_grid']
    dist  = (lat_g - lat) ** 2 + (lon_g - lon) ** 2
    r, c  = np.unravel_index(dist.argmin(), dist.shape)
    v     = data[r, c]
    return float(v) if np.isfinite(v) else None


def build_cells_response(
    result_now:  dict,
    result_prev: dict | None,
    all_history: list[dict] | None = None,
    eht_result:  dict | None       = None,
) -> dict:
    """
    Detect cells in result_now, track against result_prev,
    compute forecast positions, dBZ history and echo-top height.
    """
    parsed  = result_now['parsed']
    georef  = result_now['georef']
    scan_dt = result_now.get('scan_dt')

    if georef is None:
        return {'cells': [], 'motion_kmh': 0.0, 'motion_deg': 0.0}
    if not _is_reflectivity(parsed.get('quantity', '')):
        return {'cells': [], 'motion_kmh': 0.0, 'motion_deg': 0.0}

    cells_now = detect_cells(parsed['data'], georef)

    # ── Motion estimate ────────────────────────────────────────────────────
    vel_lat = vel_lon = 0.0
    dt_sec  = None

    if result_prev is not None:
        dt_prev = result_prev.get('scan_dt')
        if scan_dt and dt_prev:
            dt_sec = (scan_dt - dt_prev).total_seconds()
        if dt_sec and dt_sec > 30:
            georef_p = result_prev['georef']
            if georef_p is not None:
                cells_prev = detect_cells(result_prev['parsed']['data'], georef_p)
                pairs = _match_cells(cells_prev, cells_now)
                if pairs:
                    dlat = [(c['lat']-p['lat'])/dt_sec for p, c in pairs]
                    dlon = [(c['lon']-p['lon'])/dt_sec for p, c in pairs]
                    vel_lat = float(np.median(dlat))
                    vel_lon = float(np.median(dlon))

    # ── Speed / direction ──────────────────────────────────────────────────
    mid_lat = float(np.mean([c['lat'] for c in cells_now])) if cells_now else 52.0
    cos_lat = np.cos(np.radians(mid_lat))
    vn = vel_lat * 111.0 * 3600
    ve = vel_lon * 111.0 * cos_lat * 3600
    motion_kmh = float((vn**2 + ve**2) ** 0.5)
    motion_deg = float((np.degrees(np.arctan2(ve, vn)) + 360) % 360)

    # ── Build output ───────────────────────────────────────────────────────
    history_list = all_history or [result_now]

    out_cells = []
    for cell in cells_now:
        # Forecast positions
        forecast = [
            {
                'minutes': fmin,
                'lat':     round(cell['lat'] + vel_lat * fmin * 60, 5),
                'lon':     round(cell['lon'] + vel_lon * fmin * 60, 5),
            }
            for fmin in FORECAST_MIN
        ]

        # dBZ history over last N scans (oldest → newest)
        dbz_history = []
        for res in reversed(history_list):
            g = res.get('georef')
            if g is None:
                continue
            val = _max_dbz_near(res['parsed']['data'], g, cell['lat'], cell['lon'])
            dt  = res.get('scan_dt')
            dbz_history.append({
                'scan_time': dt.isoformat() if dt else None,
                'max_dbz':   round(val, 1) if val is not None else None,
            })

        # Echo-top height from EHT product
        eht_km = None
        if eht_result and eht_result.get('georef'):
            eht_km = _query_latlon(
                eht_result['parsed']['data'],
                eht_result['georef'],
                cell['lat'], cell['lon'],
            )
            if eht_km is not None:
                eht_km = round(eht_km, 1)

        out_cells.append({
            'id':          cell['id'],
            'lat':         round(cell['lat'], 5),
            'lon':         round(cell['lon'], 5),
            'max_dbz':     round(cell['max_dbz'], 1),
            'area_km2':    cell['area_km2'],
            'forecast':    forecast,
            'dbz_history': dbz_history,
            'eht_km':      eht_km,
        })

    return {
        'cells':      out_cells,
        'motion_kmh': round(motion_kmh, 1),
        'motion_deg': round(motion_deg, 1),
        'scan_time':  scan_dt.isoformat() if scan_dt else None,
    }
