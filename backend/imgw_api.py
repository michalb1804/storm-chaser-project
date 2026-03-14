"""
IMGW Radar + GFS API
--------------------
FastAPI backend serwujący dane radarowe i GFS on-demand przez cache managery.

Uruchom:
    pip install fastapi uvicorn[standard] pillow
    uvicorn imgw_api:app --reload --port 8000

Dokumentacja (auto):
    http://localhost:8000/docs
    http://localhost:8000/redoc

Endpointy radarowe:
    GET /api/radar/{product}            — PNG + metadane (główny)
    GET /api/radar/{product}/meta       — tylko metadane (bez obrazu)
    GET /api/radar/{product}/point      — wartość w punkcie lat/lon
    GET /api/radar/{product}/bounds     — zasięg geograficzny siatki
    GET /api/radar                      — lista dostępnych produktów

Endpointy GFS:
    GET /api/gfs/{param}                — PNG mapy parametru + metadane
    GET /api/gfs/{param}/meta           — tylko metadane (bez obrazu)
    GET /api/gfs/{param}/point          — wartość w punkcie lat/lon
    GET /api/gfs                        — lista dostępnych parametrów
    GET /api/gfs/run/current            — czas bieżącego runu GFS

Cache:
    GET /api/cache/status               — stan cache radarowego
    GET /api/gfs/cache/status           — stan cache GFS
    DELETE /api/cache/{product}         — wymuś odświeżenie radaru
    DELETE /api/gfs/cache/{param}       — wymuś odświeżenie GFS
    POST /api/cache/cleanup             — usuń stare pliki radarowe
    POST /api/gfs/cache/cleanup         — usuń stare pliki GFS

    GET /health                         — health check
"""

import io
import os
import time
import logging
from datetime import datetime, timezone
from typing import Any

import numpy as np
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from imgw_cache import get_cache, CACHE_TTL_S
from imgw_radar import PRODUCTS, query_point, latlon_to_pixel, nws_dbz_cmap
from gfs_cache import get_gfs_cache
from gfs_ingestor import PARAMS as GFS_PARAMS, PARAM_GROUPS as GFS_GROUPS, DERIVED_PARAMS as GFS_DERIVED

log = logging.getLogger("api")

# ── Aplikacja ─────────────────────────────────────────────────────────────────

app = FastAPI(
    title="IMGW Radar + GFS API",
    description="On-demand dane radarowe IMGW-PIB i parametry konwekcyjne GFS",
    version="0.2.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173",
                   "http://127.0.0.1:3000", "http://127.0.0.1:5173"],
    allow_methods=["GET", "DELETE"],
    allow_headers=["*"],
)

# ── Helpers ───────────────────────────────────────────────────────────────────

def _decode(val: Any) -> Any:
    """Dekoduje bytes/np.bytes_ na str (dla atrybutów ODIM)."""
    if isinstance(val, (bytes, np.bytes_)):
        return val.decode()
    if isinstance(val, np.integer):
        return int(val)
    if isinstance(val, np.floating):
        return float(val)
    return val


def _scan_meta(result: dict) -> dict:
    """Buduje słownik metadanych ze sparsowanego skanu."""
    parsed  = result["parsed"]
    georef  = result["georef"]
    data    = parsed["data"]
    what    = {k: _decode(v) for k, v in parsed.get("what", {}).items()}

    meta = {
        "product":   parsed["quantity"],
        "scan_time": result["scan_dt"].isoformat() if result["scan_dt"] else None,
        "shape":     list(parsed["shape"]),
        "quantity":  parsed["quantity"],
        "gain":      parsed["gain"],
        "offset":    parsed["offset"],
        "nodata":    parsed["nodata"],
        "undetect":  parsed["undetect"],
        "val_min":   None if np.all(np.isnan(data)) else float(np.nanmin(data)),
        "val_max":   None if np.all(np.isnan(data)) else float(np.nanmax(data)),
        "val_mean":  None if np.all(np.isnan(data)) else float(np.nanmean(data)),
        "nan_pct":   float(100 * np.isnan(data).mean()),
        "cache_age_s":   round(result["age_s"], 1),
        "cache_fresh":   result["fresh"],
        "cache_ttl_s":   CACHE_TTL_S,
        "file":      os.path.basename(result["path"]),
        "odim_what": what,
        "attribution": "Źródłem danych jest Instytut Meteorologii i Gospodarki Wodnej – PIB",
    }

    if georef is not None:
        meta["georef"] = {
            "projection": "aeqd",
            "xsize":  georef["xsize"],
            "ysize":  georef["ysize"],
            "xscale_m": round(georef["xscale"], 2),
            "yscale_m": round(georef["yscale"], 2),
            "lat_min": float(georef["lat_grid"].min()),
            "lat_max": float(georef["lat_grid"].max()),
            "lon_min": float(georef["lon_grid"].min()),
            "lon_max": float(georef["lon_grid"].max()),
        }
    else:
        meta["georef"] = None

    return meta


def _render_png(result: dict, width: int = 900, height: int = 900) -> bytes:
    """
    Renderuje dane radarowe jako PNG (RGBA, NaN = przezroczysty).
    Zwraca bytes gotowe do wysłania jako image/png.
    """
    try:
        from PIL import Image
    except ImportError:
        raise HTTPException(500, "Pillow niedostępny: pip install pillow")

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors

    parsed   = result["parsed"]
    data     = parsed["data"]
    quantity = parsed["quantity"]

    # Dobierz paletę
    if "DBZ" in quantity.upper():
        cmap, vmin, vmax = nws_dbz_cmap()
    else:
        cmap = plt.cm.turbo
        cmap.set_bad(color=(0, 0, 0, 0))
        vmin = float(np.nanmin(data)) if not np.all(np.isnan(data)) else 0
        vmax = float(np.nanmax(data)) if not np.all(np.isnan(data)) else 1

    # Normalizuj dane → RGBA
    norm    = mcolors.Normalize(vmin=vmin, vmax=vmax, clip=False)
    mapper  = plt.cm.ScalarMappable(norm=norm, cmap=cmap)
    rgba    = mapper.to_rgba(data, bytes=True)   # (H, W, 4) uint8
    rgba[np.isnan(data)] = [0, 0, 0, 0]          # NaN → przezroczysty

    # Skaluj do żądanego rozmiaru
    img = Image.fromarray(rgba, mode="RGBA")
    if img.size != (width, height):
        img = img.resize((width, height), Image.NEAREST)

    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=False)
    return buf.getvalue()


# ── Endpointy ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    """Health check — zawsze zwraca 200 jeśli serwer działa."""
    return {
        "status": "ok",
        "time":   datetime.now(timezone.utc).isoformat(),
        "cache_ttl_s": CACHE_TTL_S,
    }


@app.get("/api/radar")
def list_products():
    """
    Lista wszystkich dostępnych produktów radarowych wraz ze statusem cache.
    """
    cache    = get_cache()
    statuses = {s["product"]: s for s in cache.status_all()}

    result = {}
    for key, (folder, suffix) in PRODUCTS.items():
        s = statuses.get(key, {})
        result[key] = {
            "folder":  folder,
            "suffix":  suffix,
            "type":    "compo" if key.startswith("COMPO_") else "individual",
            "radar":   key.split("_")[0].lower() if not key.startswith("COMPO_") else None,
            "cache_status":    s.get("status", "unknown"),
            "cache_age_s":     s.get("age_s"),
            "cache_remaining": s.get("remaining"),
        }
    return result


@app.get("/api/radar/{product}/meta")
def radar_meta(product: str):
    """
    Metadane ostatniego skanu dla produktu — bez obrazu.
    Szybkie, używaj do sprawdzenia czasu skanu przed pobraniem PNG.
    """
    product = product.upper()
    if product not in PRODUCTS:
        raise HTTPException(404, f"Nieznany produkt: {product}. "
                                 f"Zobacz /api/radar po listę.")

    cache  = get_cache()
    result = cache.get(product)
    if result is None:
        raise HTTPException(503, f"Nie udało się pobrać danych dla {product}")

    return _scan_meta(result)


@app.get("/api/radar/{product}/bounds")
def radar_bounds(product: str):
    """
    Zasięg geograficzny siatki radarowej (narożniki w WGS84).
    Przydatne do inicjalizacji mapy Leaflet (fitBounds).
    """
    product = product.upper()
    if product not in PRODUCTS:
        raise HTTPException(404, f"Nieznany produkt: {product}")

    cache  = get_cache()
    result = cache.get(product)
    if result is None:
        raise HTTPException(503, f"Nie udało się pobrać danych dla {product}")

    georef = result["georef"]
    if georef is None:
        raise HTTPException(422, f"Brak georeferencji dla {product}")

    return {
        "product": product,
        "bounds": {
            "south": float(georef["lat_grid"].min()),
            "north": float(georef["lat_grid"].max()),
            "west":  float(georef["lon_grid"].min()),
            "east":  float(georef["lon_grid"].max()),
        },
        "corners": {
            "UL": [float(georef["lat_grid"][0,  0]),   float(georef["lon_grid"][0,  0])],
            "UR": [float(georef["lat_grid"][0,  -1]),  float(georef["lon_grid"][0,  -1])],
            "LL": [float(georef["lat_grid"][-1, 0]),   float(georef["lon_grid"][-1, 0])],
            "LR": [float(georef["lat_grid"][-1, -1]),  float(georef["lon_grid"][-1, -1])],
        },
        "shape": [georef["ysize"], georef["xsize"]],
        "xscale_m": round(georef["xscale"], 1),
        "yscale_m": round(georef["yscale"], 1),
    }


@app.get("/api/radar/{product}/point")
def radar_point(
    product: str,
    lat: float = Query(..., description="Szerokość geograficzna", ge=40.0, le=65.0),
    lon: float = Query(..., description="Długość geograficzna",   ge=5.0,  le=35.0),
):
    """
    Wartość radarowa (np. dBZ) dla podanego punktu lat/lon.

    Przykład: /api/radar/COMPO_CMAX/point?lat=52.23&lon=21.01
    """
    product = product.upper()
    if product not in PRODUCTS:
        raise HTTPException(404, f"Nieznany produkt: {product}")

    cache  = get_cache()
    result = cache.get(product)
    if result is None:
        raise HTTPException(503, f"Nie udało się pobrać danych dla {product}")

    georef = result["georef"]
    if georef is None:
        raise HTTPException(422, f"Brak georeferencji dla {product}")

    parsed = result["parsed"]
    rc     = latlon_to_pixel(georef, lat, lon)
    if rc is None:
        raise HTTPException(422, f"Punkt ({lat}, {lon}) poza zasięgiem siatki")

    row, col = rc
    val      = query_point(parsed, georef, lat, lon)

    return {
        "product":   product,
        "quantity":  parsed["quantity"],
        "lat":       lat,
        "lon":       lon,
        "value":     val,
        "no_signal": val is None,
        "pixel":     {"row": row, "col": col},
        "scan_time": result["scan_dt"].isoformat() if result["scan_dt"] else None,
        "cache_age_s": round(result["age_s"], 1),
    }


@app.get("/api/radar/{product}")
def radar_image(
    product: str,
    width:   int = Query(900,  ge=64,  le=2048, description="Szerokość PNG [px]"),
    height:  int = Query(900,  ge=64,  le=2048, description="Wysokość PNG [px]"),
    meta:    bool = Query(False, description="Czy dołączyć metadane w nagłówku X-Radar-Meta"),
):
    """
    Główny endpoint — zwraca obraz radarowy jako PNG (RGBA).
    NaN (brak sygnału) = przezroczysty piksel.

    Nagłówki odpowiedzi:
        X-Radar-Scan-Time   — czas skanu UTC (ISO8601)
        X-Radar-Cache-Fresh — True/False
        X-Radar-Cache-Age   — wiek danych w sekundach
        X-Radar-Val-Max     — maksymalna wartość w skanie
        X-Radar-Quantity    — nazwa ilości (DBZH, KDP, ...)

    Przykład użycia w Leaflet:
        L.imageOverlay('/api/radar/COMPO_CMAX', bounds).addTo(map)
    """
    product = product.upper()
    if product not in PRODUCTS:
        raise HTTPException(404, f"Nieznany produkt: {product}. "
                                 f"Zobacz /api/radar po listę.")

    cache  = get_cache()
    t0     = time.monotonic()
    result = cache.get(product)
    elapsed = time.monotonic() - t0

    if result is None:
        raise HTTPException(503, f"Nie udało się pobrać danych dla {product}. "
                                 "Sprawdź połączenie z internetem lub spróbuj za chwilę.")

    png_bytes = _render_png(result, width=width, height=height)

    parsed   = result["parsed"]
    data     = parsed["data"]
    scan_dt  = result["scan_dt"]

    headers = {
        "X-Radar-Scan-Time":   scan_dt.isoformat() if scan_dt else "",
        "X-Radar-Cache-Fresh": str(result["fresh"]),
        "X-Radar-Cache-Age":   str(round(result["age_s"], 1)),
        "X-Radar-Val-Max":     str(round(float(np.nanmax(data)), 1))
                               if not np.all(np.isnan(data)) else "nan",
        "X-Radar-Quantity":    parsed["quantity"],
        "X-Render-Time-Ms":    str(round(elapsed * 1000)),
        "Cache-Control":       f"public, max-age={CACHE_TTL_S}",
    }

    if meta:
        import json
        headers["X-Radar-Meta"] = json.dumps(_scan_meta(result))

    return Response(
        content=png_bytes,
        media_type="image/png",
        headers=headers,
    )


# ── Cache management ──────────────────────────────────────────────────────────

@app.get("/api/cache/status")
def cache_status(products: str = Query(None,
    description="Przecinkowa lista produktów, np. COMPO_CMAX,COMPO_EHT. "
                "Domyślnie wszystkie.")):
    """Stan cache dla wszystkich lub wybranych produktów."""
    cache = get_cache()
    keys  = [p.strip().upper() for p in products.split(",")] \
            if products else list(PRODUCTS.keys())
    return cache.status_all(keys)


@app.delete("/api/cache/{product}")
def cache_invalidate(product: str):
    """
    Usuwa plik z cache — wymusza pobranie przy następnym żądaniu.
    Przydatne gdy chcesz natychmiast odświeżyć dane.
    """
    product = product.upper()
    if product not in PRODUCTS:
        raise HTTPException(404, f"Nieznany produkt: {product}")

    get_cache().invalidate(product)
    return {"product": product, "invalidated": True}


@app.post("/api/cache/cleanup")
def cache_cleanup(keep_last: int = Query(3, ge=1, le=20,
    description="Ile najnowszych plików zostawić per produkt")):
    """Usuwa stare pliki HDF5 zostawiając keep_last najnowszych per produkt."""
    removed = get_cache().cleanup(keep_last=keep_last)
    return {"removed_files": removed, "keep_last": keep_last}



@app.get("/api/radar/{product}/history")
def radar_history(
    product: str,
    limit: int = Query(5, ge=1, le=20, description="Liczba skanów"),
):
    """
    Lista dostępnych skanów dla produktu (z lokalnego cache).
    Zwraca do `limit` najnowszych skanów z timestampami.
    Używaj do budowania suwaka historii w frontendzie.
    """
    product = product.upper()
    if product not in PRODUCTS:
        raise HTTPException(404, f"Nieznany produkt: {product}")

    scans = get_cache().history(product, limit=limit)
    return {
        "product": product,
        "count":   len(scans),
        "scans":   scans,
    }


@app.get("/api/radar/{product}/scan/{scan_time}")
def radar_by_scan_time(
    product:   str,
    scan_time: str,
    width:  int  = Query(900, ge=64, le=2048),
    height: int  = Query(900, ge=64, le=2048),
    meta:   bool = Query(False),
):
    """
    Obraz radarowy dla konkretnego timestampu skanu.
    scan_time: ISO8601 (2026-03-14T08:15:00) lub timestamp (2026031408150000).
    Skan musi być dostępny lokalnie (z history).
    """
    product = product.upper()
    if product not in PRODUCTS:
        raise HTTPException(404, f"Nieznany produkt: {product}")

    result = get_cache().get_by_scan_time(product, scan_time)
    if result is None:
        raise HTTPException(404,
            f"Skan {scan_time} niedostępny. Sprawdź /api/radar/{product}/history.")

    parsed  = result["parsed"]
    data    = parsed["data"]
    georef  = result["georef"]
    scan_dt = result["scan_dt"]

    png_bytes = _render_png(result, width=width, height=height)

    headers = {
        "X-Radar-Scan-Time":   scan_dt.isoformat() if scan_dt else "",
        "X-Radar-Cache-Fresh": "True",
        "X-Radar-Cache-Age":   str(round(result["age_s"], 1)),
        "X-Radar-Val-Max":     str(round(float(np.nanmax(data)), 1))
                               if not np.all(np.isnan(data)) else "nan",
        "X-Radar-Quantity":    parsed["quantity"],
        "Cache-Control":       "public, max-age=86400",  # archiwalne — cache 24h
    }

    if meta:
        import json
        headers["X-Radar-Meta"] = json.dumps(_scan_meta(result))

    return Response(content=png_bytes, media_type="image/png", headers=headers)

# ══════════════════════════════════════════════════════════════════════════════
# GFS ENDPOINTY
# ══════════════════════════════════════════════════════════════════════════════

# ── GFS helpers ───────────────────────────────────────────────────────────────

# Palety kolorów dla parametrów GFS
_GFS_PALETTES = {
    # CAPE/CIN
    "CAPE":    ("YlOrRd",     0,    4000),
    "CIN":     ("Blues_r",  -400,      0),
    # Temperatura
    "TMP":     ("RdYlBu_r", 250,    310),
    "DPT":     ("YlGn",     250,    295),
    "RH":      ("YlGnBu",     0,    100),
    # Wiatr / shear
    "SHEAR":   ("plasma",     0,     30),
    "UGRD":    ("RdBu_r",   -30,     30),
    "VGRD":    ("RdBu_r",   -30,     30),
    "GUST":    ("YlOrRd",     0,     30),
    # SRH / helicity
    "HLCY":    ("OrRd",       0,    500),
    "SRH":     ("OrRd",       0,    500),
    # Indeksy
    "KINDEX":  ("RdYlGn",   -20,     40),
    "TT":      ("RdYlGn",    30,     60),
    "LFTX":    ("RdYlGn",   -10,     15),
    "SWEAT":   ("YlOrRd",     0,    400),
    # Parametry kompozytowe
    "SCP":     ("plasma",     0,      8),
    "STP":     ("plasma",     0,      5),
    "SHIP":    ("YlOrRd",     0,      2),
    "EHI":     ("plasma",     0,      5),
    "BRN":     ("RdYlGn_r",  0,     60),
    "DCP":     ("YlOrRd",    0,      3),
    # Inne
    "HGT":     ("viridis",    0,   6000),
    "PWAT":    ("Blues",      0,     60),
    "HPBL":    ("YlOrBr",     0,   3000),
    "MSLP":    ("RdBu_r",  9800,  10400),
    "LCL":     ("YlGnBu_r",  0,   3000),
}

def _gfs_cmap(param_key: str):
    """Dobiera paletę i zakres dla parametru GFS."""
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors

    # Szukaj po prefiksie klucza
    for prefix, (cmap_name, vmin, vmax) in _GFS_PALETTES.items():
        if prefix in param_key.upper():
            cmap = plt.cm.get_cmap(cmap_name)
            cmap.set_bad(color=(0, 0, 0, 0))
            return cmap, vmin, vmax

    # Fallback
    cmap = plt.cm.turbo
    cmap.set_bad(color=(0, 0, 0, 0))
    return cmap, None, None


def _gfs_meta(param_key: str, entry: dict, fxx: int,
              run_time: str | None, age_s: float | None) -> dict:
    """Buduje słownik metadanych dla parametru GFS."""
    data = entry.get("data")
    lats = entry.get("lats")
    lons = entry.get("lons")

    geo = None
    if lats is not None and lons is not None:
        geo = {
            "lat_min": float(np.nanmin(lats)),
            "lat_max": float(np.nanmax(lats)),
            "lon_min": float(np.nanmin(lons)),
            "lon_max": float(np.nanmax(lons)),
            "shape":   list(data.shape) if data is not None else None,
        }

    return {
        "param":      param_key,
        "label":      entry.get("label", param_key),
        "units":      entry.get("units", ""),
        "desc":       entry.get("desc", ""),
        "derived":    entry.get("derived", False),
        "fxx":        fxx,
        "run_time":   run_time,
        "val_min":    entry.get("val_min"),
        "val_max":    entry.get("val_max"),
        "val_mean":   entry.get("val_mean"),
        "georef":     geo,
        "cache_age_s": round(age_s, 1) if age_s is not None else None,
        "attribution": "NOAA GFS — dane publiczne / public domain",
    }


def _upsample_gfs(data: np.ndarray, lats: np.ndarray, lons: np.ndarray,
                   factor: int = 8) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Upsampling siatki GFS przez interpolację bikubiczną.
    factor=8 → z ~28km do ~3.5km rozdzielczości.
    Używa scipy.interpolate.RegularGridInterpolator.
    """
    try:
        from scipy.interpolate import RegularGridInterpolator
    except ImportError:
        return data, lats, lons   # fallback bez interpolacji

    # GFS ma siatki 1D lat/lon (regularna siatka)
    lat_1d = lats if lats.ndim == 1 else lats[:, 0]
    lon_1d = lons if lons.ndim == 1 else lons[0, :]

    # Upewnij się że lat jest rosnące (wymagane przez interpolator)
    if lat_1d[0] > lat_1d[-1]:
        lat_1d = lat_1d[::-1]
        data   = data[::-1, :]

    # Zastąp NaN interpolowanymi wartościami (np.nan psuje interpolację)
    data_filled = data.copy()
    nan_mask    = np.isnan(data_filled)
    if nan_mask.any():
        # Prosta interpolacja NaN: wypełnij medianą sąsiadów
        from scipy.ndimage import generic_filter
        data_filled[nan_mask] = np.nanmedian(data_filled)

    interp = RegularGridInterpolator(
        (lat_1d, lon_1d), data_filled,
        method="linear", bounds_error=False, fill_value=np.nan
    )

    # Nowa siatka z wyższą rozdzielczością
    lat_new = np.linspace(lat_1d[0],  lat_1d[-1],  len(lat_1d)  * factor)
    lon_new = np.linspace(lon_1d[0],  lon_1d[-1],  len(lon_1d)  * factor)
    lon_grid, lat_grid = np.meshgrid(lon_new, lat_new)

    data_up = interp((lat_grid, lon_grid))

    # Przywróć NaN tam gdzie oryginał miał NaN (interpolowane z sąsiadów)
    # — zostawiamy bez NaN bo interpolacja je wygładziła poprawnie

    return data_up, lat_new, lon_new


def _render_gfs_png(entry: dict, param_key: str,
                    width: int = 512, height: int = 512,
                    interp: str = "bilinear") -> bytes:
    """
    Renderuje parametr GFS jako PNG (RGBA, NaN = przezroczysty).

    interp:
      "nearest"  — pikseloza, szybkie, wierne oryginałowi
      "bilinear" — wbudowana interpolacja PIL (rozmyte)
      "scipy"    — upsampling RegularGridInterpolator przed renderem (najlepsza jakość)
    """
    try:
        from PIL import Image
    except ImportError:
        raise HTTPException(500, "Pillow niedostępny: pip install pillow")

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors

    data = entry.get("data")
    lats = entry.get("lats")
    lons = entry.get("lons")
    if data is None:
        raise HTTPException(503, "Brak danych do renderowania")

    # Upsampling scipy przed kolorowaniem — najlepsza jakość
    if interp == "scipy" and lats is not None and lons is not None:
        data, lats, lons = _upsample_gfs(data, lats, lons, factor=8)

    cmap, vmin, vmax = _gfs_cmap(param_key)
    if vmin is None:
        vmin = float(np.nanmin(data)) if not np.all(np.isnan(data)) else 0
        vmax = float(np.nanmax(data)) if not np.all(np.isnan(data)) else 1

    norm   = mcolors.Normalize(vmin=vmin, vmax=vmax, clip=False)
    mapper = plt.cm.ScalarMappable(norm=norm, cmap=cmap)
    rgba   = mapper.to_rgba(data, bytes=True)
    rgba[np.isnan(data)] = [0, 0, 0, 0]

    img = Image.fromarray(rgba, mode="RGBA")
    if img.size != (width, height):
        resample = Image.NEAREST if interp == "nearest" else Image.LANCZOS
        img = img.resize((width, height), resample)

    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=False)
    return buf.getvalue()


def _all_gfs_param_keys() -> list[str]:
    """Lista wszystkich kluczy parametrów GFS (bezpośrednie + pochodne)."""
    from gfs_derived import DERIVED_CATALOG
    return list(GFS_PARAMS.keys()) + list(DERIVED_CATALOG.keys())


def _resolve_gfs_param(param: str) -> str:
    """Normalizuje klucz parametru — dodaje _derived suffix jeśli potrzeba."""
    from gfs_derived import DERIVED_CATALOG
    if param in GFS_PARAMS:
        return param
    if param in DERIVED_CATALOG:
        return param
    # Spróbuj z suffixem _derived
    derived_key = param if param.endswith("_derived") else f"{param}_derived"
    if derived_key in DERIVED_CATALOG:
        return derived_key
    return param.upper()


# ── GFS endpointy ─────────────────────────────────────────────────────────────

@app.get("/api/gfs")
def list_gfs_params():
    """
    Lista wszystkich dostępnych parametrów GFS (bezpośrednich i pochodnych).
    """
    from gfs_derived import DERIVED_CATALOG

    result = {}

    for key, p in GFS_PARAMS.items():
        result[key] = {
            "label":   p["label"],
            "units":   p["units"],
            "desc":    p["desc"],
            "product": p["product"],
            "derived": False,
        }

    for key in DERIVED_CATALOG:
        result[key] = {
            "label":   key.replace("_derived", ""),
            "units":   "",
            "desc":    "Parametr pochodny obliczany z danych GFS",
            "product": "derived",
            "derived": True,
        }

    return result


@app.get("/api/gfs/run/current")
def gfs_current_run():
    """Czas bieżącego i następnego runu GFS."""
    from gfs_ingestor import get_latest_run_time
    from datetime import timedelta

    current = get_latest_run_time()
    next_run = current + timedelta(hours=6)
    now      = datetime.now(timezone.utc).replace(tzinfo=None)

    return {
        "current_run":  current.strftime("%Y-%m-%d %Hz UTC"),
        "next_run":     next_run.strftime("%Y-%m-%d %Hz UTC"),
        "age_h":        round((now - current).total_seconds() / 3600, 2),
        "next_in_min":  round((next_run - now).total_seconds() / 60),
    }


@app.get("/api/gfs/cache/status")
def gfs_cache_status(fxx: str = Query("0,6,24",
    description="Przecinkowa lista godzin prognozy, np. 0,6,12,24,48")):
    """Stan cache GFS dla podanych godzin prognozy."""
    fxx_list = [int(f.strip()) for f in fxx.split(",")]
    return get_gfs_cache().status_all(fxx_list)


@app.get("/api/gfs/cache/runs")
def gfs_cache_runs():
    """Lista wszystkich dostępnych runów w cache GFS."""
    return get_gfs_cache().list_cached_runs()


@app.delete("/api/gfs/cache/{param}")
def gfs_cache_invalidate(param: str,
                          fxx: int = Query(0, description="Godzina prognozy")):
    """Wymuś odświeżenie parametru GFS przy następnym żądaniu."""
    get_gfs_cache().invalidate(fxx=fxx)
    return {"param": param, "fxx": fxx, "invalidated": True}


@app.post("/api/gfs/cache/cleanup")
def gfs_cache_cleanup(keep_runs: int = Query(4, ge=1, le=10,
    description="Ile ostatnich runów zostawić")):
    """Usuwa stare pliki cache GFS."""
    removed = get_gfs_cache().cleanup(keep_runs=keep_runs)
    return {"removed_files": removed, "keep_runs": keep_runs}


@app.get("/api/gfs/{param}/meta")
def gfs_meta(
    param: str,
    fxx: int = Query(0, ge=0, le=240, description="Godzina prognozy (0-240)"),
):
    """
    Metadane parametru GFS — bez obrazu.
    Szybkie, używaj do sprawdzenia zakresu wartości przed pobraniem PNG.
    """
    param_key = _resolve_gfs_param(param)
    all_keys  = _all_gfs_param_keys()
    if param_key not in all_keys:
        raise HTTPException(404, f"Nieznany parametr: {param}. "
                                 f"Zobacz /api/gfs po listę.")

    cache  = get_gfs_cache()
    t0     = time.monotonic()
    entry  = cache.get(param_key, fxx=fxx)
    elapsed = time.monotonic() - t0

    if entry is None:
        raise HTTPException(503, f"Nie udało się pobrać {param_key} fxx={fxx}")

    s = cache.status(fxx=fxx)
    meta = _gfs_meta(param_key, entry, fxx,
                     run_time=s.get("run_time"),
                     age_s=s.get("age_s"))
    meta["fetch_ms"] = round(elapsed * 1000)
    return meta


@app.get("/api/gfs/{param}/point")
def gfs_point(
    param: str,
    lat: float = Query(..., ge=40.0, le=65.0, description="Szerokość geograficzna"),
    lon: float = Query(..., ge=5.0,  le=35.0, description="Długość geograficzna"),
    fxx: int   = Query(0, ge=0, le=240, description="Godzina prognozy"),
):
    """
    Wartość parametru GFS dla podanego punktu lat/lon.

    Przykład: /api/gfs/CAPE_SFC/point?lat=52.23&lon=21.01
    """
    param_key = _resolve_gfs_param(param)
    all_keys  = _all_gfs_param_keys()
    if param_key not in all_keys:
        raise HTTPException(404, f"Nieznany parametr: {param}")

    cache = get_gfs_cache()
    entry = cache.get(param_key, fxx=fxx)
    if entry is None:
        raise HTTPException(503, f"Nie udało się pobrać {param_key} fxx={fxx}")

    data = entry.get("data")
    lats = entry.get("lats")
    lons = entry.get("lons")
    if data is None or lats is None or lons is None:
        raise HTTPException(422, "Brak danych georef dla tego parametru")

    # Znajdź najbliższy piksel
    dist = (lats - lat)**2 + (lons - lon)**2
    if dist.ndim == 1:
        # Lats/lons są 1D — GFS regularna siatka
        lat_idx = int(np.argmin(np.abs(lats - lat)))
        lon_idx = int(np.argmin(np.abs(lons - lon)))
        val = float(data[lat_idx, lon_idx])
    else:
        idx = np.unravel_index(np.argmin(dist), dist.shape)
        val = float(data[idx])

    return {
        "param":      param_key,
        "label":      entry.get("label", param_key),
        "units":      entry.get("units", ""),
        "lat":        lat,
        "lon":        lon,
        "value":      None if np.isnan(val) else val,
        "no_data":    np.isnan(val),
        "fxx":        fxx,
        "run_time":   cache.status(fxx).get("run_time"),
        "cache_age_s": cache.status(fxx).get("age_s"),
    }


@app.get("/api/gfs/{param}")
def gfs_image(
    param:  str,
    fxx:    int  = Query(0,   ge=0,   le=240, description="Godzina prognozy"),
    width:  int  = Query(900, ge=64,  le=2048, description="Szerokość PNG [px]"),
    height: int  = Query(900, ge=64,  le=2048, description="Wysokość PNG [px]"),
    interp: str  = Query("scipy", description="Interpolacja: scipy (najlepsza), bilinear, nearest"),
    meta:   bool = Query(False, description="Dołącz metadane w nagłówku X-GFS-Meta"),
):
    """
    Główny endpoint GFS — PNG mapy parametru (RGBA, NaN = przezroczysty).

    Obsługuje zarówno parametry bezpośrednie (CAPE_SFC, SRH_0_3, ...)
    jak i pochodne (SHEAR_0_6_derived, SCP_derived, K_INDEX_derived, ...).

    Nagłówki odpowiedzi:
        X-GFS-Run-Time    — czas runu GFS (np. 2026-03-14 06z UTC)
        X-GFS-Valid-Time  — czas ważności prognozy
        X-GFS-Fxx         — godzina prognozy
        X-GFS-Val-Max     — maksymalna wartość
        X-GFS-Units       — jednostki
        X-GFS-Cache-Age   — wiek danych w sekundach

    Przykład użycia w Leaflet:
        L.imageOverlay('/api/gfs/CAPE_SFC?fxx=0', bounds).addTo(map)
        L.imageOverlay('/api/gfs/SCP_derived?fxx=6', bounds).addTo(map)
    """
    param_key = _resolve_gfs_param(param)
    all_keys  = _all_gfs_param_keys()
    if param_key not in all_keys:
        raise HTTPException(404, f"Nieznany parametr: {param}. "
                                 f"Zobacz /api/gfs po listę.")

    cache   = get_gfs_cache()
    t0      = time.monotonic()
    entry   = cache.get(param_key, fxx=fxx)
    elapsed = time.monotonic() - t0

    if entry is None:
        raise HTTPException(503, f"Nie udało się pobrać {param_key} fxx={fxx}. "
                                 "Sprawdź połączenie lub spróbuj za chwilę.")

    png_bytes = _render_gfs_png(entry, param_key, width=width, height=height, interp=interp)

    s = cache.status(fxx=fxx)
    data = entry.get("data")

    headers = {
        "X-GFS-Run-Time":   s.get("run_time", ""),
        "X-GFS-Fxx":        str(fxx),
        "X-GFS-Val-Max":    str(round(entry["val_max"], 2))
                            if entry.get("val_max") is not None else "nan",
        "X-GFS-Val-Min":    str(round(entry["val_min"], 2))
                            if entry.get("val_min") is not None else "nan",
        "X-GFS-Units":      entry.get("units", ""),
        "X-GFS-Label":      entry.get("label", param_key),
        "X-GFS-Cache-Age":  str(round(s.get("age_s") or 0, 1)),
        "X-Render-Time-Ms": str(round(elapsed * 1000)),
        "Cache-Control":    f"public, max-age=3600",
    }

    if meta:
        import json
        gfs_meta_dict = _gfs_meta(param_key, entry, fxx,
                                   run_time=s.get("run_time"),
                                   age_s=s.get("age_s"))
        headers["X-GFS-Meta"] = json.dumps(gfs_meta_dict)

    return Response(
        content=png_bytes,
        media_type="image/png",
        headers=headers,
    )
