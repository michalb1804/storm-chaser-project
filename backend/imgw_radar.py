"""
IMGW Radar Ingestor
-------------------
Pobiera pliki radarowe z IMGW, parsuje ODIM_H5 i georeferencjonuje dane.

Wymagania:
    pip install h5py numpy requests matplotlib pyproj
"""

import re
import os
import time
import requests
import h5py
import numpy as np
from datetime import datetime, timezone, timedelta

try:
    from pyproj import Proj, Transformer
    HAS_PYPROJ = True
except ImportError:
    HAS_PYPROJ = False
    print("UWAGA: pyproj niedostepny. pip install pyproj")


# ── Produkty ──────────────────────────────────────────────────────────────────
#
# Wzorzec URL:
#   {BASE_URL}/{FOLDER}/{TIMESTAMP}{SUFFIX}
#
# TIMESTAMP: YYYYMMDDHHmmSS00  (ostatnie 00 = centisekundy)
# Skany co ~5 min, ale minuty mogą być nieregularne (np. 43, 48)
# → nie zgadujemy, skanujemy wstecz z weryfikacją sygnatury HDF5

PRODUCTS = {
    # Kompozyty (cała Polska)
    "COMPO_CMAX":  ("HVD_COMPO_CMAX_250.comp.cmax",  "dBZ.cmax.h5"),
    "COMPO_CAPPI": ("HVD_COMPO_CAPPI.comp.cappi",     "dBZ.cappi.h5"),
    "COMPO_SRI":   ("HVD_COMPO_SRI.comp.sri",         "dBR.sri.h5"),
    "COMPO_EHT":   ("HVD_COMPO_EHT.comp.eht",         "Height.eht.h5"),
    "COMPO_DPSRI": ("HVD_COMPO_DPSRI.comp.sri",       "dBR.sri.h5"),
    # Radar Legionowo
    "LEG_PPI":     ("HVD_leg_0_5.ppi",                "dBZ.ppi.h5"),
    "LEG_CAPPI_V": ("HVD_leg_125.cappi",              "V.cappi.h5"),
    "LEG_CAPPI":   ("HVD_leg_compo_pcz.cappi",        "dBZ.cappi.h5"),
    "LEG_DPSRI":   ("HVD_leg_200.dpsri",              "dBR.dpsri.h5"),
    "LEG_EHT":     ("HVD_leg_200_etz.eht",            "Height.eht.h5"),
    "LEG_SRI":     ("HVD_leg_200_leads.sri",          "dBR.sri.h5"),
    "LEG_KDP":     ("HVD_leg_250.max",                "KDP.max.h5"),
    "LEG_RHOHV":   ("HVD_leg_250.max",                "RhoHV.max.h5"),
    "LEG_ZDR":     ("HVD_leg_250.max",                "ZDR.max.h5"),
    # Radar Brzuchania
    "BRZ_PPI":     ("HVD_brz_0_5.ppi",                "dBZ.ppi.h5"),
    "BRZ_CAPPI_V": ("HVD_brz_125.cappi",              "V.cappi.h5"),
    "BRZ_CAPPI":   ("HVD_brz_compo_pcz.cappi",        "dBZ.cappi.h5"),
    "BRZ_DPSRI":   ("HVD_brz_200.dpsri",              "dBR.dpsri.h5"),
    "BRZ_EHT":     ("HVD_brz_200_etz.eht",            "Height.eht.h5"),
    "BRZ_SRI":     ("HVD_brz_200_leads.sri",          "dBR.sri.h5"),
    "BRZ_KDP":     ("HVD_brz_250.max",                "KDP.max.h5"),
    "BRZ_RHOHV":   ("HVD_brz_250.max",                "RhoHV.max.h5"),
    "BRZ_ZDR":     ("HVD_brz_250.max",                "ZDR.max.h5"),
    # Radar Gdynia
    "GDY_PPI":     ("HVD_gdy_0_5.ppi",                "dBZ.ppi.h5"),
    "GDY_CAPPI_V": ("HVD_gdy_125.cappi",              "V.cappi.h5"),
    "GDY_CAPPI":   ("HVD_gdy_compo_pcz.cappi",        "dBZ.cappi.h5"),
    "GDY_DPSRI":   ("HVD_gdy_200.dpsri",              "dBR.dpsri.h5"),
    "GDY_EHT":     ("HVD_gdy_200_etz.eht",            "Height.eht.h5"),
    "GDY_SRI":     ("HVD_gdy_200_leads.sri",          "dBR.sri.h5"),
    "GDY_KDP":     ("HVD_gdy_250.max",                "KDP.max.h5"),
    "GDY_RHOHV":   ("HVD_gdy_250.max",                "RhoHV.max.h5"),
    "GDY_ZDR":     ("HVD_gdy_250.max",                "ZDR.max.h5"),
    # Radar Gora Swietej Anny
    "GSA_PPI":     ("HVD_gsa_0_5.ppi",                "dBZ.ppi.h5"),
    "GSA_CAPPI_V": ("HVD_gsa_125.cappi",              "V.cappi.h5"),
    "GSA_CAPPI":   ("HVD_gsa_compo_pcz.cappi",        "dBZ.cappi.h5"),
    "GSA_DPSRI":   ("HVD_gsa_200.dpsri",              "dBR.dpsri.h5"),
    "GSA_EHT":     ("HVD_gsa_200_etz.eht",            "Height.eht.h5"),
    "GSA_SRI":     ("HVD_gsa_200_leads.sri",          "dBR.sri.h5"),
    "GSA_KDP":     ("HVD_gsa_250.max",                "KDP.max.h5"),
    "GSA_RHOHV":   ("HVD_gsa_250.max",                "RhoHV.max.h5"),
    "GSA_ZDR":     ("HVD_gsa_250.max",                "ZDR.max.h5"),
    # Radar Pastewnik
    "PAS_PPI":     ("HVD_pas_0_5.ppi",                "dBZ.ppi.h5"),
    "PAS_CAPPI_V": ("HVD_pas_125.cappi",              "V.cappi.h5"),
    "PAS_CAPPI":   ("HVD_pas_compo_pcz.cappi",        "dBZ.cappi.h5"),
    "PAS_DPSRI":   ("HVD_pas_200.dpsri",              "dBR.dpsri.h5"),
    "PAS_EHT":     ("HVD_pas_200_etz.eht",            "Height.eht.h5"),
    "PAS_SRI":     ("HVD_pas_200_leads.sri",          "dBR.sri.h5"),
    "PAS_KDP":     ("HVD_pas_250.max",                "KDP.max.h5"),
    "PAS_RHOHV":   ("HVD_pas_250.max",                "RhoHV.max.h5"),
    "PAS_ZDR":     ("HVD_pas_250.max",                "ZDR.max.h5"),
    # Radar Poznan
    "POZ_PPI":     ("HVD_poz_0_5.ppi",                "dBZ.ppi.h5"),
    "POZ_CAPPI_V": ("HVD_poz_125.cappi",              "V.cappi.h5"),
    "POZ_CAPPI":   ("HVD_poz_compo_pcz.cappi",        "dBZ.cappi.h5"),
    "POZ_DPSRI":   ("HVD_poz_200.dpsri",              "dBR.dpsri.h5"),
    "POZ_EHT":     ("HVD_poz_200_etz.eht",            "Height.eht.h5"),
    "POZ_SRI":     ("HVD_poz_200_leads.sri",          "dBR.sri.h5"),
    "POZ_KDP":     ("HVD_poz_250.max",                "KDP.max.h5"),
    "POZ_RHOHV":   ("HVD_poz_250.max",                "RhoHV.max.h5"),
    "POZ_ZDR":     ("HVD_poz_250.max",                "ZDR.max.h5"),
    # Radar Ramza
    "RAM_PPI":     ("HVD_ram_0_5.ppi",                "dBZ.ppi.h5"),
    "RAM_CAPPI_V": ("HVD_ram_125.cappi",              "V.cappi.h5"),
    "RAM_CAPPI":   ("HVD_ram_compo_pcz.cappi",        "dBZ.cappi.h5"),
    "RAM_DPSRI":   ("HVD_ram_200.dpsri",              "dBR.dpsri.h5"),
    "RAM_EHT":     ("HVD_ram_200_etz.eht",            "Height.eht.h5"),
    "RAM_SRI":     ("HVD_ram_200_leads.sri",          "dBR.sri.h5"),
    "RAM_KDP":     ("HVD_ram_250.max",                "KDP.max.h5"),
    "RAM_RHOHV":   ("HVD_ram_250.max",                "RhoHV.max.h5"),
    "RAM_ZDR":     ("HVD_ram_250.max",                "ZDR.max.h5"),
    # Radar Rzeszow
    "RZE_PPI":     ("HVD_rze_0_5.ppi",                "dBZ.ppi.h5"),
    "RZE_CAPPI_V": ("HVD_rze_125.cappi",              "V.cappi.h5"),
    "RZE_CAPPI":   ("HVD_rze_compo_pcz.cappi",        "dBZ.cappi.h5"),
    "RZE_DPSRI":   ("HVD_rze_200.dpsri",              "dBR.dpsri.h5"),
    "RZE_EHT":     ("HVD_rze_200_etz.eht",            "Height.eht.h5"),
    "RZE_SRI":     ("HVD_rze_200_leads.sri",          "dBR.sri.h5"),
    "RZE_KDP":     ("HVD_rze_250.max",                "KDP.max.h5"),
    "RZE_RHOHV":   ("HVD_rze_250.max",                "RhoHV.max.h5"),
    "RZE_ZDR":     ("HVD_rze_250.max",                "ZDR.max.h5"),
    # Radar Swidnik
    "SWI_PPI":     ("HVD_swi_0_5.ppi",                "dBZ.ppi.h5"),
    "SWI_CAPPI_V": ("HVD_swi_125.cappi",              "V.cappi.h5"),
    "SWI_CAPPI":   ("HVD_swi_compo_pcz.cappi",        "dBZ.cappi.h5"),
    "SWI_DPSRI":   ("HVD_swi_200.dpsri",              "dBR.dpsri.h5"),
    "SWI_EHT":     ("HVD_swi_200_etz.eht",            "Height.eht.h5"),
    "SWI_SRI":     ("HVD_swi_200_leads.sri",          "dBR.sri.h5"),
    "SWI_KDP":     ("HVD_swi_250.max",                "KDP.max.h5"),
    "SWI_RHOHV":   ("HVD_swi_250.max",                "RhoHV.max.h5"),
    "SWI_ZDR":     ("HVD_swi_250.max",                "ZDR.max.h5"),
    # Radar Uzranki
    "UZR_PPI":     ("HVD_uzr_0_5.ppi",                "dBZ.ppi.h5"),
    "UZR_CAPPI_V": ("HVD_uzr_125.cappi",              "V.cappi.h5"),
    "UZR_CAPPI":   ("HVD_uzr_compo_pcz.cappi",        "dBZ.cappi.h5"),
    "UZR_DPSRI":   ("HVD_uzr_200.dpsri",              "dBR.dpsri.h5"),
    "UZR_EHT":     ("HVD_uzr_200_etz.eht",            "Height.eht.h5"),
    "UZR_SRI":     ("HVD_uzr_200_leads.sri",          "dBR.sri.h5"),
    "UZR_KDP":     ("HVD_uzr_250.max",                "KDP.max.h5"),
    "UZR_RHOHV":   ("HVD_uzr_250.max",                "RhoHV.max.h5"),
    "UZR_ZDR":     ("HVD_uzr_250.max",                "ZDR.max.h5"),
}

BASE_URL         = "https://danepubliczne.imgw.pl/pl/datastore/getfiledown/Oper/Polrad/Produkty/HVD"
DATA_DIR         = "radar_data"
HEADERS          = {"User-Agent": "Mozilla/5.0 (radar-ingestor/1.0; private use)"}
MAX_LOOKBACK_MIN = 30
HDF5_MAGIC       = b"\x89HDF\r\n\x1a\n"


# ── Timestamp ─────────────────────────────────────────────────────────────────

# Kompozyty (COMPO_*) mają sekundy zawsze 00: 2026031408150000
# Radary indywidualne (LEG_* itp.) mają rzeczywisty czas skanu: 2026031308250800
# Dlatego rozróżniamy dwa typy produktów.

COMPO_PRODUCTS = {k for k in PRODUCTS if k.startswith("COMPO_")}

# Wszystkie możliwe sekundy skanów radarów indywidualnych (na podstawie obserwacji IMGW)
# Centisekundy są zawsze 00 (sporadycznie 01, ale 00 wystarcza do identyfikacji)
INDIVIDUAL_SECONDS = [3, 4, 6, 7, 8, 50, 52, 53]


def make_timestamp(dt: datetime) -> str:
    """datetime → YYYYMMDDHHmmSS00 (sekundy z datetime, centisekundy zawsze 00)"""
    return dt.strftime("%Y%m%d%H%M%S") + "00"


def make_url(product_key: str, dt: datetime) -> str:
    folder, suffix = PRODUCTS[product_key]
    return f"{BASE_URL}/{folder}/{make_timestamp(dt)}{suffix}"


# ── Znajdowanie najnowszego pliku ─────────────────────────────────────────────

def is_valid_hdf5(url: str) -> bool:
    """Pobiera pierwsze 16 bajtów i sprawdza sygnaturę HDF5."""
    try:
        r = requests.get(url, headers={**HEADERS, "Range": "bytes=0-15"}, timeout=8)
        return r.status_code in (200, 206) and r.content[:8] == HDF5_MAGIC
    except Exception:
        return False


def find_latest(product_key: str) -> datetime | None:
    """
    Szuka najnowszego dostępnego pliku dla produktu.
    - Kompozyty: skanuje wstecz po 1 min, sekundy zawsze 00
    - Radary indywidualne: dla każdej minuty próbuje kilku wariantów sekund
    """
    if product_key in COMPO_PRODUCTS:
        return _find_latest_compo(product_key)
    else:
        return _find_latest_individual(product_key)


def _find_latest_compo(product_key: str) -> datetime | None:
    """Kompozyty — sekundy zawsze 00, skanuj minuty wstecz."""
    start = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=2)
    print(f"  [{product_key}] Skanuję wstecz od {start.strftime('%H:%M UTC')} ...")
    for i in range(MAX_LOOKBACK_MIN):
        dt = start - timedelta(minutes=i)
        if is_valid_hdf5(make_url(product_key, dt)):
            print(f"  [{product_key}] ✓ HDF5 @ {dt.strftime('%H:%M:%S UTC')}  ({i} min temu)")
            return dt
        print(f"  [{product_key}] · {dt.strftime('%H:%M')} — brak")
    print(f"  [{product_key}] ✗ Brak pliku w ostatnich {MAX_LOOKBACK_MIN} min")
    return None


def _find_latest_individual(product_key: str) -> datetime | None:
    """
    Radary indywidualne — sekundy nieznane z góry.
    Dla każdej minuty wstecz próbuje typowych wartości sekund.
    Przy pierwszym trafieniu zapamiętuję sekundy w _individual_seconds_cache
    żeby kolejne wywołania były szybsze.
    """
    start = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=2)
    print(f"  [{product_key}] Skanuję wstecz od {start.strftime('%H:%M UTC')} ...")

    # Sprawdź czy mamy zapamiętane sekundy dla tego produktu
    cached_sec = _individual_seconds_cache.get(product_key)
    seconds_to_try = ([cached_sec] + INDIVIDUAL_SECONDS) if cached_sec is not None                      else INDIVIDUAL_SECONDS

    for i in range(MAX_LOOKBACK_MIN):
        base = start - timedelta(minutes=i)
        for sec in seconds_to_try:
            dt = base.replace(second=sec)
            if is_valid_hdf5(make_url(product_key, dt)):
                print(f"  [{product_key}] ✓ HDF5 @ {dt.strftime('%H:%M:%S UTC')}  "
                      f"({i} min temu, sec={sec})")
                _individual_seconds_cache[product_key] = sec   # zapamiętaj
                return dt
        print(f"  [{product_key}] · {base.strftime('%H:%M')} — brak")

    print(f"  [{product_key}] ✗ Brak pliku w ostatnich {MAX_LOOKBACK_MIN} min")
    return None


# Cache zapamiętujący sekundy skanów dla radarów indywidualnych
# {product_key: seconds_int}  — wypełniany przy pierwszym trafieniu
_individual_seconds_cache: dict[str, int] = {}


# ── Pobieranie ────────────────────────────────────────────────────────────────

def download_file(url: str, local_path: str) -> bool:
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    if os.path.exists(local_path):
        print(f"  → Plik juz lokalnie: {os.path.basename(local_path)}")
        return True
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        with open(local_path, "wb") as f:
            f.write(r.content)
        print(f"  ✓ Pobrano: {os.path.basename(local_path)}  ({len(r.content)/1024:.0f} KB)")
        return True
    except requests.HTTPError as e:
        print(f"  ✗ HTTP {e.response.status_code}")
        return False
    except Exception as e:
        print(f"  ✗ {e}")
        return False


# ── Parsowanie ODIM_H5 ────────────────────────────────────────────────────────

def parse_hdf5(filepath: str) -> dict | None:
    """
    Parser zgodny z ODIM_H5/V2_x.
    Struktura:
      /dataset1/data1/data  — surowe uint8
      /dataset1/what/       — gain, offset, nodata, undetect, quantity
      /where/               — projekcja, narożniki, rozdzielczość
      /what/                — czas, produkt
    """
    try:
        with h5py.File(filepath, "r") as f:

            # ── Znajdź dataset1 z produktem MAX (mapa pozioma) ──────────────
            # Pliki radarów indywidualnych zawierają kilka datasetów:
            #   dataset1: MAX  (500×500) — mapa pozioma, to nas interesuje
            #   dataset2: VSP  (przekrój pionowy)
            #   dataset3: HSP  (przekrój poziomy)
            # Kompozyty mają tylko dataset1.
            # Szukamy datasetu z product=MAX lub pierwszego z kwadratową siatką.

            data_path  = None
            what_path  = None
            where_path = None

            def find_best_dataset(name, obj):
                nonlocal data_path, what_path, where_path
                if not (isinstance(obj, h5py.Dataset) and name.endswith("/data")):
                    return
                parts    = name.split("/")          # ['dataset1','data1','data']
                ds_root  = parts[0]                 # 'dataset1'
                w_path   = f"{ds_root}/what"
                wr_path  = f"{ds_root}/where"

                # Preferuj dataset z product=MAX lub jedyny dataset
                if w_path in f:
                    product_tag = f[w_path].attrs.get("product", b"")
                    if isinstance(product_tag, (bytes, np.bytes_)):
                        product_tag = product_tag.decode()
                    is_max = product_tag in ("MAX", "CAPPI", "CMAX", "SRI",
                                             "EHT", "DPSRI", "PPI")
                else:
                    is_max = False

                # Bierz pierwszy pasujący albo pierwszy w ogóle
                if data_path is None or is_max:
                    data_path  = name
                    what_path  = "/".join(parts[:-2]) + "/what"
                    # where: najpierw dataset-level, fallback root
                    where_path = wr_path if wr_path in f else "where"

            f.visititems(find_best_dataset)

            if data_path is None:
                print(f"  ! Brak datasetu w {filepath}")
                return None

            raw = f[data_path][()].astype(float)

            # ── gain/offset/nodata z dataset what/ ──────────────────────────
            what_attrs = dict(f[what_path].attrs) if what_path in f else {}
            gain     = float(what_attrs.get("gain",     1.0))
            offset   = float(what_attrs.get("offset",   0.0))
            nodata   = float(what_attrs.get("nodata",   255.0))
            undetect = float(what_attrs.get("undetect", 0.0))
            quantity = what_attrs.get("quantity", b"UNKNOWN")
            if isinstance(quantity, (bytes, np.bytes_)):
                quantity = quantity.decode()

            # ── Skalowanie: nodata i undetect → NaN ─────────────────────────
            data = raw * gain + offset
            data[raw == nodata]   = np.nan
            data[raw == undetect] = np.nan

            # ── where: dataset-level (radary ind.) lub root (kompozyty) ─────
            where_attrs = dict(f[where_path].attrs) if where_path in f else {}
            root_what   = dict(f["what"].attrs)     if "what"  in f else {}

            return {
                "data":     data,
                "shape":    data.shape,
                "quantity": quantity,
                "gain":     gain,
                "offset":   offset,
                "nodata":   nodata,
                "undetect": undetect,
                "where":    where_attrs,
                "what":     {**root_what, **what_attrs},
                "filepath": filepath,
            }

    except Exception as e:
        print(f"  ! Błąd parsowania {filepath}: {e}")
        import traceback; traceback.print_exc()
        return None


# ── Georeferencja ─────────────────────────────────────────────────────────────

def build_georef(where: dict) -> dict | None:
    """
    Buduje siatkę współrzędnych lat/lon dla całej siatki radarowej.

    ODIM COMPO używa projekcji aeqd (azimuthal equidistant):
      +proj=aeqd +lon_0=19.0926 +lat_0=52.3469 +ellps=sphere

    Lewy górny narożnik (UL) → piksel (row=0, col=0).
    xscale/yscale to rozmiar piksela w metrach (~1 km).

    Zwraca:
      proj        — obiekt Proj (aeqd)
      transformer — Transformer aeqd → WGS84
      ul_x, ul_y  — UL w metrach układu proj
      xscale, yscale, xsize, ysize
      lat_grid    — (rows, cols) szerokości geogr.
      lon_grid    — (rows, cols) długości geogr.
    """
    if not HAS_PYPROJ:
        return None

    # Sprawdź czy where/ zawiera dane siatki (kompozyty i radary dataset-level)
    # Root where/ radarów indywidualnych ma tylko lat/lon/height stacji — bez siatki
    if "xsize" not in where or "UL_lon" not in where:
        return None

    projdef = where.get("projdef", b"")
    if isinstance(projdef, (bytes, np.bytes_)):
        projdef = projdef.decode()

    xsize  = int(where["xsize"])
    ysize  = int(where["ysize"])
    xscale = float(where["xscale"])
    yscale = float(where["yscale"])
    ul_lon = float(where["UL_lon"])
    ul_lat = float(where["UL_lat"])

    proj        = Proj(projdef)
    ul_x, ul_y  = proj(ul_lon, ul_lat)

    # Siatka środków pikseli w metrach
    xs = ul_x + (np.arange(xsize) + 0.5) * xscale
    ys = ul_y - (np.arange(ysize) + 0.5) * yscale   # Y maleje w dół

    xx, yy = np.meshgrid(xs, ys)

    transformer         = Transformer.from_proj(proj, "epsg:4326", always_xy=True)
    lon_grid, lat_grid  = transformer.transform(xx, yy)

    return {
        "proj":        proj,
        "transformer": transformer,
        "ul_x":        ul_x,
        "ul_y":        ul_y,
        "xscale":      xscale,
        "yscale":      yscale,
        "xsize":       xsize,
        "ysize":       ysize,
        "lat_grid":    lat_grid,
        "lon_grid":    lon_grid,
    }


def latlon_to_pixel(georef: dict, lat: float, lon: float) -> tuple[int, int] | None:
    """Przelicza (lat, lon) → (row, col). None jeśli poza siatką."""
    if georef is None:
        return None
    x, y = georef["proj"](lon, lat)
    col  = int((x - georef["ul_x"]) / georef["xscale"])
    row  = int((georef["ul_y"] - y) / georef["yscale"])
    if 0 <= row < georef["ysize"] and 0 <= col < georef["xsize"]:
        return row, col
    return None


def pixel_to_latlon(georef: dict, row: int, col: int) -> tuple[float, float]:
    """Przelicza (row, col) → (lat, lon) — środek piksela."""
    return float(georef["lat_grid"][row, col]), float(georef["lon_grid"][row, col])


def query_point(parsed: dict, georef: dict, lat: float, lon: float) -> float | None:
    """Wartość radarowa (np. dBZ) dla lokalizacji lat/lon. None = NaN lub poza siatką."""
    rc = latlon_to_pixel(georef, lat, lon)
    if rc is None:
        return None
    val = parsed["data"][rc[0], rc[1]]
    return None if np.isnan(val) else float(val)


def demo_locations() -> dict:
    return {
        #"Warszawa":   (52.2297, 21.0122),
        #"Krakow":     (50.0647, 19.9450),
        #"Gdansk":     (54.3520, 18.6466),
        #"Wroclaw":    (51.1079, 17.0385),
        #"Poznan":     (52.4064, 16.9252),
        #"Legionowo":  (52.4050, 20.9611),
    }


# ── Eksploracja struktury (diagnostyka) ──────────────────────────────────────

def explore_hdf5(filepath: str):
    print(f"\n{'='*60}")
    print(f"  STRUKTURA HDF5: {os.path.basename(filepath)}")
    print(f"{'='*60}")
    with h5py.File(filepath, "r") as f:
        print("\n  [Root attrs]")
        for k, v in f.attrs.items():
            print(f"    {k} = {v!r}")
        print()
        def show(name, obj):
            pad = "  " * (name.count("/") + 1)
            if isinstance(obj, h5py.Group):
                print(f"{pad}📁 {name}/")
                for k, v in obj.attrs.items():
                    print(f"{pad}   {k} = {v!r}")
            elif isinstance(obj, h5py.Dataset):
                print(f"{pad}📊 {name}  shape={obj.shape}  dtype={obj.dtype}")
                for k, v in obj.attrs.items():
                    print(f"{pad}   {k} = {v!r}")
        f.visititems(show)


# ── Wizualizacja ──────────────────────────────────────────────────────────────

def nws_dbz_cmap():
    import matplotlib.colors as mcolors
    stops = [
        (-10, "#000000"), (0,  "#04e9e7"), (5,  "#019ff4"), (10, "#0300f4"),
        (15,  "#02fd02"), (20, "#01c501"), (25, "#008e00"), (30, "#fdf802"),
        (35,  "#e5bc00"), (40, "#fd9500"), (45, "#fd0000"), (50, "#d40000"),
        (55,  "#bc0000"), (60, "#f800fd"), (65, "#9854c6"), (70, "#ffffff"),
    ]
    vmin, vmax = -10, 70
    cmap = mcolors.LinearSegmentedColormap.from_list(
        "nws_dbz", [((v - vmin) / (vmax - vmin), c) for v, c in stops])
    cmap.set_bad(color=(0, 0, 0, 0))
    return cmap, vmin, vmax


def _time_label(parsed):
    ts = parsed.get("what", {}).get("time", b"")
    ds = parsed.get("what", {}).get("date", b"")
    if isinstance(ts, (bytes, __import__("numpy").bytes_)): ts = ts.decode()
    if isinstance(ds, (bytes, __import__("numpy").bytes_)): ds = ds.decode()
    if ds:
        return f"{ds[:4]}-{ds[4:6]}-{ds[6:]} {ts[:2]}:{ts[2:4]} UTC"
    return __import__("os").path.basename(parsed["filepath"])


def plot_radar(parsed: dict, georef: dict = None, save_path: str = None):
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("  (brak matplotlib)")
        return

    data = parsed["data"]
    q    = parsed.get("quantity", "?")
    title_time = _time_label(parsed)

    if "DBZ" in q.upper():
        cmap, vmin, vmax = nws_dbz_cmap()
        cbar_label = "dBZ"
    else:
        cmap = plt.cm.turbo
        cmap.set_bad(color=(0, 0, 0, 0))
        vmin = float(np.nanmin(data))
        vmax = float(np.nanmax(data))
        cbar_label = q

    try:
        import cartopy.crs as ccrs
        import cartopy.feature as cfeature
        _plot_cartopy(data, georef, parsed, cmap, vmin, vmax,
                      cbar_label, q, title_time, save_path)
        return
    except ImportError:
        pass

    # Fallback bez cartopy
    fig, ax = plt.subplots(figsize=(11, 10))
    fig.patch.set_facecolor("#0d1117")
    ax.set_facecolor("#0d1117")
    if georef is not None:
        im = ax.pcolormesh(georef["lon_grid"], georef["lat_grid"], data,
                           cmap=cmap, vmin=vmin, vmax=vmax,
                           shading="auto", rasterized=True)
        ax.set_xlabel("Dlugosc [E]", fontsize=9, color="#9ca3af")
        ax.set_ylabel("Szerokosc [N]", fontsize=9, color="#9ca3af")
        ax.set_aspect("equal")
        ax.tick_params(colors="#9ca3af")
        for nazwa, (lat, lon) in demo_locations().items():
            ax.plot(lon, lat, "o", ms=4, color="white")
            ax.text(lon + 0.12, lat + 0.05, nazwa, color="white", fontsize=7,
                    bbox=dict(boxstyle="round,pad=0.15", fc="#0d1117", alpha=0.6, lw=0))
    else:
        im = ax.imshow(data, cmap=cmap, vmin=vmin, vmax=vmax,
                       origin="upper", aspect="equal")
        ax.axis("off")
    cbar = plt.colorbar(im, ax=ax, fraction=0.03, pad=0.02)
    cbar.set_label(cbar_label, fontsize=9, color="#d1d5db")
    plt.setp(cbar.ax.yaxis.get_ticklabels(), color="#9ca3af")
    ax.set_title(f"IMGW Radar — {q}\n{title_time}", fontsize=10, color="#f3f4f6", pad=8)
    fig.text(0.01, 0.005,
        "Zrodlem danych jest Instytut Meteorologii i Gospodarki Wodnej - PIB",
        fontsize=6, color="#6b7280")
    plt.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
        print(f"  Zapisano PNG: {save_path}")
    plt.show()


def _plot_cartopy(data, georef, parsed, cmap, vmin, vmax,
                  cbar_label, q, title_time, save_path):
    import matplotlib.pyplot as plt
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature

    if georef is not None:
        lon_min = georef["lon_grid"].min() - 0.3
        lon_max = georef["lon_grid"].max() + 0.3
        lat_min = georef["lat_grid"].min() - 0.3
        lat_max = georef["lat_grid"].max() + 0.3
    else:
        lon_min, lon_max, lat_min, lat_max = 13.5, 26.5, 48.5, 56.5

    proj = ccrs.PlateCarree()
    fig  = plt.figure(figsize=(13, 10))
    ax   = fig.add_subplot(1, 1, 1, projection=proj)
    ax.set_extent([lon_min, lon_max, lat_min, lat_max], crs=proj)
    ax.set_facecolor("#0d1117")
    fig.patch.set_facecolor("#0d1117")

    # Dane radarowe
    if georef is not None:
        im = ax.pcolormesh(
            georef["lon_grid"], georef["lat_grid"], data,
            cmap=cmap, vmin=vmin, vmax=vmax,
            shading="auto", rasterized=True, transform=proj, zorder=2)
    else:
        im = None

    # Warstwy geograficzne
    ax.add_feature(cfeature.OCEAN.with_scale("50m"),  facecolor="#1a2744", zorder=1)
    ax.add_feature(cfeature.LAND.with_scale("50m"),   facecolor="#1c2333", zorder=1)
    ax.add_feature(cfeature.BORDERS.with_scale("50m"),
                   edgecolor="#6b7280", linewidth=0.8, zorder=3)
    ax.add_feature(cfeature.COASTLINE.with_scale("50m"),
                   edgecolor="#6b7280", linewidth=0.6, zorder=3)
    ax.add_feature(cfeature.RIVERS.with_scale("50m"),
                   edgecolor="#1e4d7b", linewidth=0.4, alpha=0.7, zorder=3)
    ax.add_feature(cfeature.LAKES.with_scale("50m"),
                   facecolor="#1a2744", edgecolor="#1e4d7b", linewidth=0.3, zorder=3)

    # Siatka
    gl = ax.gridlines(draw_labels=True, linewidth=0.3,
                      color="#374151", alpha=0.8, linestyle="--", zorder=4)
    gl.top_labels   = False
    gl.right_labels = False
    gl.xlabel_style = {"size": 8, "color": "#9ca3af"}
    gl.ylabel_style = {"size": 8, "color": "#9ca3af"}

    # Miasta
    for nazwa, (lat, lon) in demo_locations().items():
        ax.plot(lon, lat, "o", ms=4, color="white",
                markeredgecolor="#374151", markeredgewidth=0.5,
                transform=proj, zorder=6)
        ax.text(lon + 0.15, lat + 0.05, nazwa,
                color="white", fontsize=7.5, fontweight="bold",
                transform=proj, zorder=6,
                bbox=dict(boxstyle="round,pad=0.2", fc="#0d1117", alpha=0.6, lw=0))

    if im is not None:
        cbar = plt.colorbar(im, ax=ax, fraction=0.025, pad=0.02)
        cbar.set_label(cbar_label, fontsize=9, color="#d1d5db")
        cbar.ax.yaxis.set_tick_params(color="#9ca3af", labelsize=8)
        plt.setp(cbar.ax.yaxis.get_ticklabels(), color="#9ca3af")

    ax.set_title(f"IMGW Radar COMPO — {q}\n{title_time}",
                 fontsize=11, color="#f3f4f6", pad=10)
    fig.text(0.01, 0.005,
        "Zrodlem danych jest Instytut Meteorologii i Gospodarki Wodnej - PIB",
        fontsize=6, color="#6b7280")
    plt.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
        print(f"  Zapisano PNG: {save_path}")
    plt.show()


# ── Główna funkcja ────────────────────────────────────────────────────────────

def run_once(product_key: str = "COMPO_CMAX") -> dict | None:
    print(f"\n── {product_key} {'─'*(40-len(product_key))}")
    dt = find_latest(product_key)
    if dt is None:
        return None
    url        = make_url(product_key, dt)
    local_path = os.path.join(DATA_DIR, product_key, f"{make_timestamp(dt)}.h5")
    print(f"  URL: {url}")
    if not download_file(url, local_path):
        return None
    return parse_hdf5(local_path)


def run_continuous(product_key: str = "COMPO_CMAX", interval_min: int = 5):
    print(f"\nIngestor ciagy — {product_key}, co {interval_min} min. Ctrl+C zatrzymuje.\n")
    while True:
        try:
            parsed = run_once(product_key)
            if parsed:
                d = parsed["data"]
                print(f"  shape={parsed['shape']}  "
                      f"min={np.nanmin(d):.1f}  max={np.nanmax(d):.1f}  "
                      f"nan%={100*np.isnan(d).mean():.1f}%  qty={parsed['quantity']}")
            nxt = datetime.now(timezone.utc) + timedelta(minutes=interval_min)
            print(f"  Nastepny run: {nxt.strftime('%H:%M UTC')}\n")
            time.sleep(interval_min * 60)
        except KeyboardInterrupt:
            print("\nZatrzymano.")
            break


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("IMGW Radar Ingestor")
    print(f"UTC: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')}")

    parsed = run_once("COMPO_CMAX")

    if parsed:
        d = parsed["data"]
        q = parsed["quantity"]
        print(f"\n  Dane (ODIM):")
        print(f"    quantity = {q}")
        print(f"    shape    = {parsed['shape']}")
        print(f"    min      = {np.nanmin(d):.2f}")
        print(f"    max      = {np.nanmax(d):.2f}")
        print(f"    mean     = {np.nanmean(d):.2f}")
        print(f"    nan%     = {100*np.isnan(d).mean():.1f}%")
        print(f"    gain     = {parsed['gain']}")
        print(f"    offset   = {parsed['offset']}")

        # Georeferencja
        georef = build_georef(parsed["where"])

        if georef is not None:
            print("\n  Georeferencja:")
            print(f"    siatka:    {georef['lat_grid'].shape}")
            print(f"    lat zakres: {georef['lat_grid'].min():.3f} .. {georef['lat_grid'].max():.3f}")
            print(f"    lon zakres: {georef['lon_grid'].min():.3f} .. {georef['lon_grid'].max():.3f}")
            print(f"    xscale:    {georef['xscale']:.1f} m/piksel")
            print(f"    yscale:    {georef['yscale']:.1f} m/piksel")

            print("\n  Weryfikacja naroznikow (piksel → lat/lon):")
            corners = [
                (0,                    0,                    "UL"),
                (0,                    georef["xsize"] - 1,  "UR"),
                (georef["ysize"] - 1,  0,                    "LL"),
                (georef["ysize"] - 1,  georef["xsize"] - 1,  "LR"),
            ]
            for row, col, name in corners:
                lat_c, lon_c = pixel_to_latlon(georef, row, col)
                print(f"    {name}: ({lat_c:.4f}, {lon_c:.4f})")

            print("\n  Zapytania punktowe:")
            for nazwa, (lat, lon) in demo_locations().items():
                rc  = latlon_to_pixel(georef, lat, lon)
                val = query_point(parsed, georef, lat, lon)
                dbz = f"{val:.1f} dBZ" if val is not None else "brak sygnalu (NaN)"
                print(f"    {nazwa:<12} ({lat:.4f}, {lon:.4f})  piksel={rc}  {dbz}")
        else:
            print("\n  (pyproj niedostepny — pip install pyproj)")
            georef = None

        plot_radar(parsed, georef=georef,
                   save_path=parsed["filepath"].replace(".h5", "_geo.png"))

    # Tryb ciagly — odkomentuj:
    # run_continuous("COMPO_CMAX", interval_min=5)
