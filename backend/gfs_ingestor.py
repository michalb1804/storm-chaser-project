"""
GFS Convective Parameters Ingestor
------------------------------------
Pobiera parametry konwekcyjne z modelu GFS przez Herbie.
Dane dla obszaru Polski (bbox 48-56N, 14-25E), rozdzielczość 0.25°.

Instalacja:
    pip install herbie-data cfgrib xarray numpy

Uruchom:
    python gfs_ingestor.py                     # najnowszy run, f00-f48
    python gfs_ingestor.py --fxx 0             # tylko analiza t+0
    python gfs_ingestor.py --fxx 0 6 12 24 48  # wybrane godziny
    python gfs_ingestor.py --explore           # pokaż dostępne zmienne w GFS

Parametry pobierane:
    pgrb2.0p25   — standardowe (CAPE, CIN, PWAT, T2m, Td2m, MSLP, wiatr, Z500)
    pgrb2b.0p25  — dodatkowe (SRH, shear, lapse rate, LCL, LFC, DCAPE, ...)
"""

import os
import argparse
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

import numpy as np

log = logging.getLogger("gfs")

# ── Katalog danych ────────────────────────────────────────────────────────────

GFS_DIR  = "gfs_data"
BBOX     = dict(
    south=48.0, north=56.0,
    west=14.0,  east=25.0,
)

# ── Definicje parametrów ──────────────────────────────────────────────────────
#
# Każdy parametr to:
#   search   — wzorzec regex dla Herbie (pasuje do inventory GRIB2)
#   product  — "pgrb2.0p25" (standardowe) lub "pgrb2b.0p25" (dodatkowe)
#   label    — czytelna nazwa
#   units    — jednostki
#
# Wzorce search: ":ZMIENNA:POZIOM:" — kolumny z inventory GRIB2
# Inventory: variable:level:forecast_type
#
# UWAGA: Wiele parametrów konwekcyjnych (SRH, shear, DCAPE, LCL, LFC,
# lapse rate itd.) jest w pliku pgrb2b.0p25, nie w standardowym pgrb2.0p25

PARAMS = {
    # ── CAPE / CIN ───────────────────────────────────────────────────────────
    # Źródło: pgrb2.0p25, potwierdzone w inventory
    "CAPE_SFC": {
        "search":  ":CAPE:surface:",
        "product": "pgrb2.0p25",
        "label":   "SBCAPE",
        "units":   "J/kg",
        "desc":    "Surface-based CAPE",
    },
    "CIN_SFC": {
        "search":  ":CIN:surface:",
        "product": "pgrb2.0p25",
        "label":   "SBCIN",
        "units":   "J/kg",
        "desc":    "Surface-based CIN",
    },
    "CAPE_255_0": {
        "search":  ":CAPE:255-0 mb above ground:",
        "product": "pgrb2.0p25",
        "label":   "MLCAPE",
        "units":   "J/kg",
        "desc":    "Mixed-layer CAPE (255-0 mb AGL)",
    },
    "CIN_255_0": {
        "search":  ":CIN:255-0 mb above ground:",
        "product": "pgrb2.0p25",
        "label":   "MLCIN",
        "units":   "J/kg",
        "desc":    "Mixed-layer CIN (255-0 mb AGL)",
    },
    "CAPE_90_0": {
        "search":  ":CAPE:90-0 mb above ground:",
        "product": "pgrb2.0p25",
        "label":   "MUCAPE",
        "units":   "J/kg",
        "desc":    "Most-unstable CAPE (90-0 mb AGL)",
    },
    "CIN_90_0": {
        "search":  ":CIN:90-0 mb above ground:",
        "product": "pgrb2.0p25",
        "label":   "MUCIN",
        "units":   "J/kg",
        "desc":    "Most-unstable CIN (90-0 mb AGL)",
    },
    "CAPE_180_0": {
        "search":  ":CAPE:180-0 mb above ground:",
        "product": "pgrb2.0p25",
        "label":   "CAPE180",
        "units":   "J/kg",
        "desc":    "CAPE 180-0 mb AGL",
    },
    "CIN_180_0": {
        "search":  ":CIN:180-0 mb above ground:",
        "product": "pgrb2.0p25",
        "label":   "CIN180",
        "units":   "J/kg",
        "desc":    "CIN 180-0 mb AGL",
    },
    "PLPL": {
        "search":  ":PLPL:255-0 mb above ground:",
        "product": "pgrb2.0p25",
        "label":   "PLPL",
        "units":   "Pa",
        "desc":    "Pressure of lifted parcel (255-0 mb) — do szacowania LCL/LFC",
    },

    # ── Powierzchnia ─────────────────────────────────────────────────────────
    "T2M": {
        "search":  ":TMP:2 m above ground:",
        "product": "pgrb2.0p25",
        "label":   "T2m",
        "units":   "K",
        "desc":    "2m temperature",
    },
    "D2M": {
        "search":  ":DPT:2 m above ground:",
        "product": "pgrb2.0p25",
        "label":   "Td2m",
        "units":   "K",
        "desc":    "2m dew point",
    },
    "RH2M": {
        "search":  ":RH:2 m above ground:",
        "product": "pgrb2.0p25",
        "label":   "RH2m",
        "units":   "%",
        "desc":    "2m relative humidity",
    },
    "PWAT": {
        "search":  ":PWAT:entire atmosphere",
        "product": "pgrb2.0p25",
        "label":   "PWAT",
        "units":   "kg/m2",
        "desc":    "Precipitable water",
    },
    "MSLP": {
        "search":  ":PRMSL:mean sea level:",
        "product": "pgrb2.0p25",
        "label":   "MSLP",
        "units":   "Pa",
        "desc":    "Mean sea level pressure",
    },
    "APCP": {
        "search":  ":APCP:surface:",
        "product": "pgrb2.0p25",
        "label":   "APCP",
        "units":   "kg/m2",
        "desc":    "Total precipitation (niedostępne przy fxx=0)",
        "skip_fxx": [0],   # GFS nie generuje APCP dla analizy t+0
    },
    "HPBL": {
        "search":  ":HPBL:surface:",
        "product": "pgrb2.0p25",
        "label":   "HPBL",
        "units":   "m",
        "desc":    "Planetary boundary layer height",
    },
    "GUST": {
        "search":  ":GUST:surface:",
        "product": "pgrb2.0p25",
        "label":   "GUST",
        "units":   "m/s",
        "desc":    "Surface wind gust",
    },

    # ── Wiatr (do obliczania shear) ───────────────────────────────────────────
    # GFS nie ma prekalkulowanego shear AGL — obliczamy z profilu wiatru
    # Dostępne poziomy AGL: 10m, 20m, 30m, 40m, 50m, 80m, 100m + warstwy mb
    "U10": {
        "search":  ":UGRD:10 m above ground:",
        "product": "pgrb2.0p25",
        "label":   "U10",
        "units":   "m/s",
        "desc":    "10m U-wind",
    },
    "V10": {
        "search":  ":VGRD:10 m above ground:",
        "product": "pgrb2.0p25",
        "label":   "V10",
        "units":   "m/s",
        "desc":    "10m V-wind",
    },
    "U50": {
        "search":  ":UGRD:50 m above ground:",
        "product": "pgrb2.0p25",
        "label":   "U50",
        "units":   "m/s",
        "desc":    "50m U-wind (do shear nizin)",
    },
    "V50": {
        "search":  ":VGRD:50 m above ground:",
        "product": "pgrb2.0p25",
        "label":   "V50",
        "units":   "m/s",
        "desc":    "50m V-wind",
    },
    "U_30MB": {
        "search":  ":UGRD:30-0 mb above ground:",
        "product": "pgrb2.0p25",
        "label":   "U_30mb",
        "units":   "m/s",
        "desc":    "U-wind 30-0 mb AGL (~1km) — do shear 0-1km",
    },
    "V_30MB": {
        "search":  ":VGRD:30-0 mb above ground:",
        "product": "pgrb2.0p25",
        "label":   "V_30mb",
        "units":   "m/s",
        "desc":    "V-wind 30-0 mb AGL (~1km) — do shear 0-1km",
    },
    "U_PBL": {
        "search":  ":UGRD:planetary boundary layer:",
        "product": "pgrb2.0p25",
        "label":   "U_PBL",
        "units":   "m/s",
        "desc":    "U-wind PBL mean",
    },
    "V_PBL": {
        "search":  ":VGRD:planetary boundary layer:",
        "product": "pgrb2.0p25",
        "label":   "V_PBL",
        "units":   "m/s",
        "desc":    "V-wind PBL mean",
    },
    # Storm motion (USTM/VSTM) — do obliczania SRH 0-1km i Supercell Composite
    "USTM": {
        "search":  ":USTM:6000-0 m above ground:",
        "product": "pgrb2.0p25",
        "label":   "USTM",
        "units":   "m/s",
        "desc":    "U-component storm motion (6km layer)",
    },
    "VSTM": {
        "search":  ":VSTM:6000-0 m above ground:",
        "product": "pgrb2.0p25",
        "label":   "VSTM",
        "units":   "m/s",
        "desc":    "V-component storm motion (6km layer)",
    },

    # ── Helicity (SRH) ────────────────────────────────────────────────────────
    # GFS ma tylko SRH 0-3km bezpośrednio; 0-1km obliczamy z profilu wiatru
    "SRH_0_3": {
        "search":  ":HLCY:3000-0 m above ground:",
        "product": "pgrb2.0p25",
        "label":   "SRH0-3",
        "units":   "m2/s2",
        "desc":    "Storm-relative helicity 0-3 km (bezpośrednio z GFS)",
    },

    # ── Poziomy izobary ───────────────────────────────────────────────────────
    "Z500": {
        "search":  ":HGT:500 mb:",
        "product": "pgrb2.0p25",
        "label":   "Z500",
        "units":   "gpm",
        "desc":    "500 hPa geopotential height",
    },
    "T500": {
        "search":  ":TMP:500 mb:",
        "product": "pgrb2.0p25",
        "label":   "T500",
        "units":   "K",
        "desc":    "500 hPa temperature",
    },
    "T700": {
        "search":  ":TMP:700 mb:",
        "product": "pgrb2.0p25",
        "label":   "T700",
        "units":   "K",
        "desc":    "700 hPa temperature",
    },
    "T850": {
        "search":  ":TMP:850 mb:",
        "product": "pgrb2.0p25",
        "label":   "T850",
        "units":   "K",
        "desc":    "850 hPa temperature",
    },
    "U500": {
        "search":  ":UGRD:500 mb:",
        "product": "pgrb2.0p25",
        "label":   "U500",
        "units":   "m/s",
        "desc":    "500 hPa U-wind — do shear 0-6km",
    },
    "V500": {
        "search":  ":VGRD:500 mb:",
        "product": "pgrb2.0p25",
        "label":   "V500",
        "units":   "m/s",
        "desc":    "500 hPa V-wind — do shear 0-6km",
    },
    "U850": {
        "search":  ":UGRD:850 mb:",
        "product": "pgrb2.0p25",
        "label":   "U850",
        "units":   "m/s",
        "desc":    "850 hPa U-wind",
    },
    "V850": {
        "search":  ":VGRD:850 mb:",
        "product": "pgrb2.0p25",
        "label":   "V850",
        "units":   "m/s",
        "desc":    "850 hPa V-wind",
    },
    "RH500": {
        "search":  ":RH:500 mb:",
        "product": "pgrb2.0p25",
        "label":   "RH500",
        "units":   "%",
        "desc":    "500 hPa relative humidity",
    },
    "RH700": {
        "search":  ":RH:700 mb:",
        "product": "pgrb2.0p25",
        "label":   "RH700",
        "units":   "%",
        "desc":    "700 hPa relative humidity",
    },
    "RH850": {
        "search":  ":RH:850 mb:",
        "product": "pgrb2.0p25",
        "label":   "RH850",
        "units":   "%",
        "desc":    "850 hPa relative humidity",
    },

    # ── Indeksy stabilności ────────────────────────────────────────────────────
    # Bezpośrednio z GFS (potwierdzone w inventory)
    "LFTX": {
        "search":  ":LFTX:surface:",
        "product": "pgrb2.0p25",
        "label":   "LI",
        "units":   "K",
        "desc":    "Surface lifted index",
    },
    "4LFTX": {
        "search":  ":4LFTX:surface:",
        "product": "pgrb2.0p25",
        "label":   "4LFTX",
        "units":   "K",
        "desc":    "Best (4-layer) lifted index",
    },
    # UWAGA: KINDEX, TOTALX, DCAPE, LCL, LFC nie istnieją w pgrb2/pgrb2b GFS 0.25
    # Obliczamy je z dostępnych parametrów w module gfs_derived.py
}

# Parametry pogrupowane dla wygody pobierania
PARAM_GROUPS = {
    "cape_cin":   ["CAPE_SFC", "CIN_SFC", "CAPE_255_0", "CIN_255_0",
                   "CAPE_90_0", "CIN_90_0", "CAPE_180_0", "CIN_180_0", "PLPL"],
    "surface":    ["T2M", "D2M", "RH2M", "U10", "V10", "MSLP", "PWAT",
                   "APCP", "HPBL", "GUST"],
    "upper":      ["Z500", "T500", "T700", "T850",
                   "U500", "V500", "U850", "V850",
                   "RH500", "RH700", "RH850"],
    "wind":       ["U10", "V10", "U50", "V50", "U_30MB", "V_30MB",
                   "U_PBL", "V_PBL", "U500", "V500", "U850", "V850",
                   "USTM", "VSTM"],
    "convective": ["SRH_0_3", "LFTX", "4LFTX", "HPBL", "USTM", "VSTM"],
    # Parametry do obliczania shear 0-1km i 0-6km (wymagają wind group)
    "shear":      ["U10", "V10", "U_30MB", "V_30MB", "U500", "V500"],
    # Minimalny zestaw dla nowcastu burzowego
    "nowcast":    ["CAPE_SFC", "CIN_SFC", "CAPE_90_0", "CIN_90_0",
                   "CAPE_180_0", "CAPE_255_0",
                   "SRH_0_3", "USTM", "VSTM",
                   "U10", "V10", "U50", "V50",
                   "U_30MB", "V_30MB", "U_PBL", "V_PBL",
                   "U500", "V500", "U850", "V850",
                   "T2M", "D2M", "RH2M", "PWAT",
                   "LFTX", "4LFTX", "HPBL", "MSLP", "GUST",
                   "Z500", "T500", "T700", "T850",
                   "RH500", "RH700", "RH850"],
}

# Parametry których GFS nie dostarcza bezpośrednio — obliczane w gfs_derived.py
# z dostępnych danych:
#   SHEAR_0_1  = sqrt((U_30mb - U10)^2 + (V_30mb - V10)^2)
#   SHEAR_0_6  = sqrt((U500 - U10)^2 + (V500 - V10)^2)  (przybliżenie)
#   SRH_0_1    = obliczana z profilu wiatru + USTM/VSTM
#   KINDEX     = T850 - T500 + Td850 - (T700 - Td700)
#   TOTALX     = T850 - T500 + Td850
#   DCAPE      = brak w GFS pgrb2/pgrb2b — można aproksymować
#   LCL/LFC    = obliczane z T2M, Td2M, PLPL metodą Bolton (1980)
DERIVED_PARAMS = [
    "SHEAR_0_1", "SHEAR_0_6", "SRH_0_1",
    "KINDEX", "TOTALX", "LCL_est", "LFC_est",
]

ALL_PARAMS = list(PARAMS.keys())


# ── Herbie runner ─────────────────────────────────────────────────────────────

def get_latest_run_time() -> datetime:
    """
    Zwraca czas ostatniego dostępnego runu GFS jako naive datetime UTC.
    GFS startuje o 00z, 06z, 12z, 18z UTC.
    Dane są dostępne ~4h po czasie inicjalizacji.
    Herbie wymaga naive datetime (bez tzinfo).
    """
    now   = datetime.now(timezone.utc)
    cycle = (now.hour - 4) // 6 * 6   # cofnij o 4h bufor, zaokrąglij do 6h
    cycle = max(0, cycle)
    # Zwróć naive datetime — Herbie nie obsługuje aware datetime
    return now.replace(hour=cycle, minute=0, second=0, microsecond=0, tzinfo=None)


def fetch_param(param_key: str, run_dt: datetime, fxx: int,
                save_dir: str = GFS_DIR) -> dict | None:
    """
    Pobiera jeden parametr z GFS dla podanego runu i godziny prognozy.

    Zwraca dict z:
      data     — numpy array (lat, lon)
      lats     — array szerokości geogr.
      lons     — array długości geogr.
      meta     — metadane (jednostki, czas, opis)
    """
    try:
        from herbie import Herbie
    except ImportError:
        log.error("Herbie niedostępny: pip install herbie-data")
        return None

    if param_key not in PARAMS:
        log.error(f"Nieznany parametr: {param_key}")
        return None

    p       = PARAMS[param_key]
    search  = p["search"]
    product = p["product"]

    # Pomiń parametry które nie są dostępne dla danej godziny prognozy
    if fxx in p.get("skip_fxx", []):
        log.debug(f"  [{param_key}] pominięty dla fxx={fxx}")
        return None

    log.info(f"  [{param_key}] run={run_dt.strftime('%Y-%m-%d %Hz')} "
             f"f{fxx:03d} product={product} search='{search}'")

    try:
        H = Herbie(
            run_dt,
            model   = "gfs",
            product = product,
            fxx     = fxx,
            save_dir= save_dir,
            verbose = False,
        )

        # Pobierz dane jako xarray Dataset
        ds = H.xarray(search, remove_grib=True)

        # Wytnij bbox Polski
        ds = ds.sel(
            latitude  = slice(BBOX["north"], BBOX["south"]),
            longitude = slice(BBOX["west"],  BBOX["east"]),
        )

        # Wyciągnij pierwszą zmienną danych (pomijamy coords)
        data_vars = [v for v in ds.data_vars if v not in
                     ("gribfile_projection", "time", "step", "valid_time")]
        if not data_vars:
            log.warning(f"  [{param_key}] Brak zmiennych w datasecie")
            return None

        var_name = data_vars[0]
        da       = ds[var_name]
        values   = da.values.squeeze()

        # Współrzędne
        lats = ds.latitude.values
        lons = ds.longitude.values

        # Czas ważności
        valid_time = ds.valid_time.values if "valid_time" in ds else None
        if valid_time is not None and hasattr(valid_time, "astype"):
            import pandas as pd
            valid_time = pd.Timestamp(valid_time).to_pydatetime()

        return {
            "data":       values,
            "lats":       lats,
            "lons":       lons,
            "param_key":  param_key,
            "label":      p["label"],
            "units":      p["units"],
            "desc":       p["desc"],
            "run_time":   run_dt,
            "fxx":        fxx,
            "valid_time": valid_time,
            "shape":      values.shape,
            "val_min":    float(np.nanmin(values)),
            "val_max":    float(np.nanmax(values)),
            "val_mean":   float(np.nanmean(values)),
        }

    except Exception as e:
        log.warning(f"  [{param_key}] Błąd: {e}")
        return None


def fetch_all(run_dt: datetime | None = None,
              fxx: int = 0,
              param_keys: list[str] | None = None,
              save_dir: str = GFS_DIR) -> dict[str, dict | None]:
    """
    Pobiera wszystkie (lub wybrane) parametry dla danego runu i godziny.
    Zwraca {param_key: result_or_None}.
    """
    if run_dt is None:
        run_dt = get_latest_run_time()

    keys = param_keys or ALL_PARAMS
    log.info(f"Pobieranie {len(keys)} parametrów: run={run_dt.strftime('%Y-%m-%dT%Hz')} f{fxx:03d}")

    results = {}
    for key in keys:
        results[key] = fetch_param(key, run_dt, fxx, save_dir)

    ok  = sum(1 for r in results.values() if r is not None)
    err = len(keys) - ok
    log.info(f"Pobrano: ok={ok} err={err}")
    return results


def fetch_forecast(run_dt: datetime | None = None,
                   fxx_list: list[int] | None = None,
                   param_keys: list[str] | None = None,
                   save_dir: str = GFS_DIR) -> dict[int, dict[str, dict | None]]:
    """
    Pobiera parametry dla wielu godzin prognozy.
    Zwraca {fxx: {param_key: result}}.
    """
    if run_dt is None:
        run_dt = get_latest_run_time()
    if fxx_list is None:
        fxx_list = list(range(0, 49, 3))   # 0-48h co 3h

    results = {}
    for fxx in fxx_list:
        log.info(f"\n── f{fxx:03d} ──────────────────────────────────────")
        results[fxx] = fetch_all(run_dt, fxx, param_keys, save_dir)

    return results


# ── Eksploracja dostępnych zmiennych ─────────────────────────────────────────

def explore_gfs(run_dt: datetime | None = None, fxx: int = 0,
                product: str = "pgrb2.0p25"):
    """
    Wyświetla pełną listę dostępnych zmiennych w pliku GFS.
    Przydatne do weryfikacji nazw search patterns.
    """
    try:
        from herbie import Herbie
    except ImportError:
        print("Herbie niedostępny: pip install herbie-data")
        return

    if run_dt is None:
        run_dt = get_latest_run_time()

    print(f"\nInventory GFS {product} run={run_dt.strftime('%Y-%m-%dT%Hz')} f{fxx:03d}")
    print("="*70)

    H = Herbie(run_dt, model="gfs", product=product, fxx=fxx, verbose=False)
    inv = H.inventory()
    print(inv.to_string(max_rows=200))


# ── Zapis do NetCDF ───────────────────────────────────────────────────────────

def save_netcdf(results: dict[str, dict | None],
                run_dt: datetime, fxx: int,
                out_dir: str = GFS_DIR) -> str | None:
    """
    Zapisuje wszystkie pobrane parametry jako jeden plik NetCDF.
    Przydatne do archiwizacji i szybkiego odczytu.
    """
    try:
        import xarray as xr
    except ImportError:
        log.warning("xarray niedostępny — pomijam zapis NetCDF")
        return None

    datasets = {}
    ref_lats = ref_lons = None

    for key, r in results.items():
        if r is None:
            continue
        da = xr.DataArray(
            r["data"],
            dims   = ["latitude", "longitude"],
            coords = {
                "latitude":  r["lats"],
                "longitude": r["lons"],
            },
            name   = key,
            attrs  = {
                "label": r["label"],
                "units": r["units"],
                "description": r["desc"],
                "val_min": r["val_min"],
                "val_max": r["val_max"],
            }
        )
        datasets[key] = da
        ref_lats = r["lats"]
        ref_lons = r["lons"]

    if not datasets:
        log.warning("Brak danych do zapisu")
        return None

    ds = xr.Dataset(datasets)
    ds.attrs = {
        "model":      "GFS 0.25deg",
        "run_time":   run_dt.isoformat(),
        "fxx":        fxx,
        "bbox_n":     BBOX["north"],
        "bbox_s":     BBOX["south"],
        "bbox_w":     BBOX["west"],
        "bbox_e":     BBOX["east"],
        "created_at": datetime.now(timezone.utc).isoformat(),
        "attribution": "NOAA GFS — dane publiczne / public domain",
    }

    os.makedirs(out_dir, exist_ok=True)
    fname = os.path.join(
        out_dir,
        f"gfs_{run_dt.strftime('%Y%m%d_%H')}z_f{fxx:03d}.nc"
    )
    ds.to_netcdf(fname)
    log.info(f"Zapisano NetCDF: {fname}  ({os.path.getsize(fname)/1024:.0f} KB)")
    return fname


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="GFS Convective Parameters Ingestor")
    parser.add_argument("--run",     default=None,
                        help="Czas runu UTC, np. '2026-03-14 12:00'. Domyślnie: najnowszy.")
    parser.add_argument("--fxx",     nargs="+", type=int, default=[0],
                        help="Godziny prognozy, np. --fxx 0 6 12 24 48")
    parser.add_argument("--params",  nargs="+", default=None,
                        help="Parametry do pobrania, np. --params CAPE_SFC SRH_0_3. "
                             "Domyślnie wszystkie.")
    parser.add_argument("--group",   default=None,
                        choices=list(PARAM_GROUPS.keys()),
                        help="Predefiniowana grupa parametrów")
    parser.add_argument("--explore", action="store_true",
                        help="Pokaż dostępne zmienne w GFS i wyjdź")
    parser.add_argument("--product", default="pgrb2.0p25",
                        help="Produkt GFS do eksploracji (--explore)")
    parser.add_argument("--save-nc", action="store_true",
                        help="Zapisz wyniki jako NetCDF")
    parser.add_argument("--dir",     default=GFS_DIR,
                        help=f"Katalog danych (domyślnie: {GFS_DIR})")
    args = parser.parse_args()

    # Czas runu
    run_dt = None
    if args.run:
        from dateutil.parser import parse
        run_dt = parse(args.run).replace(tzinfo=None)   # Herbie wymaga naive
    else:
        run_dt = get_latest_run_time()

    print(f"\nGFS Ingestor")
    print(f"Run:    {run_dt.strftime('%Y-%m-%d %Hz UTC')}")
    print(f"Fxx:    {args.fxx}")
    print(f"BBox:   {BBOX['south']}–{BBOX['north']}N, {BBOX['west']}–{BBOX['east']}E")

    # Eksploracja
    if args.explore:
        explore_gfs(run_dt, args.fxx[0], args.product)
        return

    # Dobierz parametry
    param_keys = args.params
    if args.group:
        param_keys = PARAM_GROUPS[args.group]
    if param_keys is None:
        param_keys = ALL_PARAMS

    print(f"Parametry: {len(param_keys)}\n")

    # Pobierz dla każdej godziny prognozy
    for fxx in args.fxx:
        print(f"\n── f{fxx:03d} ──────────────────────────────────────")
        results = fetch_all(run_dt, fxx, param_keys, args.dir)

        # Podsumowanie
        print(f"\n  Wyniki f{fxx:03d}:")
        for key, r in results.items():
            if r is None:
                print(f"    {key:<16} ✗ BRAK")
            else:
                print(f"    {key:<16} ✓  "
                      f"{r['label']:<12} "
                      f"min={r['val_min']:>8.2f}  "
                      f"max={r['val_max']:>8.2f}  "
                      f"{r['units']}")

        # Zapis NetCDF
        if args.save_nc:
            save_netcdf(results, run_dt, fxx, args.dir)


if __name__ == "__main__":
    main()
