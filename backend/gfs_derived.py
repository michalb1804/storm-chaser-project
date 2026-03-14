"""
GFS Derived Convective Parameters
-----------------------------------
Oblicza parametry konwekcyjne których GFS nie dostarcza bezpośrednio,
na podstawie danych pobranych przez gfs_ingestor.py.

Użycie:
    from gfs_derived import compute_all, compute

    # Z wyników fetch_all() / fetch_param()
    derived = compute_all(gfs_results)

    # Pojedynczy parametr
    shear06 = compute("SHEAR_0_6", gfs_results)

Wszystkie funkcje przyjmują numpy arrays i zwracają numpy arrays.
Jednostki SI jeśli nie zaznaczono inaczej.

Źródła metod:
    Bolton (1980)              — LCL, LFC
    Thompson et al. (2003)     — SCP, STP
    Davies-Jones et al. (1990) — SRH
    SPC Mesoanalysis           — EHI, BRN, SWEAT
"""

import numpy as np
from typing import Any


# ── Helpery ───────────────────────────────────────────────────────────────────

def _get(results: dict, key: str) -> np.ndarray | None:
    """Wyciąga tablicę danych z wyników ingestora. None jeśli brak."""
    r = results.get(key)
    if r is None:
        return None
    return r.get("data")


def _require(*arrays) -> bool:
    """True jeśli wszystkie tablice są dostępne (nie None)."""
    return all(a is not None for a in arrays)


def _mag(u: np.ndarray, v: np.ndarray) -> np.ndarray:
    """Prędkość wiatru z komponentów U, V."""
    return np.sqrt(u**2 + v**2)


def _meta(label: str, units: str, desc: str,
          data: np.ndarray, results: dict) -> dict:
    """Buduje słownik wynikowy zgodny ze strukturą ingestora."""
    # Skopiuj lats/lons z pierwszego dostępnego parametru
    lats = lons = None
    for r in results.values():
        if r is not None and "lats" in r:
            lats = r["lats"]
            lons = r["lons"]
            break

    valid = data[~np.isnan(data)] if data is not None else np.array([])
    return {
        "data":      data,
        "lats":      lats,
        "lons":      lons,
        "label":     label,
        "units":     units,
        "desc":      desc,
        "derived":   True,
        "val_min":   float(np.nanmin(data)) if len(valid) > 0 else None,
        "val_max":   float(np.nanmax(data)) if len(valid) > 0 else None,
        "val_mean":  float(np.nanmean(data)) if len(valid) > 0 else None,
    }


# ── Wind shear ────────────────────────────────────────────────────────────────

def shear_0_1(results: dict) -> dict | None:
    """
    Bulk wind shear 0-1 km [m/s].
    Przybliżenie: różnica wektora wiatru między 10m a warstwą 30-0 mb AGL (~1 km).
    """
    u0 = _get(results, "U10")
    v0 = _get(results, "V10")
    u1 = _get(results, "U_30MB")
    v1 = _get(results, "V_30MB")
    if not _require(u0, v0, u1, v1):
        return None

    shear = _mag(u1 - u0, v1 - v0)
    return _meta("Shear0-1", "m/s",
                 "Bulk wind shear 0-1 km (10m vs 30-0 mb AGL)", shear, results)


def shear_0_6(results: dict) -> dict | None:
    """
    Bulk wind shear 0-6 km [m/s].
    Różnica wektora wiatru między 10m a 500 hPa (~5.5 km, przybliżenie 6 km).
    """
    u0 = _get(results, "U10")
    v0 = _get(results, "V10")
    u6 = _get(results, "U500")
    v6 = _get(results, "V500")
    if not _require(u0, v0, u6, v6):
        return None

    shear = _mag(u6 - u0, v6 - v0)
    return _meta("Shear0-6", "m/s",
                 "Bulk wind shear 0-6 km (10m vs 500 hPa)", shear, results)


def shear_0_3(results: dict) -> dict | None:
    """
    Bulk wind shear 0-3 km [m/s].
    Różnica wektora wiatru między 10m a 700 hPa (~3 km).
    """
    u0 = _get(results, "U10")
    v0 = _get(results, "V10")
    u3 = _get(results, "U850")   # 850 hPa ≈ 1.5 km
    v3 = _get(results, "V850")
    # Lepsze przybliżenie: PBL wind jako proxy dolnych 3 km
    u3_pbl = _get(results, "U_PBL")
    v3_pbl = _get(results, "V_PBL")
    if u3_pbl is not None: u3 = u3_pbl
    if v3_pbl is not None: v3 = v3_pbl
    if not _require(u0, v0, u3, v3):
        return None

    shear = _mag(u3 - u0, v3 - v0)
    return _meta("Shear0-3", "m/s",
                 "Bulk wind shear 0-3 km (10m vs PBL mean)", shear, results)


def mean_wind_0_6(results: dict) -> dict | None:
    """
    Średni wiatr 0-6 km [m/s] — komponent wektora (u, v) i prędkość.
    Używane do obliczania BRN shear i storm motion.
    """
    u0 = _get(results, "U10")
    v0 = _get(results, "V10")
    u6 = _get(results, "U500")
    v6 = _get(results, "V500")
    if not _require(u0, v0, u6, v6):
        return None

    u_mean = (u0 + u6) / 2
    v_mean = (v0 + v6) / 2
    speed  = _mag(u_mean, v_mean)
    return _meta("MeanWind0-6", "m/s",
                 "Mean wind 0-6 km", speed, results)


# ── SRH 0-1 km ────────────────────────────────────────────────────────────────

def srh_0_1(results: dict) -> dict | None:
    """
    Storm-relative helicity 0-1 km [m²/s²].
    Obliczana z profilu wiatru 10m + 30-0mb + storm motion (USTM/VSTM).
    Metoda: Davies-Jones et al. (1990) — całkowanie wektorowe.

    SRH = ∫(V - Vstorm) × (dV/dz) dz
    Przybliżenie dwuwarstwowe: (u_sfc, v_sfc) i (u_1km, v_1km)
    """
    u0   = _get(results, "U10")
    v0   = _get(results, "V10")
    u1   = _get(results, "U_30MB")
    v1   = _get(results, "V_30MB")
    ustm = _get(results, "USTM")
    vstm = _get(results, "VSTM")
    if not _require(u0, v0, u1, v1, ustm, vstm):
        return None

    # SRH z przybliżenia dwuwarstwowego
    # SRH ≈ (u0 - ustm)(v1 - v0) - (v0 - vstm)(u1 - u0)  [prostokąt]
    # + (u1 - ustm)(v1 - v0) - (v1 - vstm)(u1 - u0)  [górna warstwa]
    srh = ((u0 - ustm) * (v1 - v0) - (v0 - vstm) * (u1 - u0))
    return _meta("SRH0-1", "m2/s2",
                 "Storm-relative helicity 0-1 km (aproks. 2-warstwowa)", srh, results)


# ── Temperatura ───────────────────────────────────────────────────────────────

def lapse_rate_700_500(results: dict) -> dict | None:
    """
    Gradient temperatury 700-500 hPa [K/km] — wskaźnik niestabilności.
    Wartości > 7 K/km wskazują na środowisko sprzyjające burzom.
    """
    t500 = _get(results, "T500")
    t700 = _get(results, "T700")
    if not _require(t500, t700):
        return None

    # Przybliżona grubość warstwy: ~2.5 km
    lr = (t700 - t500) / 2.5
    return _meta("LR700-500", "K/km",
                 "Lapse rate 700-500 hPa", lr, results)


def lapse_rate_850_500(results: dict) -> dict | None:
    """
    Gradient temperatury 850-500 hPa [K/km].
    Warstwa ~4 km, wskaźnik ogólnej niestabilności.
    """
    t500 = _get(results, "T500")
    t850 = _get(results, "T850")
    if not _require(t500, t850):
        return None

    lr = (t850 - t500) / 4.0
    return _meta("LR850-500", "K/km",
                 "Lapse rate 850-500 hPa", lr, results)


def lapse_rate_sfc_500(results: dict) -> dict | None:
    """
    Gradient temperatury powierzchnia-500 hPa [K/km].
    Przybliżenie: zakładamy powierzchnię na ~0.5 km n.p.m.
    """
    t2m  = _get(results, "T2M")
    t500 = _get(results, "T500")
    if not _require(t2m, t500):
        return None

    lr = (t2m - t500) / 5.0   # ~5 km grubość warstwy
    return _meta("LR_SFC-500", "K/km",
                 "Lapse rate surface-500 hPa", lr, results)


# ── Indeksy stabilności ───────────────────────────────────────────────────────

def k_index(results: dict) -> dict | None:
    """
    K-Index [K].
    KI = T850 - T500 + Td850 - (T700 - Td700)
    Gdzie Td = punkt rosy obliczany z RH i T przez równanie Magnusa.

    Interpretacja:
      KI < 20  — burze mało prawdopodobne
      20-25    — izolowane burze możliwe
      26-30    — rozrzucone burze
      31-35    — liczne burze
      > 35     — ekstremalne środowisko burzowe
    """
    t850  = _get(results, "T850")
    t700  = _get(results, "T700")
    t500  = _get(results, "T500")
    rh850 = _get(results, "RH850")
    rh700 = _get(results, "RH700")
    if not _require(t850, t700, t500, rh850, rh700):
        return None

    # Punkt rosy z RH i T (równanie Magnus)
    td850 = _dewpoint(t850, rh850)
    td700 = _dewpoint(t700, rh700)

    # Formuła KI operuje na °C — różnice K = różnice °C, ale wartości bezwzględne nie
    t850_c  = t850  - 273.15
    t700_c  = t700  - 273.15
    t500_c  = t500  - 273.15
    td850_c = td850 - 273.15
    td700_c = td700 - 273.15

    ki = (t850_c - t500_c) + td850_c - (t700_c - td700_c)
    return _meta("K-Index", "°C",
                 "K-Index (T850-T500+Td850-(T700-Td700)), wartości w °C", ki, results)


def total_totals(results: dict) -> dict | None:
    """
    Total Totals Index [K].
    TT = (T850 + Td850) - 2*T500
    Wskaźnik gwałtowności potencjalnych burz.

    > 50 — burze możliwe
    > 55 — gwałtowne burze możliwe
    > 60 — tornada i gwałtowny grad możliwy
    """
    t850  = _get(results, "T850")
    t500  = _get(results, "T500")
    rh850 = _get(results, "RH850")
    if not _require(t850, t500, rh850):
        return None

    td850   = _dewpoint(t850, rh850)
    t850_c  = t850  - 273.15
    t500_c  = t500  - 273.15
    td850_c = td850 - 273.15

    tt = (t850_c + td850_c) - 2 * t500_c
    return _meta("TT", "°C",
                 "Total Totals Index (T850+Td850-2*T500), wartości w °C", tt, results)


def sweat_index(results: dict) -> dict | None:
    """
    SWEAT Index (Severe Weather Threat).
    SWEAT = 12*Td850 + 20*(TT-49) + 2*f8 + f5 + 125*(S+0.2)

    Gdzie f5, f8 = prędkość wiatru na 500/850 hPa [kt],
    S = sin(kierunek_500 - kierunek_850).

    > 300 — możliwe gwałtowne burze
    > 400 — możliwe tornada
    """
    t850  = _get(results, "T850")
    t500  = _get(results, "T500")
    rh850 = _get(results, "RH850")
    u850  = _get(results, "U850")
    v850  = _get(results, "V850")
    u500  = _get(results, "U500")
    v500  = _get(results, "V500")
    if not _require(t850, t500, rh850, u850, v850, u500, v500):
        return None

    td850 = _dewpoint(t850, rh850)
    tt    = total_totals(results)
    if tt is None:
        return None
    tt_val = tt["data"]

    # Prędkości w węzłach (1 m/s = 1.944 kt)
    f8 = _mag(u850, v850) * 1.944
    f5 = _mag(u500, v500) * 1.944

    # Kierunek wiatru [rad]
    dir850 = np.arctan2(-u850, -v850)
    dir500 = np.arctan2(-u500, -v500)
    s      = np.sin(dir500 - dir850)

    # Człon Td850 (w °C)
    td850_c = td850 - 273.15

    sweat = (12 * np.maximum(td850_c, 0) +
             20 * np.maximum(tt_val - 49, 0) +
             2 * f8 + f5 +
             125 * (s + 0.2))
    sweat = np.maximum(sweat, 0)

    return _meta("SWEAT", "adim",
                 "SWEAT Index (Severe Weather Threat)", sweat, results)


# ── LCL / LFC (Bolton 1980) ──────────────────────────────────────────────────

def lcl_height(results: dict) -> dict | None:
    """
    Szacowanie wysokości LCL [m] metodą Bolton (1980).
    LCL_T ≈ T - (T - Td) / 8.2  [przybliżona temperatura LCL w K]
    HLCL ≈ (T - LCL_T) * 125    [wysokość w metrach]

    Gdzie T = temperatura 2m, Td = punkt rosy 2m.
    """
    t2m = _get(results, "T2M")
    d2m = _get(results, "D2M")
    if not _require(t2m, d2m):
        return None

    # Bolton (1980): TLCL = 1 / (1/(Td-56) + ln(T/Td)/800) + 56
    td = d2m
    with np.errstate(divide="ignore", invalid="ignore"):
        tlcl = 1.0 / (1.0 / (td - 56.0) + np.log(t2m / td) / 800.0) + 56.0
    hlcl = (t2m - tlcl) * 125.0   # [m] — przybliżenie suchoadiabatyczne

    hlcl = np.maximum(hlcl, 0)
    return _meta("LCL", "m",
                 "LCL height AGL (Bolton 1980)", hlcl, results)


def delta_t(results: dict) -> dict | None:
    """
    Delta-T = T2m - Td2m [K] — niedobór punktu rosy przy powierzchni.
    Niskie wartości = duża wilgotność = sprzyjające dla konwekcji.
    Wysokie wartości (>15 K) = suche środowisko, ryzyko suchych mikrowybuchów.
    """
    t2m = _get(results, "T2M")
    d2m = _get(results, "D2M")
    if not _require(t2m, d2m):
        return None

    dt = t2m - d2m
    return _meta("DeltaT", "K",
                 "T2m - Td2m (dew point depression)", dt, results)


# ── Parametry kompozytowe ─────────────────────────────────────────────────────

def energy_helicity_index(results: dict) -> dict | None:
    """
    Energy-Helicity Index (EHI) dla 0-1 km.
    EHI = (CAPE * SRH) / 160000
    Gdzie SRH = SRH 0-3 km (z GFS) lub SRH 0-1 km (obliczone).

    Interpretacja:
      EHI > 1  — tornada możliwe
      EHI > 2  — znaczące tornada możliwe
      EHI > 5  — silne tornada możliwe
    """
    cape = _get(results, "CAPE_SFC")
    # Preferuj SRH 0-1, fallback na SRH 0-3
    srh_r = results.get("SRH_0_1_derived") or results.get("SRH_0_3")
    if srh_r is None:
        return None
    srh = srh_r.get("data") if isinstance(srh_r, dict) else srh_r

    if not _require(cape, srh):
        return None

    ehi = (np.maximum(cape, 0) * np.maximum(srh, 0)) / 160000.0
    return _meta("EHI", "adim",
                 "Energy-Helicity Index (CAPE*SRH/160000)", ehi, results)


def bulk_richardson_number(results: dict) -> dict | None:
    """
    Bulk Richardson Number (BRN).
    BRN = CAPE / (0.5 * Vshear²)
    Gdzie Vshear = bulk wind shear 0-6 km.

    Interpretacja:
      BRN 10-45   — sprzyjające supercellom
      BRN < 10    — zbyt duży shear (liniowe systemy)
      BRN > 45    — zbyt mały shear (wielokomórkowe, słabe rotacja)
    """
    cape = _get(results, "CAPE_SFC")
    sh06_r = results.get("SHEAR_0_6_derived")
    if sh06_r is None:
        return None
    sh06 = sh06_r.get("data") if isinstance(sh06_r, dict) else sh06_r

    if not _require(cape, sh06):
        return None

    with np.errstate(divide="ignore", invalid="ignore"):
        brn = np.where(sh06 > 0.1,
                       np.maximum(cape, 0) / (0.5 * sh06**2),
                       np.nan)
    return _meta("BRN", "adim",
                 "Bulk Richardson Number (CAPE / 0.5*shear²)", brn, results)


def supercell_composite(results: dict) -> dict | None:
    """
    Supercell Composite Parameter (SCP) — Thompson et al. (2003).
    SCP = (MUCAPE/1000) * (SRH_0_3/50) * (Shear_0_6/20)

    Normalizacja: MUCAPE w J/kg, SRH w m²/s², Shear w m/s.

    Interpretacja:
      SCP > 1   — środowisko sprzyjające supercellom
      SCP > 4   — silne środowisko supercellowe
    """
    mucape = _get(results, "CAPE_90_0")
    srh03  = _get(results, "SRH_0_3")

    sh06_r = results.get("SHEAR_0_6_derived")
    if sh06_r is None:
        return None
    sh06 = sh06_r.get("data") if isinstance(sh06_r, dict) else sh06_r

    if not _require(mucape, srh03, sh06):
        return None

    scp = (np.maximum(mucape, 0) / 1000.0) * \
          (np.maximum(srh03, 0) / 50.0) * \
          (np.maximum(sh06, 0) / 20.0)

    return _meta("SCP", "adim",
                 "Supercell Composite Parameter (Thompson 2003)", scp, results)


def sig_tornado_parameter(results: dict) -> dict | None:
    """
    Significant Tornado Parameter (STP) — Thompson et al. (2003).
    STP = (SBCAPE/1500) * (LCL_est/1500 factor) * (SRH_0_1/150) * (Shear_0_6/20)

    LCL factor: 1 jeśli LCL < 1000m, liniowo do 0 przy LCL = 2000m.

    Interpretacja:
      STP > 1  — warunki sprzyjające znaczącym tornadom (EF2+)
      STP > 3  — silne warunki tornadogenne
    """
    sbcape = _get(results, "CAPE_SFC")

    lcl_r = results.get("LCL_derived")
    if lcl_r is None:
        return None
    lcl = lcl_r.get("data") if isinstance(lcl_r, dict) else lcl_r

    srh01_r = results.get("SRH_0_1_derived")
    if srh01_r is None:
        srh03 = _get(results, "SRH_0_3")
        if srh03 is None:
            return None
        srh_val = srh03 / 3.0   # przybliżenie SRH 0-1 jako 1/3 SRH 0-3
    else:
        srh_val = srh01_r.get("data") if isinstance(srh01_r, dict) else srh01_r

    sh06_r = results.get("SHEAR_0_6_derived")
    if sh06_r is None:
        return None
    sh06 = sh06_r.get("data") if isinstance(sh06_r, dict) else sh06_r

    if not _require(sbcape, lcl, srh_val, sh06):
        return None

    # LCL factor
    lcl_factor = np.where(lcl <= 1000, 1.0,
                 np.where(lcl >= 2000, 0.0,
                          (2000 - lcl) / 1000.0))

    stp = (np.maximum(sbcape, 0) / 1500.0) * \
           lcl_factor * \
          (np.maximum(srh_val, 0) / 150.0) * \
          (np.maximum(sh06, 0) / 20.0)

    return _meta("STP", "adim",
                 "Significant Tornado Parameter (Thompson 2003)", stp, results)


def sig_hail_parameter(results: dict) -> dict | None:
    """
    Significant Hail Parameter (SHIP) — SPC.
    SHIP = (MUCAPE/1000) * (MUPW/20) * (T500/-10) * (LR700_500/6.5) * (Shear0_6/20)
    gdzie T500 w °C, LR w K/km.

    Proxy MUPW (Mixing ratio Unstable Parcel) ≈ PWAT/20 jako przybliżenie.

    Interpretacja:
      SHIP > 0.5 — grad ≥ 2 cm możliwy
      SHIP > 1   — grad ≥ 4 cm (duży grad) możliwy
    """
    mucape  = _get(results, "CAPE_90_0")
    pwat    = _get(results, "PWAT")
    t500    = _get(results, "T500")

    lr_r = results.get("LR700_500_derived")
    if lr_r is None:
        return None
    lr = lr_r.get("data") if isinstance(lr_r, dict) else lr_r

    sh06_r = results.get("SHEAR_0_6_derived")
    if sh06_r is None:
        return None
    sh06 = sh06_r.get("data") if isinstance(sh06_r, dict) else sh06_r

    if not _require(mucape, pwat, t500, lr, sh06):
        return None

    t500_c = t500 - 273.15   # K → °C

    # T500 factor: wzmocnienie przy zimnym 500 hPa (lód gradu)
    t500_factor = np.where(t500_c > -5, t500_c / -5.0, 1.0)
    t500_factor = np.maximum(t500_factor, 0)

    ship = (np.maximum(mucape, 0) / 1000.0) * \
           (pwat / 20.0) * \
           t500_factor * \
           (np.maximum(lr, 0) / 6.5) * \
           (np.maximum(sh06, 0) / 20.0)

    return _meta("SHIP", "adim",
                 "Significant Hail Parameter (SPC)", ship, results)


def derecho_composite(results: dict) -> dict | None:
    """
    Derecho Composite Parameter (DCP) — Evans & Doswell (2001).
    DCP = (DCAPE_proxy/980) * (MUCAPE/2000) * (Shear_0_6/20) * (Mean_Wind_0_6/16)

    DCAPE_proxy ≈ CAPE_180_0 jako przybliżenie downdraft CAPE.
    """
    mucape   = _get(results, "CAPE_90_0")
    dcape_p  = _get(results, "CAPE_180_0")   # proxy

    sh06_r = results.get("SHEAR_0_6_derived")
    mw06_r = results.get("MeanWind0_6_derived")
    if sh06_r is None or mw06_r is None:
        return None
    sh06 = sh06_r.get("data") if isinstance(sh06_r, dict) else sh06_r
    mw06 = mw06_r.get("data") if isinstance(mw06_r, dict) else mw06_r

    if not _require(mucape, dcape_p, sh06, mw06):
        return None

    dcp = (np.maximum(dcape_p, 0) / 980.0) * \
          (np.maximum(mucape, 0) / 2000.0) * \
          (np.maximum(sh06, 0) / 20.0) * \
          (mw06 / 16.0)

    return _meta("DCP", "adim",
                 "Derecho Composite Parameter (Evans & Doswell 2001)", dcp, results)


def downburst_index(results: dict) -> dict | None:
    """
    Uproszczony wskaźnik środowiska microburstu.
    Proxy oparty na: CAPE + dry layer (wysokie RH700 ale niskie RH500)
    i gradient temperatury 850-500.

    Wartości > 1 wskazują na możliwość mokrych microburst.
    """
    cape  = _get(results, "CAPE_SFC")
    if cape is None:
        cape = _get(results, "CAPE_90_0")
    rh700 = _get(results, "RH700")
    rh500 = _get(results, "RH500")
    hpbl  = _get(results, "HPBL")
    if not _require(cape, rh700, rh500, hpbl):
        return None

    # Suche powietrze w środkowej troposferze sprzyja evaporative cooling
    dry_factor = np.maximum(1 - rh500 / 100.0, 0)
    wet_factor = np.maximum(rh700 / 100.0, 0)

    dbi = (np.maximum(cape, 0) / 1000.0) * dry_factor * wet_factor * \
          (np.minimum(hpbl, 2000) / 1000.0)

    return _meta("DBI", "adim",
                 "Downburst Index (proxy)", dbi, results)


# ── Pomocnicze obliczenia meteo ───────────────────────────────────────────────

def _dewpoint(T: np.ndarray, RH: np.ndarray) -> np.ndarray:
    """
    Punkt rosy z temperatury [K] i wilgotności względnej [%].
    Metoda: Magnus formula.
    """
    T_c  = T - 273.15
    RH_f = np.clip(RH, 0.001, 100.0)

    # Magnus: γ = ln(RH/100) + (17.625 * T) / (243.04 + T)
    with np.errstate(divide="ignore", invalid="ignore"):
        gamma = np.log(np.maximum(RH_f, 0.001) / 100.0) +                 (17.625 * T_c) / (243.04 + T_c)
        denom = 17.625 - gamma
        td_c  = np.where(np.abs(denom) > 1e-6,
                         243.04 * gamma / denom,
                         T_c)   # fallback: Td ≈ T przy RH≈100%
    return td_c + 273.15   # → K


def mixing_ratio_0_1(results: dict) -> dict | None:
    """
    Przybliżony mixing ratio w warstwie 0-1 km [g/kg].
    Obliczany z T2m i Td2m.
    """
    t2m = _get(results, "T2M")
    d2m = _get(results, "D2M")
    if not _require(t2m, d2m):
        return None

    # Prężność nasycenia (hPa), formuła Tetens
    es  = 6.112 * np.exp(17.67 * (d2m - 273.15) / (d2m - 29.65))
    mslp_data = _get(results, "MSLP")
    p   = (mslp_data / 100.0) if mslp_data is not None else np.full_like(t2m, 1013.25)

    w   = 621.97 * es / (p - es)   # g/kg
    return _meta("MR0-1", "g/kg",
                 "Mixing ratio 0-1 km (from T2m/Td2m)", w, results)


def cold_pool_strength(results: dict) -> dict | None:
    """
    Cold pool strength proxy — różnica temperatury 850 hPa i 2m.
    Duże wartości (>10 K) wskazują na silne chłodne baseny powietrza
    pod burzami — sprzyjające derechom.
    """
    t2m  = _get(results, "T2M")
    t850 = _get(results, "T850")
    if not _require(t2m, t850):
        return None

    # Przelicz 850 hPa ~1.5 km; normalizacja do poziomu morza
    cps = t2m - t850   # różnica K = różnica °C
    return _meta("CPS", "°C",
                 "Cold pool strength proxy (T2m - T850)", cps, results)


def equilibrium_level_temp(results: dict) -> dict | None:
    """
    Przybliżona temperatura poziomu równowagi (EL) [K].
    EL jest tam gdzie T parcela = T środowiska.
    Proxy: dla CAPE > 0, EL ≈ T500 + CAPE/1500 (bardzo przybliżone).
    Właściwy EL wymaga sondowania — tu dajemy tylko orientacyjny wskaźnik.
    """
    cape = _get(results, "CAPE_SFC")
    t500 = _get(results, "T500")
    if not _require(cape, t500):
        return None

    el_t_c = (t500 - 273.15) + np.sqrt(np.maximum(cape, 0)) / 30.0
    return _meta("EL_T", "°C",
                 "Equilibrium level temperature proxy", el_t_c, results)


# ── Główna funkcja ────────────────────────────────────────────────────────────

# Mapa wszystkich parametrów pochodnych
DERIVED_CATALOG = {
    # Shear
    "SHEAR_0_1_derived":    shear_0_1,
    "SHEAR_0_6_derived":    shear_0_6,
    "SHEAR_0_3_derived":    shear_0_3,
    "MeanWind0_6_derived":  mean_wind_0_6,
    # SRH
    "SRH_0_1_derived":      srh_0_1,
    # Lapse rate
    "LR700_500_derived":    lapse_rate_700_500,
    "LR850_500_derived":    lapse_rate_850_500,
    "LR_SFC500_derived":    lapse_rate_sfc_500,
    # Indeksy
    "K_INDEX_derived":      k_index,
    "TT_derived":           total_totals,
    "SWEAT_derived":        sweat_index,
    # LCL / punkt rosy
    "LCL_derived":          lcl_height,
    "DeltaT_derived":       delta_t,
    "MR0_1_derived":        mixing_ratio_0_1,
    # Parametry kompozytowe
    "EHI_derived":          energy_helicity_index,
    "BRN_derived":          bulk_richardson_number,
    "SCP_derived":          supercell_composite,
    "STP_derived":          sig_tornado_parameter,
    "SHIP_derived":         sig_hail_parameter,
    "DCP_derived":          derecho_composite,
    "DBI_derived":          downburst_index,
    "CPS_derived":          cold_pool_strength,
    "EL_T_derived":         equilibrium_level_temp,
}

# Kolejność obliczeń — niektóre parametry zależą od innych
# (np. SCP wymaga SHEAR_0_6, STP wymaga LCL i SRH_0_1)
COMPUTE_ORDER = [
    # Najpierw podstawowe
    "SHEAR_0_1_derived",
    "SHEAR_0_6_derived",
    "SHEAR_0_3_derived",
    "MeanWind0_6_derived",
    "SRH_0_1_derived",
    "LR700_500_derived",
    "LR850_500_derived",
    "LR_SFC500_derived",
    "K_INDEX_derived",
    "TT_derived",
    "LCL_derived",
    "DeltaT_derived",
    "MR0_1_derived",
    "CPS_derived",
    "EL_T_derived",
    # Potem złożone (wymagają powyższych)
    "SWEAT_derived",
    "EHI_derived",
    "BRN_derived",
    "SCP_derived",
    "STP_derived",
    "SHIP_derived",
    "DCP_derived",
    "DBI_derived",
]


def compute(param_key: str, gfs_results: dict) -> dict | None:
    """Oblicza jeden parametr pochodny."""
    fn = DERIVED_CATALOG.get(param_key)
    if fn is None:
        return None
    return fn(gfs_results)


def compute_all(gfs_results: dict,
                keys: list[str] | None = None) -> dict[str, dict | None]:
    """
    Oblicza wszystkie (lub wybrane) parametry pochodne.

    Wyniki są stopniowo dodawane do słownika — każdy parametr
    może korzystać z poprzednio obliczonych pochodnych.

    Zwraca {param_key: result_or_None}.
    """
    keys = keys or COMPUTE_ORDER
    # Połącz dane GFS z wynikami pochodnymi do wspólnego słownika
    combined = dict(gfs_results)
    results  = {}

    for key in keys:
        fn = DERIVED_CATALOG.get(key)
        if fn is None:
            continue
        result = fn(combined)
        results[key]  = result
        combined[key] = result   # udostępnij kolejnym obliczeniom

    return results


# ── Demo ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s  %(levelname)-7s  %(message)s",
                        datefmt="%H:%M:%S")

    print("GFS Derived Parameters — test")
    print("Pobieranie danych GFS (nowcast group) ...\n")

    from gfs_ingestor import fetch_all, get_latest_run_time, PARAM_GROUPS

    run_dt = get_latest_run_time()
    gfs    = fetch_all(run_dt, fxx=0,
                       param_keys=PARAM_GROUPS["nowcast"])

    print("\nObliczanie parametrów pochodnych ...")
    derived = compute_all(gfs)

    print(f"\n{'='*65}")
    print(f"  {'Parametr':<20} {'Min':>10} {'Max':>10}  Jednostki")
    print(f"{'='*65}")
    for key, r in derived.items():
        if r is None:
            print(f"  {key:<20} {'BRAK':>10}")
            continue
        label = r.get("label", key.replace("_derived", ""))
        vmin  = r.get("val_min")
        vmax  = r.get("val_max")
        units = r.get("units", "")
        vmin_s = f"{vmin:>10.2f}" if vmin is not None else f"{'—':>10}"
        vmax_s = f"{vmax:>10.2f}" if vmax is not None else f"{'—':>10}"
        print(f"  {label:<20} {vmin_s} {vmax_s}  {units}")
