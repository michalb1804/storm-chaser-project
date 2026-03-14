"""
IMGW Radar — Przekrój Pionowy (VSP)
------------------------------------
Wyciąga i wizualizuje przekrój pionowy przez burzę z plików .max.h5

Każdy plik indywidualnego radaru zawiera trzy datasety:
  dataset1: MAX  (500×500)   — mapa pozioma (używana przez ingestor)
  dataset2: VSP  (500×100)   — przekrój pionowy W→E przez radar
  dataset3: HSP  (100×500)   — przekrój poziomy N→S przez radar

VSP: oś Y = wysokość [0.5–18 km], oś X = odległość wzdłuż linii W→E [km]
     środek X = pozycja radaru

Uruchom: python imgw_vsp.py
         python imgw_vsp.py --radar leg --quantity KDP
         python imgw_vsp.py --file radar_data/LEG_KDP/2026031409150700.h5

Wymagania:
    pip install h5py numpy matplotlib requests pyproj
"""

import argparse
import os
import sys
import h5py
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from datetime import datetime, timezone

# Importuj z ingestora
sys.path.insert(0, os.path.dirname(__file__))
from imgw_radar import (
    PRODUCTS, find_latest, make_url, make_timestamp,
    download_file, DATA_DIR,
)


# ── Metadane radarów (pozycja, do opisu osi X) ────────────────────────────────

RADAR_META = {
    "leg": {"name": "Legionowo",          "lat": 52.4053, "lon": 20.9611},
    "brz": {"name": "Brzuchania",         "lat": 50.3939, "lon": 20.0681},
    "gdy": {"name": "Gdynia",             "lat": 54.5856, "lon": 18.5517},
    "gsa": {"name": "Góra Św. Anny",      "lat": 50.4581, "lon": 18.1353},
    "pas": {"name": "Pastewnik",          "lat": 50.4344, "lon": 16.7928},
    "poz": {"name": "Poznań",             "lat": 52.4131, "lon": 16.7970},
    "ram": {"name": "Ramża",              "lat": 53.3967, "lon": 22.1597},
    "rze": {"name": "Rzeszów",            "lat": 50.1139, "lon": 22.0367},
    "swi": {"name": "Świdnik",            "lat": 51.2197, "lon": 22.7033},
    "uzr": {"name": "Użranki",            "lat": 53.8617, "lon": 20.9158},
}

# Dostępne ilości polaryzacyjne w plikach .max.h5
QUANTITIES = {
    "KDP":   ("KDP.max.h5",   "KDP [deg/km]",     "RdYlBu_r", -0.5, 3.0),
    "ZDR":   ("ZDR.max.h5",   "ZDR [dB]",         "RdYlGn",   -1.0, 5.0),
    "RhoHV": ("RhoHV.max.h5", "RhoHV [-]",         "plasma",    0.6, 1.05),
    "DBZH":  ("dBZ.ppi.h5",   "DBZH [dBZ]",       "nws_dbz",  -10,  70),
}


# ── Parser VSP z pliku HDF5 ───────────────────────────────────────────────────

def parse_vsp(filepath: str) -> dict | None:
    """
    Wyciąga VSP (dataset2) i HSP (dataset3) z pliku .max.h5

    VSP — przekrój pionowy W→E:
      shape (ysize, xsize) = (500, 100)
      Y: wysokość od minheight do maxheight [m]
      X: odległość od W do E przez radar [m]

    HSP — przekrój pionowy N→S:
      shape (ysize, xsize) = (100, 500)
      Y: wysokość
      X: odległość od N do S przez radar
    """
    result = {}

    try:
        with h5py.File(filepath, "r") as f:

            # Przejdź przez wszystkie datasety
            datasets_found = {}
            def collect(name, obj):
                if not (isinstance(obj, h5py.Dataset) and name.endswith("/data")):
                    return
                ds_root = name.split("/")[0]
                w_path  = f"{ds_root}/what"
                wr_path = f"{ds_root}/where"
                if w_path not in f:
                    return

                product_tag = f[w_path].attrs.get("product", b"")
                if isinstance(product_tag, (bytes, np.bytes_)):
                    product_tag = product_tag.decode()

                datasets_found[product_tag] = {
                    "data_path":  name,
                    "what_path":  w_path,
                    "where_path": wr_path if wr_path in f else None,
                }
            f.visititems(collect)

            for product_tag in ("VSP", "HSP"):
                if product_tag not in datasets_found:
                    continue

                info       = datasets_found[product_tag]
                raw        = f[info["data_path"]][()].astype(float)
                what_attrs = dict(f[info["what_path"]].attrs)

                gain     = float(what_attrs.get("gain",     1.0))
                offset   = float(what_attrs.get("offset",   0.0))
                nodata   = float(what_attrs.get("nodata",   255.0))
                undetect = float(what_attrs.get("undetect", 0.0))
                quantity = what_attrs.get("quantity", b"?")
                if isinstance(quantity, (bytes, np.bytes_)):
                    quantity = quantity.decode()

                data = raw * gain + offset
                data[raw == nodata]   = np.nan
                data[raw == undetect] = np.nan

                # Odczytaj geometrię przekroju
                where_attrs = {}
                if info["where_path"] and info["where_path"] in f:
                    where_attrs = dict(f[info["where_path"]].attrs)

                xscale    = float(where_attrs.get("xscale",    1000.0))
                yscale    = float(where_attrs.get("yscale",    1000.0))
                xsize     = int(where_attrs.get("xsize",    data.shape[1]))
                ysize     = int(where_attrs.get("ysize",    data.shape[0]))
                minheight = float(where_attrs.get("minheight", 500.0))
                maxheight = float(where_attrs.get("maxheight", 18000.0))

                # Oś X: odległość [km] od lewej krawędzi przekroju
                # Środek = pozycja radaru
                # Zakres: xsize * xscale metrów, środek w xsize//2
                half_range_km = (xsize * xscale / 2) / 1000
                x_km = np.linspace(-half_range_km, half_range_km, xsize)

                # Oś Y: wysokość [km]
                y_km = np.linspace(minheight / 1000, maxheight / 1000, ysize)

                result[product_tag] = {
                    "data":      data,
                    "quantity":  quantity,
                    "x_km":      x_km,        # odległość od radaru [km], W(-) → E(+)
                    "y_km":      y_km,        # wysokość [km n.p.m.]
                    "xscale_m":  xscale,
                    "yscale_m":  yscale,
                    "shape":     data.shape,
                    "minheight": minheight,
                    "maxheight": maxheight,
                    "gain":      gain,
                    "offset":    offset,
                }

            # Metadane pliku
            root_what = dict(f["what"].attrs) if "what" in f else {}
            root_how  = dict(f["how"].attrs)  if "how"  in f else {}
            result["_meta"] = {
                "filepath": filepath,
                "what":     root_what,
                "how":      root_how,
            }

    except Exception as e:
        print(f"  ! Błąd parsowania VSP {filepath}: {e}")
        import traceback; traceback.print_exc()
        return None

    if not any(k in result for k in ("VSP", "HSP")):
        print(f"  ! Brak VSP/HSP w {filepath}")
        return None

    return result


# ── Palety kolorów ────────────────────────────────────────────────────────────

def get_cmap_vrange(quantity: str):
    """Zwraca (cmap, vmin, vmax) dla danej ilości."""
    presets = {
        "KDP":   ("RdYlBu_r", -0.5,  3.0),
        "ZDR":   ("RdYlGn",   -1.0,  5.0),
        "RHOHV": ("plasma",    0.6,  1.05),
        "DBZH":  (None,       -10,   70),   # None = użyj NWS poniżej
    }
    q = quantity.upper()
    cmap_name, vmin, vmax = presets.get(q, ("turbo", None, None))

    if cmap_name is None:
        # NWS dBZ palette
        stops = [
            (-10, "#000000"), (0,  "#04e9e7"), (5,  "#019ff4"), (10, "#0300f4"),
            (15,  "#02fd02"), (20, "#01c501"), (25, "#008e00"), (30, "#fdf802"),
            (35,  "#e5bc00"), (40, "#fd9500"), (45, "#fd0000"), (50, "#d40000"),
            (55,  "#bc0000"), (60, "#f800fd"), (65, "#9854c6"), (70, "#ffffff"),
        ]
        cmap = mcolors.LinearSegmentedColormap.from_list(
            "nws_dbz", [((v - vmin) / (vmax - vmin), c) for v, c in stops])
    else:
        cmap = plt.cm.get_cmap(cmap_name)

    cmap.set_bad(color=(0, 0, 0, 0))
    return cmap, vmin, vmax


# ── Wizualizacja ──────────────────────────────────────────────────────────────

def plot_vsp(vsp_data: dict, radar_code: str = "?",
             show_hsp: bool = True, save_path: str = None):
    """
    Rysuje przekrój pionowy VSP (i opcjonalnie HSP).

    Layout:
      Jeśli show_hsp i HSP dostępne:
        [VSP — przekrój W→E]
        [HSP — przekrój N→S]
      Inaczej:
        [VSP — pełna szerokość]
    """
    has_vsp = "VSP" in vsp_data
    has_hsp = "HSP" in vsp_data and show_hsp

    if not has_vsp and not has_hsp:
        print("  Brak danych VSP/HSP do wyświetlenia")
        return

    meta      = vsp_data.get("_meta", {})
    root_what = meta.get("what", {})
    root_how  = meta.get("how", {})

    # Czas skanu
    ts = root_what.get("time", b"")
    ds = root_what.get("date", b"")
    if isinstance(ts, (bytes, np.bytes_)): ts = ts.decode()
    if isinstance(ds, (bytes, np.bytes_)): ds = ds.decode()
    time_label = f"{ds[:4]}-{ds[4:6]}-{ds[6:]} {ts[:2]}:{ts[2:4]} UTC" if ds else ""

    radar_name = RADAR_META.get(radar_code.lower(), {}).get("name", radar_code.upper())
    system     = root_how.get("system", b"")
    if isinstance(system, (bytes, np.bytes_)): system = system.decode()

    n_panels = (1 if has_vsp else 0) + (1 if has_hsp else 0)
    fig, axes = plt.subplots(n_panels, 1,
                             figsize=(14, 5 * n_panels),
                             squeeze=False)
    fig.patch.set_facecolor("#0d1117")

    panel = 0
    for section_key, direction, xlabel in [
        ("VSP", "W → E", "Odległość od radaru W→E [km]"),
        ("HSP", "N → S", "Odległość od radaru N→S [km]"),
    ]:
        if section_key not in vsp_data:
            continue
        if section_key == "HSP" and not show_hsp:
            continue

        sd   = vsp_data[section_key]
        data = sd["data"]
        x_km = sd["x_km"]
        y_km = sd["y_km"]
        qty  = sd["quantity"]

        cmap, vmin, vmax = get_cmap_vrange(qty)
        if vmin is None:
            vmin = float(np.nanpercentile(data, 2))
            vmax = float(np.nanpercentile(data, 98))

        ax = axes[panel, 0]
        ax.set_facecolor("#0d1117")

        # Główny obraz — pcolormesh daje właściwe wymiary pikseli
        XX, YY = np.meshgrid(x_km, y_km)
        im = ax.pcolormesh(XX, YY, data,
                           cmap=cmap, vmin=vmin, vmax=vmax,
                           shading="auto", rasterized=True)

        # Colorbar
        cbar = plt.colorbar(im, ax=ax, fraction=0.02, pad=0.01)
        cbar.set_label(qty, fontsize=9, color="#d1d5db")
        cbar.ax.yaxis.set_tick_params(color="#9ca3af", labelsize=8)
        plt.setp(cbar.ax.yaxis.get_ticklabels(), color="#9ca3af")

        # Linia pozycji radaru (x=0)
        ax.axvline(x=0, color="#ffffff", linewidth=0.8,
                   linestyle="--", alpha=0.5, label=f"Radar ({radar_name})")

        # Poziomy referencyjne wysokości
        for h_km, label, ls in [
            (0.0,  "",          "-"),
            (1.0,  "1 km",     ":"),
            (3.0,  "3 km",     ":"),
            (5.0,  "5 km",     ":"),
            (10.0, "10 km",    ":"),
            (15.0, "15 km",    ":"),
        ]:
            if y_km.min() <= h_km <= y_km.max():
                ax.axhline(y=h_km, color="#374151", linewidth=0.5,
                           linestyle=ls, alpha=0.7)
                if label:
                    ax.text(x_km.max() * 0.98, h_km + 0.1, label,
                            color="#6b7280", fontsize=7, ha="right", va="bottom")

        # Izolinia 0°C (orientacyjna, ~3.5 km latem, ~1.5 km zimą) — opcjonalna
        # ax.axhline(y=3.5, color="#60a5fa", linewidth=1.0, linestyle="-.", alpha=0.6)
        # ax.text(x_km.min()+1, 3.6, "0°C (szac.)", color="#60a5fa", fontsize=7)

        ax.set_xlabel(xlabel, fontsize=9, color="#9ca3af")
        ax.set_ylabel("Wysokość [km n.p.m.]", fontsize=9, color="#9ca3af")
        ax.set_ylim(y_km.min(), y_km.max())
        ax.set_xlim(x_km.min(), x_km.max())
        ax.tick_params(colors="#9ca3af", labelsize=8)
        for spine in ax.spines.values():
            spine.set_edgecolor("#374151")

        ax.set_title(
            f"Przekrój pionowy {direction} — {qty}  |  {radar_name}  |  {time_label}",
            fontsize=10, color="#f3f4f6", pad=6)

        # Adnotacja rozdzielczości
        ax.text(0.01, 0.02,
                f"Δx={sd['xscale_m']:.0f} m  Δz={sd['yscale_m']:.0f} m  "
                f"shape={sd['shape']}",
                transform=ax.transAxes,
                fontsize=7, color="#6b7280", va="bottom")

        panel += 1

    fig.text(0.01, 0.005,
        "Źródłem danych jest Instytut Meteorologii i Gospodarki Wodnej – PIB",
        fontsize=6, color="#6b7280")

    plt.tight_layout(rect=[0, 0.02, 1, 1])

    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches="tight",
                    facecolor=fig.get_facecolor())
        print(f"  Zapisano: {save_path}")
    plt.show()


# ── Funkcja pomocnicza: pobierz i wykreśl VSP dla radaru ─────────────────────

def fetch_and_plot_vsp(radar: str = "leg", quantity: str = "KDP",
                       filepath: str = None, save: bool = True):
    """
    Pobiera najnowszy plik .max.h5 dla podanego radaru i ilości,
    wyciąga VSP+HSP i rysuje przekrój.

    radar    — kod radaru: leg, brz, gdy, gsa, pas, poz, ram, rze, swi, uzr
    quantity — KDP, ZDR, RhoHV
    filepath — jeśli podany, użyj tego pliku zamiast pobierać
    """
    radar = radar.lower()
    qty_upper = quantity.upper()
    if qty_upper == "RHOHV":
        qty_upper = "RhoHV"

    # Znajdź klucz produktu
    product_key = f"{radar.upper()}_{qty_upper.upper().replace('RHOHV', 'RHOHV')}"
    # Normalizuj — RhoHV ma specjalny klucz
    if "RHOHV" in product_key:
        product_key = f"{radar.upper()}_RHOHV"

    if filepath is None:
        if product_key not in PRODUCTS:
            print(f"  ! Nieznany produkt: {product_key}")
            print(f"    Dostępne: {[k for k in PRODUCTS if k.startswith(radar.upper())]}")
            return

        print(f"\nPobieranie {product_key} ...")
        dt = find_latest(product_key)
        if dt is None:
            print("  ! Nie znaleziono pliku")
            return

        url        = make_url(product_key, dt)
        ts         = make_timestamp(dt)
        local_path = os.path.join(DATA_DIR, product_key, f"{ts}.h5")
        print(f"  URL: {url}")
        if not download_file(url, local_path):
            return
        filepath = local_path

    print(f"\nParsowanie VSP: {filepath}")
    vsp = parse_vsp(filepath)
    if vsp is None:
        return

    for section in ("VSP", "HSP"):
        if section in vsp:
            sd = vsp[section]
            d  = sd["data"]
            print(f"  {section}: shape={sd['shape']}  "
                  f"x=[{sd['x_km'].min():.0f}..{sd['x_km'].max():.0f}] km  "
                  f"z=[{sd['y_km'].min():.1f}..{sd['y_km'].max():.1f}] km  "
                  f"nan%={100*np.isnan(d).mean():.0f}%  "
                  f"min={np.nanmin(d):.3f}  max={np.nanmax(d):.3f}")

    save_path = filepath.replace(".h5", "_vsp.png") if save else None
    plot_vsp(vsp, radar_code=radar, show_hsp=True, save_path=save_path)


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Przekrój pionowy VSP z danych radarowych IMGW")
    parser.add_argument("--radar",    default="poz",
                        help="Kod radaru: leg, brz, gdy, gsa, pas, poz, ram, rze, swi, uzr")
    parser.add_argument("--quantity", default="CAPPI",
                        help="Ilość: KDP, ZDR, RhoHV")
    parser.add_argument("--file",     default=None,
                        help="Ścieżka do istniejącego pliku .h5 (pomija pobieranie)")
    parser.add_argument("--no-save",  action="store_true",
                        help="Nie zapisuj PNG")
    args = parser.parse_args()

    fetch_and_plot_vsp(
        radar    = args.radar,
        quantity = args.quantity,
        filepath = args.file,
        save     = not args.no_save,
    )


if __name__ == "__main__":
    main()
