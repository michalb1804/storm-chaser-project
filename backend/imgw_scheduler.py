"""
IMGW Radar Scheduler
--------------------
Automatyczny zapis danych radarowych co ~5 minut do bazy SQLite.

Uruchom:    python imgw_scheduler.py
Zatrzymaj:  Ctrl+C

Wymagania:
    pip install h5py numpy requests pyproj apscheduler
    (matplotlib i cartopy opcjonalnie — tylko do podgladu)

Struktura bazy (radar.db):
    scans              — metadane każdego pobranego skanu
    point_observations — wartości radarowe nad zdefiniowanymi punktami
    download_log       — log prób pobierania (sukces/błąd)
"""

import os
import time
import logging
import sqlite3
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np

# Importuj moduł ingestora
from imgw_radar import (
    PRODUCTS, find_latest, make_url, make_timestamp,
    download_file, parse_hdf5, build_georef, query_point,
    demo_locations, DATA_DIR,
)

# ── Konfiguracja ──────────────────────────────────────────────────────────────

DB_PATH          = "radar.db"
LOG_PATH         = "scheduler.log"
POLL_INTERVAL_S  = 60        # sprawdzaj co 60 sekund
MIN_RESCAN_MIN   = 4         # nie pobieraj ponownie jeśli ostatni skan < 4 min temu
MAX_AGE_DAYS     = 7         # usuń pliki HDF5 starsze niż 7 dni (metadane zostają)

# Które produkty zbierać (klucze z PRODUCTS w imgw_radar.py)
WATCH_PRODUCTS = [
    "COMPO_CMAX",
    "COMPO_CAPPI",
    "COMPO_EHT",
    "COMPO_SRI",
    "COMPO_DPSRI",
    "LEG_PPI",
    "LEG_CAPPI",
    "LEG_DPSRI",
    "LEG_EHT",
    "LEG_SRI",
    "LEG_KDP",
    "LEG_ZDR",
    "LEG_RHOHV",
]

# Maks. równoległych pobierań naraz
MAX_WORKERS = 4

# Punkty do obserwacji (dodaj własne)
WATCH_POINTS = {
    **demo_locations(),
    # "MojaMiejscowosc": (51.1234, 20.5678),
}


# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
    ],
)
log = logging.getLogger("scheduler")


# ── Baza danych ───────────────────────────────────────────────────────────────

@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")   # bezpieczny zapis współbieżny
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with get_db() as db:
        db.executescript("""
            CREATE TABLE IF NOT EXISTS scans (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                product     TEXT    NOT NULL,
                scan_time   TEXT    NOT NULL,   -- ISO8601 UTC
                file_path   TEXT,               -- ścieżka do HDF5 (NULL = usunięty)
                file_size_kb REAL,
                quantity    TEXT,
                val_min     REAL,
                val_max     REAL,
                val_mean    REAL,
                nan_pct     REAL,
                ingested_at TEXT    NOT NULL,
                UNIQUE(product, scan_time)
            );

            CREATE TABLE IF NOT EXISTS point_observations (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_id   INTEGER NOT NULL REFERENCES scans(id),
                point     TEXT    NOT NULL,
                lat       REAL    NOT NULL,
                lon       REAL    NOT NULL,
                value     REAL,               -- NULL = brak sygnalu (NaN)
                row_px    INTEGER,
                col_px    INTEGER
            );

            CREATE TABLE IF NOT EXISTS download_log (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                product    TEXT NOT NULL,
                attempted  TEXT NOT NULL,    -- ISO8601 UTC
                scan_time  TEXT,             -- czas skanu (jeśli znaleziony)
                status     TEXT NOT NULL,    -- ok | skip | error
                message    TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_scans_product_time
                ON scans(product, scan_time);
            CREATE INDEX IF NOT EXISTS idx_point_obs_scan
                ON point_observations(scan_id);
            CREATE INDEX IF NOT EXISTS idx_point_obs_point
                ON point_observations(point, scan_id);
        """)
    log.info(f"Baza zainicjowana: {DB_PATH}")


def last_scan_time(product: str) -> datetime | None:
    """Zwraca czas ostatniego zapisanego skanu dla produktu."""
    with get_db() as db:
        row = db.execute(
            "SELECT scan_time FROM scans WHERE product=? ORDER BY scan_time DESC LIMIT 1",
            (product,)
        ).fetchone()
    if row:
        return datetime.fromisoformat(row["scan_time"])
    return None


def insert_scan(product: str, scan_dt: datetime, parsed: dict,
                local_path: str) -> int | None:
    """Zapisuje metadane skanu i wartości punktowe. Zwraca scan_id."""
    d        = parsed["data"]
    now_utc  = datetime.now(timezone.utc).isoformat()
    scan_iso = scan_dt.isoformat()

    try:
        with get_db() as db:
            cur = db.execute("""
                INSERT OR IGNORE INTO scans
                    (product, scan_time, file_path, file_size_kb, quantity,
                     val_min, val_max, val_mean, nan_pct, ingested_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (
                product, scan_iso,
                str(local_path),
                os.path.getsize(local_path) / 1024 if os.path.exists(local_path) else None,
                parsed["quantity"],
                float(np.nanmin(d)),
                float(np.nanmax(d)),
                float(np.nanmean(d)),
                float(100 * np.isnan(d).mean()),
                now_utc,
            ))
            scan_id = cur.lastrowid

            if scan_id == 0:
                # IGNORE zadziałał — skan już istnieje, pobierz id
                row = db.execute(
                    "SELECT id FROM scans WHERE product=? AND scan_time=?",
                    (product, scan_iso)
                ).fetchone()
                return row["id"] if row else None

            # Wartości punktowe
            georef = build_georef(parsed["where"])
            if georef is not None:
                rows = []
                for nazwa, (lat, lon) in WATCH_POINTS.items():
                    val = query_point(parsed, georef, lat, lon)
                    from imgw_radar import latlon_to_pixel
                    rc  = latlon_to_pixel(georef, lat, lon)
                    rows.append((
                        scan_id, nazwa, lat, lon,
                        val,
                        rc[0] if rc else None,
                        rc[1] if rc else None,
                    ))
                db.executemany("""
                    INSERT INTO point_observations
                        (scan_id, point, lat, lon, value, row_px, col_px)
                    VALUES (?,?,?,?,?,?,?)
                """, rows)

        return scan_id

    except Exception as e:
        log.error(f"  Błąd zapisu do bazy: {e}")
        return None


def log_download(product: str, status: str,
                 scan_dt: datetime | None = None, message: str = ""):
    with get_db() as db:
        db.execute("""
            INSERT INTO download_log (product, attempted, scan_time, status, message)
            VALUES (?,?,?,?,?)
        """, (
            product,
            datetime.now(timezone.utc).isoformat(),
            scan_dt.isoformat() if scan_dt else None,
            status,
            message,
        ))


# ── Czyszczenie starych plików HDF5 ──────────────────────────────────────────

def cleanup_old_files(max_age_days: int = MAX_AGE_DAYS):
    """
    Usuwa pliki HDF5 starsze niż max_age_days.
    Metadane w bazie zostają — tylko plik_path → NULL.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
    cutoff_iso = cutoff.isoformat()

    with get_db() as db:
        rows = db.execute(
            "SELECT id, file_path FROM scans WHERE scan_time < ? AND file_path IS NOT NULL",
            (cutoff_iso,)
        ).fetchall()

    removed = 0
    for row in rows:
        path = row["file_path"]
        if path and os.path.exists(path):
            try:
                os.remove(path)
                removed += 1
            except Exception as e:
                log.warning(f"  Nie mogę usunąć {path}: {e}")
        with get_db() as db:
            db.execute("UPDATE scans SET file_path=NULL WHERE id=?", (row["id"],))

    if removed:
        log.info(f"Cleanup: usunięto {removed} plików HDF5 starszych niż {max_age_days} dni")


# ── Jeden cykl pobierania ─────────────────────────────────────────────────────

def run_product(product_key: str):
    """Sprawdź czy jest nowy skan i jeśli tak — pobierz, sparsuj, zapisz."""
    log.info(f"[{product_key}] Sprawdzam ...")

    # Czy ostatni skan jest wystarczająco stary żeby szukać nowego?
    last = last_scan_time(product_key)
    if last is not None:
        age_min = (datetime.now(timezone.utc) - last).total_seconds() / 60
        if age_min < MIN_RESCAN_MIN:
            log.info(f"[{product_key}] Ostatni skan {age_min:.1f} min temu — pomijam")
            log_download(product_key, "skip",
                         message=f"ostatni skan {age_min:.1f} min temu")
            return

    # Znajdź najnowszy dostępny plik
    scan_dt = find_latest(product_key)
    if scan_dt is None:
        log.warning(f"[{product_key}] Nie znaleziono pliku")
        log_download(product_key, "error", message="find_latest zwróciło None")
        return

    # Czy to nowy skan (nie mamy go jeszcze w bazie)?
    scan_iso = scan_dt.isoformat()
    with get_db() as db:
        exists = db.execute(
            "SELECT 1 FROM scans WHERE product=? AND scan_time=?",
            (product_key, scan_iso)
        ).fetchone()
    if exists:
        log.info(f"[{product_key}] Skan {scan_dt.strftime('%H:%M')} już w bazie — pomijam")
        log_download(product_key, "skip", scan_dt, "już w bazie")
        return

    # Pobierz plik
    url        = make_url(product_key, scan_dt)
    ts         = make_timestamp(scan_dt)
    local_path = os.path.join(DATA_DIR, product_key, f"{ts}.h5")

    ok = download_file(url, local_path)
    if not ok:
        log.error(f"[{product_key}] Pobieranie nieudane: {url}")
        log_download(product_key, "error", scan_dt, f"pobieranie nieudane: {url}")
        return

    # Sparsuj i zapisz
    parsed = parse_hdf5(local_path)
    if parsed is None:
        log.error(f"[{product_key}] Parsowanie nieudane: {local_path}")
        log_download(product_key, "error", scan_dt, "parsowanie nieudane")
        return

    d = parsed["data"]
    scan_id = insert_scan(product_key, scan_dt, parsed, local_path)

    log.info(
        f"[{product_key}] "
        f"✓ {scan_dt.strftime('%H:%M UTC')}  "
        f"min={np.nanmin(d):.1f}  max={np.nanmax(d):.1f}  "
        f"nan={100*np.isnan(d).mean():.0f}%  "
        f"scan_id={scan_id}"
    )
    log_download(product_key, "ok", scan_dt)


# ── Statystyki z bazy ─────────────────────────────────────────────────────────

def print_stats():
    with get_db() as db:
        print("\n" + "="*60)
        print("  STATYSTYKI BAZY")
        print("="*60)

        for product in WATCH_PRODUCTS:
            row = db.execute("""
                SELECT COUNT(*) as cnt,
                       MIN(scan_time) as first,
                       MAX(scan_time) as last,
                       AVG(val_max)   as avg_max,
                       AVG(nan_pct)   as avg_nan
                FROM scans WHERE product=?
            """, (product,)).fetchone()
            print(f"\n  {product}:")
            print(f"    Skanów:      {row['cnt']}")
            if row['cnt'] > 0:
                print(f"    Pierwszy:    {row['first'][:16]}")
                print(f"    Ostatni:     {row['last'][:16]}")
                avg_max = f"{row['avg_max']:.1f}" if row['avg_max'] is not None else "n/d"
                avg_nan = f"{row['avg_nan']:.1f}%" if row['avg_nan'] is not None else "n/d"
                print(f"    Avg max:     {avg_max}")
                print(f"    Avg NaN%:    {avg_nan}")

        # Ostatnie obserwacje punktowe dla COMPO_CMAX
        print(f"\n  Ostatnie wartości punktowe (COMPO_CMAX):")
        rows = db.execute("""
            SELECT po.point, po.value, s.scan_time
            FROM point_observations po
            JOIN scans s ON po.scan_id = s.id
            WHERE s.product = 'COMPO_CMAX'
              AND s.scan_time = (
                  SELECT MAX(scan_time) FROM scans WHERE product='COMPO_CMAX'
              )
            ORDER BY po.point
        """).fetchall()
        for r in rows:
            val = f"{r['value']:.1f} dBZ" if r['value'] is not None else "brak sygnału"
            print(f"    {r['point']:<14} {val}")

        print()


# ── Pętla główna ──────────────────────────────────────────────────────────────

# ── Równoległe pobieranie ─────────────────────────────────────────────────────

def run_products_parallel(products: list[str]) -> dict[str, str]:
    """
    Pobiera wszystkie produkty równolegle przez ThreadPoolExecutor.
    Zwraca słownik {product_key: status} gdzie status to "ok"/"skip"/"error".
    SQLite w trybie WAL obsługuje równoczesne zapisy bezpiecznie.
    """
    results = {}

    def _run(product):
        try:
            run_product(product)
            with get_db() as db:
                row = db.execute(
                    "SELECT status FROM download_log WHERE product=? "
                    "ORDER BY id DESC LIMIT 1",
                    (product,)
                ).fetchone()
            return product, row["status"] if row else "ok"
        except Exception as e:
            log.error(f"[{product}] Nieoczekiwany błąd: {e}")
            log.debug(traceback.format_exc())
            return product, "error"

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_run, p): p for p in products}
        for future in as_completed(futures):
            product, status = future.result()
            results[product] = status

    return results


def main():
    log.info("="*50)
    log.info("IMGW Radar Scheduler uruchomiony")
    log.info(f"Produkty: {WATCH_PRODUCTS}")
    log.info(f"Interwał odpytywania: {POLL_INTERVAL_S}s")
    log.info(f"Baza: {DB_PATH}")
    log.info("="*50)

    init_db()

    cycle = 0
    try:
        while True:
            cycle += 1
            log.info(f"── Cykl #{cycle} ({datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}) ──")

            t0 = time.monotonic()
            results = run_products_parallel(WATCH_PRODUCTS)
            elapsed = time.monotonic() - t0

            ok    = sum(1 for r in results.values() if r == "ok")
            skip  = sum(1 for r in results.values() if r == "skip")
            err   = sum(1 for r in results.values() if r == "error")
            log.info(f"Cykl zakończony: ok={ok} skip={skip} err={err} t={elapsed:.1f}s")

            # Co 10 cykli — cleanup starych plików
            if cycle % 10 == 0:
                cleanup_old_files()

            # Co 20 cykli — wyświetl statystyki
            if cycle % 20 == 0:
                print_stats()

            log.info(f"Czekam {POLL_INTERVAL_S}s ...")
            time.sleep(POLL_INTERVAL_S)

    except KeyboardInterrupt:
        log.info("Zatrzymano przez użytkownika.")
        print_stats()


if __name__ == "__main__":
    main()
