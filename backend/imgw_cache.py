"""
Radar Cache Manager
--------------------
On-demand pobieranie danych radarowych z TTL = 2 minuty.

Zasada działania:
  - Przy każdym żądaniu sprawdza czy plik HDF5 istnieje i jest świeży (< TTL)
  - Jeśli świeży  → zwraca z dysku natychmiast
  - Jeśli nieaktualny lub brak → odpala ingestor, pobiera, zwraca
  - Brak schedulera, brak pobierania "na zapas"
  - Wątkowo bezpieczny — jeden produkt pobierany tylko raz naraz (lock per produkt)

Użycie:
    from imgw_cache import CacheManager

    cache = CacheManager()
    result = cache.get("COMPO_CMAX")
    if result:
        parsed  = result["parsed"]   # dict z data, shape, quantity, where, ...
        georef  = result["georef"]   # dict z lat_grid, lon_grid, ... (lub None)
        path    = result["path"]     # ścieżka do pliku HDF5
        fresh   = result["fresh"]    # True = z cache, False = właśnie pobrano
        age_s   = result["age_s"]    # wiek pliku w sekundach
"""

import os
import time
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path

from imgw_radar import (
    PRODUCTS, find_latest, make_url, make_timestamp,
    download_file, parse_hdf5, build_georef, DATA_DIR,
)

log = logging.getLogger("cache")


# ── Konfiguracja ──────────────────────────────────────────────────────────────

CACHE_TTL_S   = 120     # czas życia danych w sekundach (2 minuty)
CACHE_DIR     = DATA_DIR
MAX_RETRIES   = 2       # ile razy próbować pobrać jeśli się nie uda


# ── Cache Manager ─────────────────────────────────────────────────────────────

class CacheManager:
    """
    Zarządza lokalnym cache plików HDF5.

    Każdy produkt ma własny lock — równoległe żądania tego samego produktu
    czekają na siebie zamiast pobierać ten sam plik dwa razy.
    Różne produkty są pobierane równolegle bez blokowania.
    """

    def __init__(self, ttl_s: int = CACHE_TTL_S, cache_dir: str = CACHE_DIR):
        self.ttl_s     = ttl_s
        self.cache_dir = cache_dir
        self._locks: dict[str, threading.Lock] = {}
        self._locks_meta = threading.Lock()   # chroni słownik _locks
        log.info(f"CacheManager: TTL={ttl_s}s  dir={cache_dir}")

    def _get_lock(self, product_key: str) -> threading.Lock:
        """Zwraca (tworząc jeśli brak) lock dla danego produktu."""
        with self._locks_meta:
            if product_key not in self._locks:
                self._locks[product_key] = threading.Lock()
            return self._locks[product_key]

    # ── Główna metoda publiczna ───────────────────────────────────────────────

    def get(self, product_key: str) -> dict | None:
        """
        Zwraca dane dla produktu — z cache lub pobrane na żądanie.

        Zwracany słownik:
          parsed   — wynik parse_hdf5() (data, shape, quantity, where, what, ...)
          georef   — wynik build_georef() lub None
          path     — ścieżka do pliku HDF5
          age_s    — wiek pliku w sekundach
          fresh    — True = dane z cache, False = właśnie pobrano
          scan_dt  — datetime UTC ostatniego skanu

        Zwraca None jeśli pobieranie nieudane.
        """
        if product_key not in PRODUCTS:
            log.error(f"Nieznany produkt: {product_key}")
            return None

        lock = self._get_lock(product_key)
        with lock:
            return self._get_locked(product_key)

    def get_many(self, product_keys: list[str]) -> dict[str, dict | None]:
        """
        Pobiera wiele produktów równolegle.
        Zwraca {product_key: result_or_None}.
        """
        import concurrent.futures
        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
            futures = {ex.submit(self.get, k): k for k in product_keys}
            for future in concurrent.futures.as_completed(futures):
                key = futures[future]
                try:
                    results[key] = future.result()
                except Exception as e:
                    log.error(f"[{key}] get_many błąd: {e}")
                    results[key] = None
        return results

    # ── Logika wewnętrzna (pod lockiem) ──────────────────────────────────────

    def _get_locked(self, product_key: str) -> dict | None:
        """Wywoływana tylko pod lockiem produktu."""

        # Sprawdź czy mamy świeży plik na dysku
        cached = self._find_cached(product_key)
        if cached is not None:
            path, age_s = cached
            log.info(f"[{product_key}] cache hit  ({age_s:.0f}s temu)  {os.path.basename(path)}")
            return self._load(path, age_s=age_s, fresh=True)

        # Brak lub nieaktualny — pobierz
        log.info(f"[{product_key}] cache miss — pobieram ...")
        return self._fetch(product_key)

    def _find_cached(self, product_key: str) -> tuple[str, float] | None:
        """
        Szuka najnowszego pliku HDF5 w katalogu produktu.
        Zwraca (path, age_s) jeśli plik istnieje i jest świeży, None w p.p.
        """
        product_dir = os.path.join(self.cache_dir, product_key)
        if not os.path.isdir(product_dir):
            return None

        # Znajdź wszystkie pliki HDF5 posortowane od najnowszego
        files = sorted(
            [f for f in Path(product_dir).glob("*.h5")],
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if not files:
            return None

        newest = files[0]
        age_s  = time.time() - newest.stat().st_mtime

        if age_s <= self.ttl_s:
            return str(newest), age_s

        log.debug(f"[{product_key}] plik nieaktualny ({age_s:.0f}s > TTL {self.ttl_s}s)")
        return None

    def _fetch(self, product_key: str) -> dict | None:
        """Pobiera najnowszy plik z IMGW i ładuje do pamięci."""
        for attempt in range(1, MAX_RETRIES + 1):
            scan_dt = find_latest(product_key)
            if scan_dt is None:
                log.warning(f"[{product_key}] find_latest zwróciło None (próba {attempt})")
                if attempt < MAX_RETRIES:
                    time.sleep(3)
                continue

            ts         = make_timestamp(scan_dt)
            local_path = os.path.join(self.cache_dir, product_key, f"{ts}.h5")
            url        = make_url(product_key, scan_dt)

            ok = download_file(url, local_path)
            if not ok:
                log.warning(f"[{product_key}] pobieranie nieudane (próba {attempt})")
                if attempt < MAX_RETRIES:
                    time.sleep(3)
                continue

            age_s = time.time() - os.path.getmtime(local_path)
            log.info(f"[{product_key}] pobrano  {os.path.basename(local_path)}")
            return self._load(local_path, age_s=age_s, fresh=False, scan_dt=scan_dt)

        log.error(f"[{product_key}] wszystkie próby nieudane")
        return None

    def _load(self, path: str, age_s: float = 0,
              fresh: bool = True, scan_dt: datetime | None = None) -> dict | None:
        """Parsuje plik HDF5 i buduje georef."""
        parsed = parse_hdf5(path)
        if parsed is None:
            return None

        georef = build_georef(parsed["where"])

        # Wyciągnij scan_dt z metadanych ODIM jeśli nie podany
        if scan_dt is None:
            what  = parsed.get("what", {})
            date_ = what.get("date", b"")
            time_ = what.get("time", b"")
            if isinstance(date_, (bytes,)): date_ = date_.decode()
            if isinstance(time_, (bytes,)): time_ = time_.decode()
            if date_ and time_:
                try:
                    scan_dt = datetime.strptime(
                        date_ + time_[:6], "%Y%m%d%H%M%S"
                    ).replace(tzinfo=timezone.utc)
                except ValueError:
                    pass

        return {
            "parsed":  parsed,
            "georef":  georef,
            "path":    path,
            "age_s":   age_s,
            "fresh":   fresh,
            "scan_dt": scan_dt,
        }

    # ── Narzędzia pomocnicze ──────────────────────────────────────────────────

    def status(self, product_key: str) -> dict:
        """
        Zwraca status cache dla produktu bez pobierania.
        Przydatne do health-checka i logowania.
        """
        cached = self._find_cached(product_key)
        if cached:
            path, age_s = cached
            return {
                "product":   product_key,
                "status":    "fresh",
                "age_s":     age_s,
                "ttl_s":     self.ttl_s,
                "remaining": max(0, self.ttl_s - age_s),
                "path":      path,
            }
        product_dir = os.path.join(self.cache_dir, product_key)
        files = list(Path(product_dir).glob("*.h5")) if os.path.isdir(product_dir) else []
        return {
            "product":   product_key,
            "status":    "stale" if files else "empty",
            "age_s":     None,
            "ttl_s":     self.ttl_s,
            "remaining": None,
            "path":      None,
        }

    def status_all(self, product_keys: list[str] | None = None) -> list[dict]:
        """Status cache dla listy produktów (domyślnie wszystkich)."""
        keys = product_keys or list(PRODUCTS.keys())
        return [self.status(k) for k in keys]

    def invalidate(self, product_key: str):
        """
        Wymusza odświeżenie przy następnym get() przez usunięcie
        najnowszego pliku z cache.
        """
        cached = self._find_cached(product_key)
        if cached:
            path, _ = cached
            try:
                os.remove(path)
                log.info(f"[{product_key}] cache unieważniony: {os.path.basename(path)}")
            except Exception as e:
                log.warning(f"[{product_key}] nie można usunąć {path}: {e}")

    def cleanup(self, keep_last: int = 5):
        """
        Usuwa stare pliki HDF5 zostawiając keep_last najnowszych per produkt.
        Wywołuj ręcznie lub co jakiś czas z zewnątrz.
        """
        removed = 0
        for product_key in PRODUCTS:
            product_dir = os.path.join(self.cache_dir, product_key)
            if not os.path.isdir(product_dir):
                continue
            files = sorted(
                Path(product_dir).glob("*.h5"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            for old_file in files[keep_last:]:
                try:
                    old_file.unlink()
                    removed += 1
                except Exception:
                    pass
        if removed:
            log.info(f"cleanup: usunięto {removed} starych plików HDF5")
        return removed



    def history(self, product_key: str, limit: int = 5) -> list[dict]:
        """
        Zwraca listę dostępnych skanów dla produktu (z dysku), posortowanych
        od najnowszego. Każdy wpis zawiera scan_time, path, age_s.
        Limit domyślnie 5 — tyle ile trzymamy w cache.
        """
        product_dir = os.path.join(self.cache_dir, product_key)
        if not os.path.isdir(product_dir):
            return []

        files = sorted(
            Path(product_dir).glob("*.h5"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )[:limit]

        result = []
        for f in files:
            # Timestamp zakodowany w nazwie pliku: YYYYMMDDHHmmSS00.h5
            stem = f.stem  # np. "2026031408150000"
            scan_dt = None
            try:
                scan_dt = datetime.strptime(stem[:12], "%Y%m%d%H%M").replace(
                    tzinfo=timezone.utc)
            except ValueError:
                pass

            result.append({
                "scan_time": scan_dt.isoformat() if scan_dt else None,
                "timestamp": stem,
                "path":      str(f),
                "age_s":     round(time.time() - f.stat().st_mtime, 1),
                "size_kb":   round(f.stat().st_size / 1024, 0),
            })
        return result

    def get_by_scan_time(self, product_key: str,
                          scan_time: str) -> dict | None:
        """
        Zwraca dane dla konkretnego skanu po jego scan_time (ISO8601 lub
        timestamp YYYYMMDDHHmmSS00).
        Skan musi być dostępny lokalnie — nie pobiera z IMGW.
        """
        product_dir = os.path.join(self.cache_dir, product_key)
        if not os.path.isdir(product_dir):
            return None

        # Normalizuj: wyciągnij timestamp z ISO lub użyj bezpośrednio
        ts_search = None
        if "T" in scan_time or "-" in scan_time:
            # ISO format: 2026-03-14T08:15:00+00:00
            try:
                dt = datetime.fromisoformat(scan_time.replace("Z", "+00:00"))
                ts_search = dt.strftime("%Y%m%d%H%M")
            except ValueError:
                return None
        else:
            # Timestamp format: 2026031408150000
            ts_search = scan_time[:12]

        # Znajdź plik pasujący do timestampu
        for f in Path(product_dir).glob("*.h5"):
            if f.stem.startswith(ts_search):
                age_s = time.time() - f.stat().st_mtime
                return self._load(str(f), age_s=age_s, fresh=True)

        log.warning(f"[{product_key}] Skan {ts_search} niedostępny lokalnie")
        return None


# ── Singleton (opcjonalny) ────────────────────────────────────────────────────

_default_cache: CacheManager | None = None

def get_cache() -> CacheManager:
    """Zwraca globalną instancję CacheManager (lazy init)."""
    global _default_cache
    if _default_cache is None:
        _default_cache = CacheManager()
    return _default_cache


# ── Demo / test ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )

    cache = CacheManager(ttl_s=CACHE_TTL_S)

    print("\n── Test 1: pojedyncze żądanie COMPO_CMAX ────────────────")
    result = cache.get("COMPO_CMAX")
    if result:
        p = result["parsed"]
        import numpy as np
        d = p["data"]
        print(f"  quantity  = {p['quantity']}")
        print(f"  shape     = {p['shape']}")
        print(f"  min/max   = {np.nanmin(d):.1f} / {np.nanmax(d):.1f}")
        print(f"  fresh     = {result['fresh']}")
        print(f"  age_s     = {result['age_s']:.1f}s")
        print(f"  scan_dt   = {result['scan_dt']}")
        print(f"  georef    = {'tak' if result['georef'] else 'brak'}")

    print("\n── Test 2: to samo żądanie — powinno trafić w cache ────")
    result2 = cache.get("COMPO_CMAX")
    if result2:
        print(f"  fresh     = {result2['fresh']}  (oczekiwane: True)")
        print(f"  age_s     = {result2['age_s']:.1f}s")

    print("\n── Test 3: status cache ─────────────────────────────────")
    for s in cache.status_all(["COMPO_CMAX", "COMPO_EHT", "LEG_KDP"]):
        remaining = f"{s['remaining']:.0f}s" if s["remaining"] is not None else "—"
        age_str = f"{s['age_s']:.0f}s" if s["age_s"] is not None else "—"
        print(f"  {s['product']:<16} {s['status']:<8} wiek={age_str:>6}  pozostało={remaining}")

    print("\n── Test 4: równoległe pobieranie ────────────────────────")
    import time as _time
    t0 = _time.monotonic()
    results = cache.get_many(["COMPO_CMAX", "COMPO_EHT", "COMPO_SRI"])
    elapsed = _time.monotonic() - t0
    for k, r in results.items():
        ok = "ok" if r else "BRAK"
        fresh = r["fresh"] if r else "—"
        print(f"  {k:<16} {ok}  fresh={fresh}")
    print(f"  Łączny czas: {elapsed:.1f}s")
