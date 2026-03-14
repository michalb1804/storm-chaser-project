"""
GFS Cache Manager
-----------------
On-demand pobieranie danych GFS z inteligentnym TTL.

Różnice względem imgw_cache.py (radar):
  - GFS ma run co 6h (00z/06z/12z/18z), nie co 5 min
  - Dane są ważne do następnego runu (~6h) + bufor
  - Prognoza do +48h — każda godzina prognozy to osobny wpis cache
  - Parametry pochodne (gfs_derived) obliczane automatycznie po pobraniu
  - NetCDF jako format cache (xarray, szybki odczyt)

Użycie:
    from gfs_cache import GFSCacheManager, get_gfs_cache

    cache = get_gfs_cache()

    # Najnowsza analiza, jeden parametr
    result = cache.get("CAPE_SFC")

    # Prognoza +24h
    result = cache.get("CAPE_SFC", fxx=24)

    # Wiele parametrów naraz
    results = cache.get_many(["CAPE_SFC", "SRH_0_3", "SHEAR_0_6_derived"])

    # Pełny run z parametrami pochodnymi
    run = cache.get_run(fxx_list=[0, 6, 12, 24, 48])
"""

import os
import json
import time
import logging
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path

import numpy as np

log = logging.getLogger("gfs_cache")


# ── Konfiguracja ──────────────────────────────────────────────────────────────

# TTL zależy od horyzontu prognozy
# Analiza (f00): ważna do następnego runu GFS (~6h) minus bufor
# Prognozy: ważne dłużej bo nie zmieniają się między runami
TTL_BY_FXX = {
    0:   5 * 3600,    # analiza: 5h
    6:   6 * 3600,    # f06: 6h
    12:  6 * 3600,
    24:  6 * 3600,
    48:  6 * 3600,
}
TTL_DEFAULT = 6 * 3600   # 6h dla pozostałych

GFS_CACHE_DIR = "gfs_cache"
MAX_RUNS_KEPT = 4         # ile runów trzymać na dysku (4 × ~6h = 24h)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ttl_for_fxx(fxx: int) -> int:
    return TTL_BY_FXX.get(fxx, TTL_DEFAULT)


def _run_cache_path(run_dt: datetime, fxx: int) -> Path:
    """Ścieżka do pliku cache dla danego runu i godziny prognozy."""
    run_str = run_dt.strftime("%Y%m%d_%H")
    return Path(GFS_CACHE_DIR) / f"gfs_{run_str}z_f{fxx:03d}.json"


def _array_to_list(arr) -> list | None:
    """numpy array → lista (do JSON)."""
    if arr is None:
        return None
    if hasattr(arr, "tolist"):
        return arr.tolist()
    return arr


def _list_to_array(lst) -> np.ndarray | None:
    """lista → numpy array."""
    if lst is None:
        return None
    return np.array(lst, dtype=np.float32)


# ── Cache entry ───────────────────────────────────────────────────────────────

class GFSRunCache:
    """
    Cache jednego runu GFS (jeden run_dt + fxx).
    Przechowuje wszystkie parametry (bezpośrednie + pochodne) w jednym pliku JSON.
    Lekki format — tablice float32, bbox już wycięty.
    """

    def __init__(self, path: Path):
        self.path = path
        self._data: dict | None = None
        self._mtime: float = 0

    def exists(self) -> bool:
        return self.path.exists()

    def age_s(self) -> float | None:
        if not self.path.exists():
            return None
        return time.time() - self.path.stat().st_mtime

    def is_fresh(self, fxx: int) -> bool:
        age = self.age_s()
        if age is None:
            return False
        return age <= _ttl_for_fxx(fxx)

    def load(self) -> dict | None:
        """Wczytuje cache z dysku. Zwraca None jeśli plik nie istnieje."""
        if not self.path.exists():
            return None
        try:
            mtime = self.path.stat().st_mtime
            if self._data is not None and mtime == self._mtime:
                return self._data   # już w pamięci, nic się nie zmieniło

            with open(self.path, encoding="utf-8") as f:
                raw = json.load(f)

            # Deserializuj tablice
            params = {}
            for key, entry in raw.get("params", {}).items():
                if entry is None:
                    params[key] = None
                    continue
                params[key] = {
                    **entry,
                    "data": _list_to_array(entry.get("data")),
                    "lats": _list_to_array(entry.get("lats")),
                    "lons": _list_to_array(entry.get("lons")),
                }

            self._data = {
                "run_time":   raw.get("run_time"),
                "fxx":        raw.get("fxx"),
                "valid_time": raw.get("valid_time"),
                "saved_at":   raw.get("saved_at"),
                "params":     params,
            }
            self._mtime = mtime
            return self._data

        except Exception as e:
            log.warning(f"Błąd wczytywania cache {self.path}: {e}")
            return None

    def save(self, run_time: datetime, fxx: int,
             params: dict[str, dict | None]) -> bool:
        """Zapisuje wyniki do pliku JSON."""
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)

            def _json_safe(v):
                """Konwertuje wartości do typów JSON-serializable."""
                if isinstance(v, np.ndarray):
                    return _array_to_list(v)
                if isinstance(v, (datetime,)):
                    return v.isoformat()
                if hasattr(v, 'isoformat'):   # pandas Timestamp, numpy datetime64 itd.
                    return str(v)
                if isinstance(v, (np.integer,)):
                    return int(v)
                if isinstance(v, (np.floating,)):
                    return float(v)
                if isinstance(v, (np.bool_,)):
                    return bool(v)
                return v

            # Serializuj tablice numpy → listy, datetime → str
            serialized = {}
            for key, entry in params.items():
                if entry is None:
                    serialized[key] = None
                    continue
                serialized[key] = {
                    k: _json_safe(v)
                    for k, v in entry.items()
                }

            payload = {
                "run_time":   run_time.strftime("%Y-%m-%dT%H:%M") if run_time else None,
                "fxx":        fxx,
                "valid_time": (run_time + timedelta(hours=fxx)).strftime("%Y-%m-%dT%H:%M")
                               if run_time else None,
                "saved_at":   datetime.now(timezone.utc).isoformat(),
                "params":     serialized,
            }

            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(payload, f, allow_nan=True)

            self._data  = None   # invalidate in-memory cache
            self._mtime = 0
            log.info(f"Zapisano cache: {self.path.name}  "
                     f"({self.path.stat().st_size / 1024:.0f} KB)")
            return True

        except Exception as e:
            log.error(f"Błąd zapisu cache {self.path}: {e}")
            return False


# ── GFS Cache Manager ─────────────────────────────────────────────────────────

class GFSCacheManager:
    """
    Zarządza cache danych GFS.

    Każdy run × fxx ma własny plik JSON z wszystkimi parametrami.
    Wątkowo bezpieczny — lock per (run_dt, fxx).
    Parametry pochodne obliczane automatycznie przy pobieraniu.
    """

    def __init__(self, cache_dir: str = GFS_CACHE_DIR,
                 compute_derived: bool = True):
        self.cache_dir       = cache_dir
        self.compute_derived = compute_derived
        self._locks: dict[str, threading.Lock] = {}
        self._locks_meta     = threading.Lock()
        log.info(f"GFSCacheManager: dir={cache_dir} derived={compute_derived}")

    def _lock_key(self, run_dt: datetime, fxx: int) -> str:
        return f"{run_dt.strftime('%Y%m%d_%H')}_{fxx:03d}"

    def _get_lock(self, run_dt: datetime, fxx: int) -> threading.Lock:
        key = self._lock_key(run_dt, fxx)
        with self._locks_meta:
            if key not in self._locks:
                self._locks[key] = threading.Lock()
            return self._locks[key]

    # ── Pobieranie jednego parametru ─────────────────────────────────────────

    def get(self, param_key: str, fxx: int = 0,
            run_dt: datetime | None = None) -> dict | None:
        """
        Zwraca jeden parametr (bezpośredni lub pochodny) dla podanego fxx.
        Pobiera cały run jeśli cache nieaktualny.

        Parametry pochodne mają suffix '_derived' w kluczu
        (np. 'SHEAR_0_6_derived', 'SCP_derived').
        """
        run_dt = run_dt or self._latest_run()
        lock   = self._get_lock(run_dt, fxx)

        with lock:
            cache = GFSRunCache(_run_cache_path(run_dt, fxx))

            if cache.is_fresh(fxx):
                data = cache.load()
                if data:
                    result = data["params"].get(param_key)
                    if result is not None:
                        log.debug(f"cache hit: {param_key} fxx={fxx}")
                        return result

            # Cache miss lub parametr niedostępny — pobierz cały run
            all_params = self._fetch_run(run_dt, fxx, cache)
            if all_params is None:
                return None
            return all_params.get(param_key)

    def get_many(self, param_keys: list[str], fxx: int = 0,
                 run_dt: datetime | None = None) -> dict[str, dict | None]:
        """
        Pobiera wiele parametrów dla tego samego fxx.
        Jeden request do IMGW/GFS zamiast N.
        """
        run_dt = run_dt or self._latest_run()
        lock   = self._get_lock(run_dt, fxx)

        with lock:
            cache = GFSRunCache(_run_cache_path(run_dt, fxx))

            if cache.is_fresh(fxx):
                data = cache.load()
                if data:
                    results = {k: data["params"].get(k) for k in param_keys}
                    # Jeśli wszystkie są w cache — zwróć od razu
                    if all(v is not None for v in results.values()):
                        log.debug(f"cache hit: {len(param_keys)} params fxx={fxx}")
                        return results

            # Pobierz cały run
            all_params = self._fetch_run(run_dt, fxx, cache)
            if all_params is None:
                return {k: None for k in param_keys}
            return {k: all_params.get(k) for k in param_keys}

    def get_run(self, fxx_list: list[int] | None = None,
                param_keys: list[str] | None = None,
                run_dt: datetime | None = None) -> dict[int, dict[str, dict | None]]:
        """
        Pobiera dane dla wielu godzin prognozy równolegle.
        Zwraca {fxx: {param_key: result}}.
        """
        import concurrent.futures

        run_dt   = run_dt or self._latest_run()
        fxx_list = fxx_list or [0, 6, 12, 24, 48]

        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
            futures = {}
            for fxx in fxx_list:
                if param_keys:
                    futures[ex.submit(self.get_many, param_keys, fxx, run_dt)] = fxx
                else:
                    futures[ex.submit(self._get_all_fxx, fxx, run_dt)] = fxx

            for future in concurrent.futures.as_completed(futures):
                fxx = futures[future]
                try:
                    results[fxx] = future.result()
                except Exception as e:
                    log.error(f"get_run fxx={fxx}: {e}")
                    results[fxx] = {}

        return results

    def _get_all_fxx(self, fxx: int, run_dt: datetime) -> dict:
        """Zwraca wszystkie parametry dla jednego fxx."""
        run_dt = run_dt or self._latest_run()
        lock   = self._get_lock(run_dt, fxx)
        with lock:
            cache = GFSRunCache(_run_cache_path(run_dt, fxx))
            if cache.is_fresh(fxx):
                data = cache.load()
                if data:
                    return data["params"]
            result = self._fetch_run(run_dt, fxx, cache)
            return result or {}

    # ── Pobieranie z GFS ─────────────────────────────────────────────────────

    def _fetch_run(self, run_dt: datetime, fxx: int,
                   cache: GFSRunCache) -> dict | None:
        """Pobiera dane z GFS i oblicza parametry pochodne. Zapisuje do cache."""
        from gfs_ingestor import fetch_all, PARAM_GROUPS

        log.info(f"GFS fetch: run={run_dt.strftime('%Y-%m-%d %Hz')} fxx={fxx:03d}")

        # Pobierz wszystkie parametry z grupy nowcast
        gfs_results = fetch_all(
            run_dt    = run_dt,
            fxx       = fxx,
            param_keys= PARAM_GROUPS["nowcast"],
        )

        ok  = sum(1 for r in gfs_results.values() if r is not None)
        err = len(gfs_results) - ok
        log.info(f"  Pobrano: ok={ok} err={err}")

        if ok == 0:
            log.error(f"  Brak danych GFS dla run={run_dt} fxx={fxx}")
            return None

        # Oblicz parametry pochodne
        all_params = dict(gfs_results)
        if self.compute_derived:
            from gfs_derived import compute_all
            derived = compute_all(gfs_results)
            d_ok  = sum(1 for r in derived.values() if r is not None)
            d_err = len(derived) - d_ok
            log.info(f"  Pochodne: ok={d_ok} err={d_err}")
            all_params.update(derived)

        # Zapisz do cache
        cache.save(run_dt, fxx, all_params)

        return all_params

    # ── Run time ─────────────────────────────────────────────────────────────

    def _latest_run(self) -> datetime:
        """Zwraca czas ostatniego dostępnego runu GFS (naive datetime)."""
        from gfs_ingestor import get_latest_run_time
        return get_latest_run_time()

    def current_run(self) -> datetime:
        """Publiczny dostęp do czasu bieżącego runu."""
        return self._latest_run()

    # ── Status i zarządzanie ──────────────────────────────────────────────────

    def status(self, fxx: int = 0, run_dt: datetime | None = None) -> dict:
        """Status cache dla danego fxx."""
        run_dt = run_dt or self._latest_run()
        cache  = GFSRunCache(_run_cache_path(run_dt, fxx))
        age    = cache.age_s()
        ttl    = _ttl_for_fxx(fxx)
        fresh  = cache.is_fresh(fxx)

        return {
            "run_time":   run_dt.strftime("%Y-%m-%d %Hz"),
            "fxx":        fxx,
            "status":     "fresh" if fresh else ("stale" if cache.exists() else "empty"),
            "age_s":      round(age, 1) if age is not None else None,
            "ttl_s":      ttl,
            "remaining_s": max(0, round(ttl - age, 1)) if age is not None else None,
            "path":       str(cache.path),
        }

    def status_all(self, fxx_list: list[int] | None = None,
                   run_dt: datetime | None = None) -> list[dict]:
        """Status cache dla listy godzin prognozy."""
        run_dt   = run_dt or self._latest_run()
        fxx_list = fxx_list or [0, 6, 12, 24, 48]
        return [self.status(fxx, run_dt) for fxx in fxx_list]

    def invalidate(self, fxx: int = 0, run_dt: datetime | None = None):
        """Unieważnia cache dla danego fxx."""
        run_dt = run_dt or self._latest_run()
        path   = _run_cache_path(run_dt, fxx)
        if path.exists():
            path.unlink()
            log.info(f"Cache unieważniony: {path.name}")

    def cleanup(self, keep_runs: int = MAX_RUNS_KEPT):
        """Usuwa stare pliki cache zostawiając keep_runs najnowszych runów."""
        cache_dir = Path(self.cache_dir)
        if not cache_dir.exists():
            return 0

        files = sorted(cache_dir.glob("gfs_*.json"),
                       key=lambda p: p.stat().st_mtime, reverse=True)

        # Grupuj po run_time (pierwsze 15 znaków nazwy: gfs_YYYYMMDD_HHz)
        seen_runs = set()
        to_delete = []
        for f in files:
            run_id = "_".join(f.stem.split("_")[:3])   # gfs_YYYYMMDD_HHz
            if run_id not in seen_runs:
                seen_runs.add(run_id)
            if len(seen_runs) > keep_runs:
                to_delete.append(f)

        for f in to_delete:
            f.unlink()
        if to_delete:
            log.info(f"Cleanup: usunięto {len(to_delete)} plików GFS cache")
        return len(to_delete)

    def list_cached_runs(self) -> list[dict]:
        """Lista wszystkich dostępnych runów w cache."""
        cache_dir = Path(self.cache_dir)
        if not cache_dir.exists():
            return []

        runs = {}
        for f in sorted(cache_dir.glob("gfs_*.json")):
            parts = f.stem.split("_")   # ['gfs', 'YYYYMMDD', 'HHz', 'fNNN']
            if len(parts) < 4:
                continue
            run_id = f"{parts[1]}_{parts[2]}"
            fxx    = int(parts[3][1:]) if parts[3].startswith("f") else -1
            age    = time.time() - f.stat().st_mtime

            if run_id not in runs:
                runs[run_id] = {"run_id": run_id, "fxx_cached": [], "files": 0}
            runs[run_id]["fxx_cached"].append(fxx)
            runs[run_id]["files"] += 1
            runs[run_id]["age_h"] = round(age / 3600, 1)

        return sorted(runs.values(), key=lambda r: r["run_id"], reverse=True)


# ── Singleton ─────────────────────────────────────────────────────────────────

_default_cache: GFSCacheManager | None = None


def get_gfs_cache(compute_derived: bool = True) -> GFSCacheManager:
    """Zwraca globalną instancję GFSCacheManager (lazy init)."""
    global _default_cache
    if _default_cache is None:
        _default_cache = GFSCacheManager(compute_derived=compute_derived)
    return _default_cache


# ── Demo ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )

    cache = GFSCacheManager()

    print("\n── Test 1: jeden parametr f000 ──────────────────────────")
    r = cache.get("CAPE_SFC", fxx=0)
    if r:
        print(f"  CAPE_SFC: min={r['val_min']:.1f}  max={r['val_max']:.1f}  {r['units']}")

    print("\n── Test 2: to samo — powinno trafić w cache ─────────────")
    import time as _time
    t0 = _time.monotonic()
    r2 = cache.get("CAPE_SFC", fxx=0)
    print(f"  Czas: {(_time.monotonic()-t0)*1000:.0f} ms  (oczekiwane: <10 ms)")

    print("\n── Test 3: parametr pochodny ────────────────────────────")
    r3 = cache.get("SHEAR_0_6_derived", fxx=0)
    if r3:
        print(f"  Shear0-6: min={r3['val_min']:.1f}  max={r3['val_max']:.1f}  {r3['units']}")

    print("\n── Test 4: wiele parametrów naraz ───────────────────────")
    params = ["CAPE_SFC", "SRH_0_3", "SHEAR_0_6_derived",
              "K_INDEX_derived", "SCP_derived"]
    results = cache.get_many(params, fxx=0)
    for k, r in results.items():
        if r:
            print(f"  {k:<25} min={r['val_min']:>8.2f}  max={r['val_max']:>8.2f}  {r['units']}")
        else:
            print(f"  {k:<25} BRAK")

    print("\n── Test 5: status cache ─────────────────────────────────")
    for s in cache.status_all([0, 6, 24]):
        rem = f"{s['remaining_s']:.0f}s" if s['remaining_s'] is not None else "—"
        age_str = f"{s['age_s']:.0f}s" if s['age_s'] is not None else "—"
        print(f"  f{s['fxx']:03d}  {s['status']:<8}  wiek={age_str:>7}  pozostało={rem}")

    print("\n── Test 6: lista zbuforowanych runów ────────────────────")
    for run in cache.list_cached_runs():
        print(f"  {run['run_id']}  fxx={sorted(run['fxx_cached'])}  "
              f"wiek={run['age_h']}h  pliki={run['files']}")
