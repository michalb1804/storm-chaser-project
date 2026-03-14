"""
Radar Cache Manager
--------------------
On-demand pobieranie danych radarowych z automatyczną historią 5 skanów.

Zasada działania:
  - get() sprawdza czy najnowszy plik jest świeży (< TTL=120s)
  - Cache miss → pobiera najnowszy skan synchronicznie, zwraca go od razu
  - Jednocześnie odpala wątek tła który pobiera 4 poprzednie skany
  - Na dysku trzymamy max HISTORY_SIZE=5 plików per produkt (auto-cleanup)
  - history() zwraca co jest na dysku — od razu po pierwszym get()
    masz 1 skan, po ~30s masz 5

  Bez schedulera. Bez bazy. Czysto on-demand.

Użycie:
    from imgw_cache import get_cache

    cache = get_cache()
    result = cache.get("COMPO_CMAX")   # blokuje ~2-5s przy pierwszym pobraniu
    scans  = cache.history("COMPO_CMAX")   # lista do 5 skanów
"""

import os
import time
import logging
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path

from imgw_radar import (
    PRODUCTS, find_latest, make_url, make_timestamp,
    download_file, parse_hdf5, build_georef, DATA_DIR,
    COMPO_PRODUCTS, INDIVIDUAL_SECONDS, is_valid_hdf5,
)

log = logging.getLogger("cache")


# ── Konfiguracja ──────────────────────────────────────────────────────────────

CACHE_TTL_S   = 120          # świeżość najnowszego skanu [s]
CACHE_DIR     = DATA_DIR
HISTORY_SIZE  = 5            # ile skanów trzymamy na dysku per produkt
HISTORY_STEP  = 5            # co ile minut cofamy się szukając starszych skanów
HISTORY_MAX_LOOKBACK = (HISTORY_SIZE - 1) * HISTORY_STEP + 2  # = 22 min — dokładnie tyle ile potrzeba
MAX_RETRIES   = 2


# ── Cache Manager ─────────────────────────────────────────────────────────────

class CacheManager:
    """
    Cache radarowy on-demand z automatyczną historią.

    Wątki:
      Główny wątek   → get() blokuje tylko na pobraniu najnowszego skanu
      Wątek tła      → _backfill() pobiera starsze skany asynchronicznie
    """

    # Pliki starsze niż STALE_AGE_S są czyszczone przy starcie serwera
    STALE_AGE_S = HISTORY_MAX_LOOKBACK * 60 + 60   # 22 min + 1 min bufor

    def __init__(self, ttl_s: int = CACHE_TTL_S, cache_dir: str = CACHE_DIR):
        self.ttl_s     = ttl_s
        self.cache_dir = cache_dir
        self._locks:     dict[str, threading.Lock] = {}
        self._locks_meta = threading.Lock()
        self._backfill_active: set[str] = set()
        self._backfill_lock   = threading.Lock()
        log.info(f"CacheManager: TTL={ttl_s}s  dir={cache_dir}  history={HISTORY_SIZE}")
        self._flush_stale_all()

    def _flush_stale_all(self):
        """
        Przy starcie usuwa pliki HDF5 starsze niż STALE_AGE_S.
        Zapobiega mieszaniu skanów z poprzednich sesji/dni w suwaku historii.
        """
        if not os.path.isdir(self.cache_dir):
            return
        removed = 0
        cutoff  = time.time() - self.STALE_AGE_S
        for product_dir in Path(self.cache_dir).iterdir():
            if not product_dir.is_dir():
                continue
            for f in product_dir.glob("*.h5"):
                if f.stat().st_mtime < cutoff:
                    try:
                        f.unlink()
                        removed += 1
                    except Exception:
                        pass
        if removed:
            log.info(f"flush stale: usunięto {removed} plików starszych niż {self.STALE_AGE_S}s")

    # ── Locki per produkt ─────────────────────────────────────────────────────

    def _get_lock(self, product_key: str) -> threading.Lock:
        with self._locks_meta:
            if product_key not in self._locks:
                self._locks[product_key] = threading.Lock()
            return self._locks[product_key]

    # ── Główna metoda publiczna ───────────────────────────────────────────────

    def get(self, product_key: str) -> dict | None:
        """
        Zwraca najnowszy skan — z cache (natychmiast) lub pobierając z IMGW.
        Po pobraniu nowego skanu uruchamia backfill historii w tle.
        """
        if product_key not in PRODUCTS:
            log.error(f"Nieznany produkt: {product_key}")
            return None

        lock = self._get_lock(product_key)
        with lock:
            return self._get_locked(product_key)

    def get_many(self, product_keys: list[str]) -> dict[str, dict | None]:
        """Pobiera wiele produktów równolegle."""
        import concurrent.futures
        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
            futures = {ex.submit(self.get, k): k for k in product_keys}
            for f in concurrent.futures.as_completed(futures):
                key = futures[f]
                try:
                    results[key] = f.result()
                except Exception as e:
                    log.error(f"[{key}] get_many błąd: {e}")
                    results[key] = None
        return results

    # ── Logika wewnętrzna ─────────────────────────────────────────────────────

    def _get_locked(self, product_key: str) -> dict | None:
        cached = self._find_cached(product_key)
        if cached is not None:
            path, age_s = cached
            log.info(f"[{product_key}] cache hit ({age_s:.0f}s)  {Path(path).name}")
            # Uruchom backfill jeśli historia niepełna
            self._maybe_backfill(product_key)
            return self._load(path, age_s=age_s, fresh=True)

        log.info(f"[{product_key}] cache miss — pobieram najnowszy ...")
        result = self._fetch_latest(product_key)
        if result is not None:
            self._maybe_backfill(product_key)
        return result

    def _find_cached(self, product_key: str) -> tuple[str, float] | None:
        """Zwraca (path, age_s) jeśli najnowszy plik jest świeży."""
        product_dir = os.path.join(self.cache_dir, product_key)
        if not os.path.isdir(product_dir):
            return None

        files = sorted(
            Path(product_dir).glob("*.h5"),
            key=lambda p: p.stem,
            reverse=True,
        )
        if not files:
            return None

        newest = files[0]
        age_s  = time.time() - newest.stat().st_mtime
        if age_s <= self.ttl_s:
            return str(newest), age_s

        log.debug(f"[{product_key}] nieaktualny ({age_s:.0f}s > TTL {self.ttl_s}s)")
        return None

    def _fetch_latest(self, product_key: str) -> dict | None:
        """Pobiera najnowszy skan z IMGW. Blokujące."""
        for attempt in range(1, MAX_RETRIES + 1):
            scan_dt = find_latest(product_key)
            if scan_dt is None:
                log.warning(f"[{product_key}] find_latest → None (próba {attempt})")
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

            self._trim(product_key)
            age_s = time.time() - os.path.getmtime(local_path)
            log.info(f"[{product_key}] pobrano {Path(local_path).name}")
            return self._load(local_path, age_s=age_s, fresh=False, scan_dt=scan_dt)

        log.error(f"[{product_key}] wszystkie próby nieudane")
        return None

    # ── Historia — backfill ───────────────────────────────────────────────────

    def _maybe_backfill(self, product_key: str):
        """Uruchamia backfill w tle jeśli historia niepełna i nie trwa już."""
        n = self._count_files(product_key)
        if n >= HISTORY_SIZE:
            return

        with self._backfill_lock:
            if product_key in self._backfill_active:
                return
            self._backfill_active.add(product_key)

        t = threading.Thread(
            target=self._backfill,
            args=(product_key,),
            daemon=True,
            name=f"backfill-{product_key}",
        )
        t.start()

    def _backfill(self, product_key: str):
        """
        Pobiera starsze skany w tle aż do HISTORY_SIZE lub max lookback.
        Iteruje wstecz od najstarszego posiadanego pliku, co HISTORY_STEP minut.
        """
        try:
            log.info(f"[{product_key}] backfill start (mamy {self._count_files(product_key)}/{HISTORY_SIZE})")

            # Znajdź czas najstarszego posiadanego skanu
            oldest_dt = self._oldest_scan_dt(product_key)
            if oldest_dt is None:
                log.warning(f"[{product_key}] backfill: brak pliku bazowego")
                return

            # Iteruj wstecz od najstarszego
            added = 0
            current_dt = oldest_dt - timedelta(minutes=HISTORY_STEP)
            cutoff_dt  = oldest_dt - timedelta(minutes=HISTORY_MAX_LOOKBACK)

            while (self._count_files(product_key) < HISTORY_SIZE
                   and current_dt >= cutoff_dt):

                ts         = make_timestamp(current_dt)
                local_path = os.path.join(self.cache_dir, product_key, f"{ts}.h5")

                if not os.path.exists(local_path):
                    url = self._make_url_for_dt(product_key, current_dt)
                    if url and download_file(url, local_path):
                        added += 1
                        log.info(f"[{product_key}] backfill +{Path(local_path).name}")
                    # Przy błędzie: spróbuj kilka sekund-wariantów (dla radarów indyw.)
                    elif product_key not in COMPO_PRODUCTS:
                        url = self._probe_individual(product_key, current_dt)
                        if url and download_file(url, local_path):
                            added += 1
                else:
                    added += 1  # mamy już ten plik

                current_dt -= timedelta(minutes=HISTORY_STEP)

            self._trim(product_key)
            log.info(f"[{product_key}] backfill done — dodano {added}, mamy {self._count_files(product_key)}/{HISTORY_SIZE}")

        except Exception as e:
            log.error(f"[{product_key}] backfill błąd: {e}", exc_info=True)
        finally:
            with self._backfill_lock:
                self._backfill_active.discard(product_key)

    def _make_url_for_dt(self, product_key: str, dt: datetime) -> str | None:
        """Buduje URL dla podanego datetime. Dla kompozytów bezpośrednio, dla indyw. sprawdza HDF5."""
        if product_key in COMPO_PRODUCTS:
            # Kompozyty mają sekundy=0, timestamp jest deterministyczny
            dt_clean = dt.replace(second=0, microsecond=0)
            url = make_url(product_key, dt_clean)
            return url if is_valid_hdf5(url) else None
        return None   # indywidualne obsługuje _probe_individual

    def _probe_individual(self, product_key: str, base_dt: datetime) -> str | None:
        """Dla radarów indywidualnych sprawdza warianty sekund."""
        base_clean = base_dt.replace(second=0, microsecond=0)
        for sec in INDIVIDUAL_SECONDS:
            dt = base_clean.replace(second=sec)
            url = make_url(product_key, dt)
            if is_valid_hdf5(url):
                return url
        return None

    def _oldest_scan_dt(self, product_key: str) -> datetime | None:
        """Datetime najstarszego pliku w cache."""
        product_dir = os.path.join(self.cache_dir, product_key)
        if not os.path.isdir(product_dir):
            return None
        files = sorted(Path(product_dir).glob("*.h5"), key=lambda p: p.stat().st_mtime)
        if not files:
            return None
        stem = files[0].stem
        try:
            return datetime.strptime(stem[:12], "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
        except ValueError:
            return None

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _count_files(self, product_key: str) -> int:
        product_dir = os.path.join(self.cache_dir, product_key)
        if not os.path.isdir(product_dir):
            return 0
        return len(list(Path(product_dir).glob("*.h5")))

    def _trim(self, product_key: str, keep: int = HISTORY_SIZE):
        """Usuwa nadmiarowe pliki — zostaw keep najnowszych."""
        product_dir = os.path.join(self.cache_dir, product_key)
        if not os.path.isdir(product_dir):
            return
        files = sorted(
            Path(product_dir).glob("*.h5"),
            key=lambda p: p.stem,
            reverse=True,
        )
        for old in files[keep:]:
            try:
                old.unlink()
                log.debug(f"[{product_key}] trim: usunięto {old.name}")
            except Exception:
                pass

    def _load(self, path: str, age_s: float = 0,
              fresh: bool = True, scan_dt: datetime | None = None) -> dict | None:
        """Parsuje plik HDF5 i buduje georef."""
        parsed = parse_hdf5(path)
        if parsed is None:
            return None

        georef = build_georef(parsed["where"])

        if scan_dt is None:
            what  = parsed.get("what", {})
            date_ = what.get("date", b"")
            time_ = what.get("time", b"")
            if isinstance(date_, bytes): date_ = date_.decode()
            if isinstance(time_, bytes): time_ = time_.decode()
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

    # ── Historia ──────────────────────────────────────────────────────────────

    def history(self, product_key: str, limit: int = HISTORY_SIZE) -> list[dict]:
        """
        Zwraca listę dostępnych skanów z dysku, od najnowszego.
        Sortuje po nazwie pliku (timestamp w nazwie), nie po mtime —
        mtime jest mylący bo backfill pobiera starsze skany później.
        """
        product_dir = os.path.join(self.cache_dir, product_key)
        if not os.path.isdir(product_dir):
            return []

        files = sorted(
            Path(product_dir).glob("*.h5"),
            key=lambda p: p.stem,   # YYYYMMDDHHmmSS00 — leksykograficznie = chronologicznie
            reverse=True,           # najnowszy pierwszy
        )[:limit]

        result = []
        for f in files:
            stem    = f.stem
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

    def get_by_scan_time(self, product_key: str, scan_time: str) -> dict | None:
        """Zwraca dane dla konkretnego skanu po timestampie (ISO lub YYYYMMDDHHmm...)."""
        product_dir = os.path.join(self.cache_dir, product_key)
        if not os.path.isdir(product_dir):
            return None

        ts_search = None
        if "T" in scan_time or "-" in scan_time:
            try:
                dt = datetime.fromisoformat(scan_time.replace("Z", "+00:00"))
                ts_search = dt.strftime("%Y%m%d%H%M")
            except ValueError:
                return None
        else:
            ts_search = scan_time[:12]

        for f in Path(product_dir).glob("*.h5"):
            if f.stem.startswith(ts_search):
                age_s = time.time() - f.stat().st_mtime
                return self._load(str(f), age_s=age_s, fresh=True)

        log.warning(f"[{product_key}] skan {ts_search} niedostępny lokalnie")
        return None

    # ── Status / zarządzanie ──────────────────────────────────────────────────

    def status(self, product_key: str) -> dict:
        cached = self._find_cached(product_key)
        n = self._count_files(product_key)
        if cached:
            path, age_s = cached
            return {
                "product":       product_key,
                "status":        "fresh",
                "age_s":         age_s,
                "ttl_s":         self.ttl_s,
                "remaining":     max(0, self.ttl_s - age_s),
                "files_on_disk": n,
                "history_full":  n >= HISTORY_SIZE,
            }
        return {
            "product":       product_key,
            "status":        "stale" if n > 0 else "empty",
            "age_s":         None,
            "ttl_s":         self.ttl_s,
            "remaining":     None,
            "files_on_disk": n,
            "history_full":  n >= HISTORY_SIZE,
        }

    def status_all(self, product_keys: list[str] | None = None) -> list[dict]:
        keys = product_keys or list(PRODUCTS.keys())
        return [self.status(k) for k in keys]

    def invalidate(self, product_key: str):
        """Usuwa najnowszy plik — wymusza pobranie przy następnym get()."""
        cached = self._find_cached(product_key)
        if cached:
            path, _ = cached
            try:
                os.remove(path)
                log.info(f"[{product_key}] cache unieważniony: {Path(path).name}")
            except Exception as e:
                log.warning(f"[{product_key}] nie można usunąć: {e}")

    def cleanup(self, keep_last: int = HISTORY_SIZE):
        """Ręczny cleanup — usuwa stare pliki we wszystkich produktach."""
        removed = 0
        for product_key in PRODUCTS:
            before = self._count_files(product_key)
            self._trim(product_key, keep=keep_last)
            removed += max(0, before - self._count_files(product_key))
        if removed:
            log.info(f"cleanup: usunięto {removed} starych plików")
        return removed


# ── Singleton ─────────────────────────────────────────────────────────────────

_default_cache: CacheManager | None = None

def get_cache() -> CacheManager:
    global _default_cache
    if _default_cache is None:
        _default_cache = CacheManager()
    return _default_cache
