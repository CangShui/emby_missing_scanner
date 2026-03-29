from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import functools
import hashlib
import html
import http.server
import json
import locale
import logging
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, TextIO


DEFAULT_EMBY_URL = "http://192.168.1.1:32400"
DEFAULT_EMBY_API_KEY = "66666666666666666666666666666666"
DEFAULT_LIBRARY_NAME = "媒体库01"
DEFAULT_TMDB_BEARER = (
    "66JhbGci66J666666666666666666666666666666MDNlYTMz66Q3NzZhNz66YTliOWNj"
    "YWE1OSIsI66iZiI6MTU66TYyNjEwMC45NDgsInN1YiI66666666666666666MWI0MDAzM2"
    "Q1YzU666I66666666666I6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uI66666666666666666666"
    "j3Sx56666666666TcQDMSq4L51fY0"
)
DEFAULT_TMDB_API_KEY = "66666666666666666666666666666669"
DEFAULT_CONFIG_PATH = r"C:\Users\Public\emby_scan.json"


def _normalize_language(value: Any) -> str:
    raw = str(value or "").strip().lower().replace("_", "-")
    if raw.startswith("zh"):
        return "zh-CN"
    return "en-US"


def _detect_system_language() -> str:
    candidates: list[Optional[str]] = []
    try:
        ctype = locale.getlocale(locale.LC_CTYPE)
        candidates.append(ctype[0] if ctype else None)
    except Exception:
        candidates.append(None)
    try:
        current = locale.getlocale()
        candidates.append(current[0] if current else None)
    except Exception:
        candidates.append(None)
    try:
        candidates.append(locale.setlocale(locale.LC_CTYPE, None))
    except Exception:
        candidates.append(None)
    candidates.append(os.environ.get("LANG"))
    candidates.append(os.environ.get("LC_ALL"))
    for item in candidates:
        if item and str(item).lower().startswith("zh"):
            return "zh-CN"
    return "en-US"


def _default_config() -> dict[str, Any]:
    return {
        "emby_url": DEFAULT_EMBY_URL,
        "emby_api_key": DEFAULT_EMBY_API_KEY,
        "library_name": DEFAULT_LIBRARY_NAME,
        "tmdb_bearer": DEFAULT_TMDB_BEARER,
        "tmdb_api_key": DEFAULT_TMDB_API_KEY,
        "timeout": 15.0,
        "include_specials": False,
        "include_unaired": False,
        "max_series": None,
        "max_lookup_errors": None,
        "log_file": "emby_scan.log",
        "show_progress": True,
        "skip_series_names": [],
        "skip_series_ids": [],
        "concurrency_workers": 8,
        "tmdb_max_retries": 4,
        "tmdb_retry_delay": 2,
        "cache_dir": r"C:\Users\Public\emby_scan",
        "tmdb_api_cache_ttl_hours": 168,
        "tmdb_image_cache_ttl_hours": 999999,
        "cache_images": True,
        "language": _detect_system_language(),
        "web_enabled": True,
        "web_host": "127.0.0.1",
        "web_port": 8765,
    }


def _as_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_name(name: str) -> str:
    return "".join(ch for ch in name.lower() if ch.isalnum())


def _parse_date(value: Any) -> Optional[dt.date]:
    if not value or not isinstance(value, str):
        return None
    try:
        return dt.date.fromisoformat(value)
    except ValueError:
        return None


def _truncate(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def _is_placeholder_overview(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    normalized = text.lower()
    # TMDB placeholder in English (different locales may still show this sentence).
    if "we don't have an overview translated in" in normalized and "help us expand our database by adding one" in normalized:
        return True
    # TMDB placeholder in Chinese (both simplified variants are treated as placeholder).
    if "暂无英文版的简介，请添加内容帮助我们完善数据库" in text:
        return True
    if "暂无中文版的简介，请添加内容帮助我们完善数据库" in text:
        return True
    return False


def _has_meaningful_overview(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    return not _is_placeholder_overview(text)


def _is_generic_episode_name(name: Any, episode_number: int) -> bool:
    text = str(name or "").strip().lower()
    if not text:
        return True
    patterns = [
        rf"^episode\s*0*{episode_number}$",
        rf"^ep\s*0*{episode_number}$",
        rf"^第\s*0*{episode_number}\s*集$",
    ]
    return any(re.match(pattern, text) for pattern in patterns)


class ProgressBar:
    def __init__(self, stream: TextIO, width: int = 30, enabled: bool = True) -> None:
        self.stream = stream
        self.width = width
        self.enabled = enabled
        self._is_tty = bool(getattr(stream, "isatty", lambda: False)())

    def update(self, current: int, total: int, series_name: str) -> None:
        if not self.enabled or total <= 0:
            return
        ratio = min(max(current / total, 0.0), 1.0)
        filled = int(self.width * ratio)
        bar = "#" * filled + "-" * (self.width - filled)
        percent = ratio * 100
        title = _truncate(series_name, 36)
        line = f"[{bar}] {current}/{total} ({percent:5.1f}%) {title}"
        if self._is_tty:
            self.stream.write("\r" + line)
            self.stream.flush()
        else:
            self.stream.write(line + "\n")
            self.stream.flush()

    def finish(self) -> None:
        if self.enabled and self._is_tty:
            self.stream.write("\n")
            self.stream.flush()


def _configure_logger(log_file: Optional[str]) -> Optional[logging.Logger]:
    if not log_file:
        return None

    logger = logging.getLogger("emby_missing_scanner")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.handlers.clear()

    # Use UTF-8 with BOM for better compatibility with Windows text tools.
    handler = logging.FileHandler(log_file, encoding="utf-8-sig")
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    return logger


def load_config(config_path: str) -> dict[str, Any]:
    defaults = _default_config()
    if not os.path.exists(config_path):
        parent = os.path.dirname(config_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(config_path, "w", encoding="utf-8-sig") as file_obj:
            json.dump(defaults, file_obj, ensure_ascii=False, indent=2)
        return defaults

    with open(config_path, "r", encoding="utf-8-sig") as file_obj:
        loaded = json.load(file_obj)
    if not isinstance(loaded, dict):
        raise RuntimeError(f"Invalid config format in {config_path}: expected JSON object.")

    merged = dict(defaults)
    merged.update(loaded)
    merged["language"] = _normalize_language(merged.get("language"))
    # Backward compatibility for legacy single TTL key.
    legacy_ttl = loaded.get("tmdb_cache_ttl_hours")
    if legacy_ttl is not None:
        if "tmdb_api_cache_ttl_hours" not in loaded:
            merged["tmdb_api_cache_ttl_hours"] = legacy_ttl
        if "tmdb_image_cache_ttl_hours" not in loaded:
            merged["tmdb_image_cache_ttl_hours"] = 99999
    merged.pop("tmdb_cache_ttl_hours", None)
    if merged != loaded:
        with open(config_path, "w", encoding="utf-8-sig") as file_obj:
            json.dump(merged, file_obj, ensure_ascii=False, indent=2)
    return merged


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _hash_key(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()


def _parse_timestamp(value: Any) -> Optional[dt.datetime]:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed


class EmbyClient:
    def __init__(self, base_url: str, api_key: str, timeout: float = 15.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout

    def _get(self, path: str, params: Optional[dict[str, Any]] = None) -> Any:
        full_params: dict[str, Any] = {"api_key": self.api_key}
        if params:
            full_params.update({k: v for k, v in params.items() if v is not None})
        query = urllib.parse.urlencode(full_params, doseq=True)
        url = f"{self.base_url}{path}?{query}"
        request = urllib.request.Request(url=url, headers={"Accept": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"Emby API request failed: {exc.code} {exc.reason} ({url})") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Emby API request failed: {exc.reason} ({url})") from exc

    def get_library_id(self, library_name: str) -> Optional[str]:
        payload = self._get("/emby/Library/VirtualFolders")
        folders: list[dict[str, Any]]
        if isinstance(payload, dict):
            folders = payload.get("Items", []) or []
        elif isinstance(payload, list):
            folders = payload
        else:
            folders = []

        wanted = library_name.casefold()
        for folder in folders:
            if str(folder.get("Name", "")).casefold() == wanted:
                library_id = folder.get("ItemId") or folder.get("Id")
                if library_id:
                    return str(library_id)
        return None

    def get_server_id(self) -> Optional[str]:
        payload = self._get("/emby/System/Info/Public")
        if isinstance(payload, dict):
            for key in ("Id", "ServerId", "id", "serverId"):
                value = payload.get(key)
                if value:
                    return str(value)
        return None

    def get_series_items(self, library_id: str) -> list[dict[str, Any]]:
        payload = self._get(
            "/emby/Items",
            {
                "ParentId": library_id,
                "Recursive": "true",
                "IncludeItemTypes": "Series",
                "Fields": "ProviderIds,ProductionYear",
                "Limit": 100000,
            },
        )
        return payload.get("Items", []) if isinstance(payload, dict) else []

    def get_emby_episode_map(self, series_id: str) -> dict[int, set[int]]:
        seasons_payload = self._get(
            "/emby/Items",
            {
                "ParentId": series_id,
                "Recursive": "false",
                "IncludeItemTypes": "Season",
                "Fields": "IndexNumber",
                "Limit": 1000,
            },
        )
        seasons = seasons_payload.get("Items", []) if isinstance(seasons_payload, dict) else []
        season_to_episodes: dict[int, set[int]] = {}

        for season in seasons:
            season_id = season.get("Id")
            season_number = _as_int(season.get("IndexNumber"))
            if season_id is None or season_number is None:
                continue
            episodes_payload = self._get(
                "/emby/Items",
                {
                    "ParentId": season_id,
                    "Recursive": "false",
                    "IncludeItemTypes": "Episode",
                    "Fields": "IndexNumber",
                    "Limit": 10000,
                },
            )
            episodes = episodes_payload.get("Items", []) if isinstance(episodes_payload, dict) else []
            episode_numbers: set[int] = set()
            for episode in episodes:
                episode_number = _as_int(episode.get("IndexNumber"))
                if episode_number is not None:
                    episode_numbers.add(episode_number)
            season_to_episodes[season_number] = episode_numbers

        return season_to_episodes


class TmdbClient:
    def __init__(
        self,
        bearer_token: Optional[str],
        api_key: Optional[str],
        language: str = "en-US",
        timeout: float = 15.0,
        base_url: str = "https://api.themoviedb.org/3",
        max_retries: int = 2,
        retry_delay: float = 0.5,
        cache_dir: str = r"C:\Users\Public\emby_scan",
        api_cache_ttl_hours: float = 1.0,
        image_cache_ttl_hours: float = 99999.0,
        cache_images: bool = True,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.bearer_token = (bearer_token or "").strip()
        self.api_key = (api_key or "").strip()
        self.language = _normalize_language(language)
        self.timeout = timeout
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.cache_dir = cache_dir
        self.api_cache_ttl_seconds = max(0.0, float(api_cache_ttl_hours)) * 3600.0
        self.image_cache_ttl_seconds = max(0.0, float(image_cache_ttl_hours)) * 3600.0
        self.cache_images = bool(cache_images)
        self.logger = logger
        self.image_base_url = "https://image.tmdb.org/t/p/w500"
        self.api_cache_dir = os.path.join(self.cache_dir, "cache", "api")
        self.image_cache_dir = os.path.join(self.cache_dir, "cache", "images")
        os.makedirs(self.api_cache_dir, exist_ok=True)
        os.makedirs(self.image_cache_dir, exist_ok=True)
        self._cache_lock = threading.Lock()
        if self.api_cache_ttl_seconds <= 0:
            self._purge_cache_directory(self.api_cache_dir)
        if self.image_cache_ttl_seconds <= 0:
            self._purge_cache_directory(self.image_cache_dir)

    def _purge_cache_directory(self, directory: str) -> None:
        removed = 0
        for root, _, files in os.walk(directory):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                try:
                    os.remove(file_path)
                    removed += 1
                except OSError:
                    continue
        if removed and self.logger:
            self.logger.info("Purged %d cache files under %s", removed, directory)

    def _headers(self, use_bearer: bool) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if use_bearer and self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"
        return headers

    def _is_api_cache_fresh(self, fetched_at: Optional[dt.datetime]) -> bool:
        if fetched_at is None:
            return False
        if self.api_cache_ttl_seconds <= 0:
            return False
        age = (_utc_now() - fetched_at).total_seconds()
        return age <= self.api_cache_ttl_seconds

    def _is_image_cache_fresh(self, fetched_at: Optional[dt.datetime]) -> bool:
        if fetched_at is None:
            return False
        if self.image_cache_ttl_seconds <= 0:
            return False
        age = (_utc_now() - fetched_at).total_seconds()
        return age <= self.image_cache_ttl_seconds

    def _api_cache_path(self, url: str) -> str:
        return os.path.join(self.api_cache_dir, f"{_hash_key(url)}.json")

    def _load_api_cache(self, url: str) -> Optional[Any]:
        cache_path = self._api_cache_path(url)
        if not os.path.exists(cache_path):
            return None
        try:
            with open(cache_path, "r", encoding="utf-8-sig") as cache_file:
                payload = json.load(cache_file)
            if not isinstance(payload, dict):
                return None
            fetched_at = _parse_timestamp(payload.get("fetched_at"))
            if self._is_api_cache_fresh(fetched_at):
                if self.logger:
                    self.logger.info("TMDB cache hit: %s", url)
                return payload.get("data")
        except Exception:
            return None
        return None

    def _save_api_cache(self, url: str, data: Any) -> None:
        if self.api_cache_ttl_seconds <= 0:
            return
        cache_path = self._api_cache_path(url)
        payload = {"fetched_at": _utc_now().isoformat(), "url": url, "data": data}
        try:
            with open(cache_path, "w", encoding="utf-8-sig") as cache_file:
                json.dump(payload, cache_file, ensure_ascii=False)
        except Exception:
            if self.logger:
                self.logger.warning("Failed to write TMDB API cache: %s", cache_path)

    def _image_cache_paths(self, image_path: str) -> tuple[str, str, str]:
        ext = os.path.splitext(image_path)[1] or ".jpg"
        key = _hash_key(image_path)
        file_name = f"{key}{ext}"
        image_file = os.path.join(self.image_cache_dir, file_name)
        meta_file = os.path.join(self.image_cache_dir, f"{key}.json")
        rel_path = f"cache/images/{file_name}".replace("\\", "/")
        return image_file, meta_file, rel_path

    def cache_image(self, image_path: Any) -> Optional[str]:
        if not isinstance(image_path, str) or not image_path.strip():
            return None
        normalized = image_path.strip()
        image_url = f"{self.image_base_url}{normalized}"
        if not self.cache_images or self.image_cache_ttl_seconds <= 0:
            return image_url

        image_file, meta_file, rel_path = self._image_cache_paths(normalized)

        with self._cache_lock:
            if os.path.exists(image_file) and os.path.exists(meta_file):
                try:
                    with open(meta_file, "r", encoding="utf-8-sig") as meta:
                        payload = json.load(meta)
                    fetched_at = _parse_timestamp(payload.get("fetched_at"))
                    if self._is_image_cache_fresh(fetched_at):
                        return rel_path
                except Exception:
                    pass

        request = urllib.request.Request(url=image_url, headers={"Accept": "image/*"})
        data: Optional[bytes] = None
        for attempt in range(self.max_retries + 1):
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    data = response.read()
                break
            except urllib.error.URLError:
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay)
                    continue
                break
            except urllib.error.HTTPError as exc:
                if (exc.code == 429 or exc.code >= 500) and attempt < self.max_retries:
                    time.sleep(self.retry_delay)
                    continue
                break

        if data is None:
            return None

        with self._cache_lock:
            try:
                with open(image_file, "wb") as image_out:
                    image_out.write(data)
                with open(meta_file, "w", encoding="utf-8-sig") as meta_out:
                    json.dump(
                        {"fetched_at": _utc_now().isoformat(), "source_path": normalized, "source_url": image_url},
                        meta_out,
                        ensure_ascii=False,
                    )
                return rel_path
            except Exception:
                if self.logger:
                    self.logger.warning("Failed to write TMDB image cache for %s", normalized)
                return None

    def _get(self, path: str, params: Optional[dict[str, Any]] = None, force_api_key: bool = False) -> Any:
        full_params = dict(params or {})
        use_bearer = bool(self.bearer_token) and not force_api_key
        if not use_bearer and self.api_key:
            full_params["api_key"] = self.api_key
        query = urllib.parse.urlencode({k: v for k, v in full_params.items() if v is not None}, doseq=True)
        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{query}"

        with self._cache_lock:
            cached = self._load_api_cache(url)
        if cached is not None:
            return cached

        request = urllib.request.Request(url=url, headers=self._headers(use_bearer))
        last_error: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    payload = json.load(response)
                with self._cache_lock:
                    self._save_api_cache(url, payload)
                return payload
            except urllib.error.HTTPError as exc:
                if exc.code == 401 and use_bearer and self.api_key:
                    # Fall back to api_key auth for this request when bearer auth is rejected.
                    return self._get(path, params, force_api_key=True)
                if (exc.code == 429 or exc.code >= 500) and attempt < self.max_retries:
                    time.sleep(self.retry_delay)
                    continue
                last_error = exc
                break
            except urllib.error.URLError as exc:
                last_error = exc
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay)
                    continue
                break

        if isinstance(last_error, urllib.error.HTTPError):
            raise RuntimeError(
                f"TMDB API request failed: {last_error.code} {last_error.reason} ({url})"
            ) from last_error
        if isinstance(last_error, urllib.error.URLError):
            raise RuntimeError(f"TMDB API request failed: {last_error.reason} ({url})") from last_error
        raise RuntimeError(f"TMDB API request failed for unknown reason ({url})")

    def resolve_tv_id(
        self,
        provider_ids: Optional[dict[str, Any]],
        series_name: str,
        production_year: Optional[int],
    ) -> Optional[int]:
        provider_ids = provider_ids or {}
        tmdb_from_provider = provider_ids.get("Tmdb") or provider_ids.get("TMDB") or provider_ids.get("tmdb")
        tmdb_id = _as_int(tmdb_from_provider)
        if tmdb_id is not None:
            return tmdb_id

        search_payload = self._get(
            "/search/tv",
            {
                "query": series_name,
                "first_air_date_year": production_year,
                "include_adult": "false",
                "language": self.language,
            },
        )
        results = search_payload.get("results", []) if isinstance(search_payload, dict) else []
        if not results and production_year is not None:
            search_payload = self._get(
                "/search/tv",
                {"query": series_name, "include_adult": "false", "language": self.language},
            )
            results = search_payload.get("results", []) if isinstance(search_payload, dict) else []
        if not results:
            return None

        normalized_target = _normalize_name(series_name)
        exact_match = None
        for result in results:
            candidate_names = [str(result.get("name", "")), str(result.get("original_name", ""))]
            if normalized_target in {_normalize_name(name) for name in candidate_names if name}:
                exact_match = result
                break

        picked = exact_match or results[0]
        return _as_int(picked.get("id"))

    def get_expected_episode_map(
        self,
        tv_id: int,
        include_specials: bool = False,
        include_unaired: bool = False,
    ) -> tuple[
        dict[int, set[int]],
        dict[int, str],
        dict[int, dict[int, str]],
        dict[int, dict[int, str]],
        dict[int, dict[int, dict[str, Any]]],
        dict[str, Any],
    ]:
        show_payload = self._get(f"/tv/{tv_id}", {"language": self.language})
        seasons = show_payload.get("seasons", []) if isinstance(show_payload, dict) else []
        series_poster_path = str(show_payload.get("poster_path") or "").strip()

        today = dt.date.today()
        expected: dict[int, set[int]] = {}
        skipped_unaired_seasons: dict[int, str] = {}
        skipped_placeholder_episodes: dict[int, dict[int, str]] = {}
        translation_fallback_episodes: dict[int, dict[int, str]] = {}
        episode_meta: dict[int, dict[int, dict[str, Any]]] = {}
        for season in seasons:
            season_number = _as_int(season.get("season_number"))
            if season_number is None:
                continue
            if season_number == 0 and not include_specials:
                continue

            episode_count = _as_int(season.get("episode_count")) or 0
            if episode_count <= 0:
                continue

            season_poster_path = str(season.get("poster_path") or "").strip()
            season_payload = self._get(f"/tv/{tv_id}/season/{season_number}", {"language": self.language})
            episodes_local = season_payload.get("episodes", []) if isinstance(season_payload, dict) else []

            local_by_num: dict[int, dict[str, Any]] = {}
            for episode in episodes_local:
                episode_number = _as_int(episode.get("episode_number"))
                if episode_number is not None:
                    local_by_num[episode_number] = episode

            en_by_num: dict[int, dict[str, Any]] = {}
            en_loaded = False

            def ensure_en_loaded() -> None:
                nonlocal en_loaded
                if en_loaded or self.language == "en-US":
                    return
                season_payload_en = self._get(f"/tv/{tv_id}/season/{season_number}", {"language": "en-US"})
                episodes_en = season_payload_en.get("episodes", []) if isinstance(season_payload_en, dict) else []
                for en_episode in episodes_en:
                    episode_number = _as_int(en_episode.get("episode_number"))
                    if episode_number is not None:
                        en_by_num[episode_number] = en_episode
                en_loaded = True

            first_episode_air_date: Optional[dt.date] = None
            first_ep_local = local_by_num.get(1)
            if first_ep_local is not None:
                first_episode_air_date = _parse_date(first_ep_local.get("air_date"))
            if first_episode_air_date is None:
                for episode_number in sorted(local_by_num.keys()):
                    first_episode_air_date = _parse_date(local_by_num[episode_number].get("air_date"))
                    if first_episode_air_date is not None:
                        break
            if first_episode_air_date is None and self.language != "en-US":
                ensure_en_loaded()
                first_ep_en = en_by_num.get(1)
                if first_ep_en is not None:
                    first_episode_air_date = _parse_date(first_ep_en.get("air_date"))
                if first_episode_air_date is None:
                    for episode_number in sorted(en_by_num.keys()):
                        first_episode_air_date = _parse_date(en_by_num[episode_number].get("air_date"))
                        if first_episode_air_date is not None:
                            break

            if not include_unaired:
                if first_episode_air_date is None:
                    skipped_unaired_seasons[season_number] = "first episode has no air_date"
                    continue
                if first_episode_air_date > today:
                    skipped_unaired_seasons[season_number] = (
                        f"first episode air_date {first_episode_air_date.isoformat()} is in the future"
                    )
                    continue

            episode_numbers: set[int] = set()
            ambiguous_numbers: list[int] = []
            for episode_number in sorted(local_by_num.keys()):
                local_episode = local_by_num.get(episode_number, {})
                local_overview = str(local_episode.get("overview") or "")
                local_still = str(local_episode.get("still_path") or "").strip()
                air_date_raw = local_episode.get("air_date")
                overview = local_overview
                still_path = local_still

                name = str(local_episode.get("name") or f"Episode {episode_number}")
                episode_meta.setdefault(season_number, {})[episode_number] = {
                    "name": name,
                    "overview": overview,
                    "air_date": air_date_raw,
                    "still_path": still_path,
                    "season_poster_path": season_poster_path,
                    "series_poster_path": series_poster_path,
                }

                has_meaningful_local = _has_meaningful_overview(local_overview)
                runtime_local = _as_int(local_episode.get("runtime"))
                vote_count_local = _as_int(local_episode.get("vote_count")) or 0
                weak_placeholder_local = (
                    (not has_meaningful_local)
                    and _is_generic_episode_name(name, episode_number)
                    and runtime_local in (None, 0)
                    and vote_count_local == 0
                )
                if not has_meaningful_local and (not local_still or weak_placeholder_local):
                    ambiguous_numbers.append(episode_number)
                    continue
                if not include_unaired:
                    air_date = _parse_date(air_date_raw)
                    if air_date is None or air_date > today:
                        continue
                episode_numbers.add(episode_number)

            if ambiguous_numbers and self.language != "en-US":
                ensure_en_loaded()

            for episode_number in ambiguous_numbers:
                local_episode = local_by_num.get(episode_number, {})
                en_episode = en_by_num.get(episode_number, {})

                local_overview = str(local_episode.get("overview") or "")
                en_overview = str(en_episode.get("overview") or "")
                local_still = str(local_episode.get("still_path") or "").strip()
                en_still = str(en_episode.get("still_path") or "").strip()
                air_date_raw = local_episode.get("air_date") or en_episode.get("air_date")
                name = str(local_episode.get("name") or en_episode.get("name") or f"Episode {episode_number}")
                runtime_local = _as_int(local_episode.get("runtime"))
                runtime_en = _as_int(en_episode.get("runtime"))
                runtime_value = runtime_local if runtime_local not in (None, 0) else runtime_en
                vote_count_local = _as_int(local_episode.get("vote_count")) or 0
                vote_count_en = _as_int(en_episode.get("vote_count")) or 0
                vote_count_value = max(vote_count_local, vote_count_en)

                used_fallback = False
                overview = local_overview
                if not _has_meaningful_overview(local_overview) and _has_meaningful_overview(en_overview):
                    overview = en_overview
                    used_fallback = True
                still_path = local_still or en_still

                episode_meta.setdefault(season_number, {})[episode_number] = {
                    "name": name,
                    "overview": overview,
                    "air_date": air_date_raw,
                    "still_path": still_path,
                    "season_poster_path": season_poster_path,
                    "series_poster_path": series_poster_path,
                }

                has_meaningful_overview = _has_meaningful_overview(local_overview) or _has_meaningful_overview(en_overview)
                if not has_meaningful_overview and not still_path:
                    skipped_placeholder_episodes.setdefault(season_number, {})[episode_number] = (
                        f"no meaningful overview in {self.language}/en-US and no still image"
                    )
                    continue
                if (
                    not has_meaningful_overview
                    and _is_generic_episode_name(name, episode_number)
                    and runtime_value in (None, 0)
                    and vote_count_value == 0
                ):
                    skipped_placeholder_episodes.setdefault(season_number, {})[episode_number] = (
                        "generic title + empty overview + no runtime + zero votes"
                    )
                    continue
                if used_fallback:
                    translation_fallback_episodes.setdefault(season_number, {})[episode_number] = (
                        f"{self.language} overview unavailable; used en-US fallback"
                    )
                if not include_unaired:
                    air_date = _parse_date(air_date_raw)
                    if air_date is None or air_date > today:
                        continue
                episode_numbers.add(episode_number)

            if episode_numbers:
                expected[season_number] = episode_numbers
            elif include_unaired and not local_by_num and not en_by_num:
                expected[season_number] = set(range(1, episode_count + 1))
        series_meta = {
            "series_name": str(show_payload.get("name") or ""),
            "series_overview": str(show_payload.get("overview") or ""),
            "series_poster_path": series_poster_path,
        }
        return (
            expected,
            skipped_unaired_seasons,
            skipped_placeholder_episodes,
            translation_fallback_episodes,
            episode_meta,
            series_meta,
        )


@dataclass(frozen=True)
class MissingEpisodeDetail:
    season_number: int
    episode_number: int
    title: str
    overview: str
    air_date: Optional[str]
    image_relpath: Optional[str]
    emby_url: str
    tmdb_url: str


@dataclass(frozen=True)
class MissingReport:
    series_name: str
    emby_series_id: str
    tmdb_tv_id: int
    missing_seasons: list[int]
    missing_episodes: dict[int, list[int]]
    series_overview: str = ""
    series_poster_relpath: Optional[str] = None
    emby_series_url: str = ""
    tmdb_series_url: str = ""
    missing_episode_details: list[MissingEpisodeDetail] = field(default_factory=list)


@dataclass(frozen=True)
class ScanSummary:
    library_name: str
    total_series_in_library: int
    target_series_to_scan: int
    processed_series: int
    skipped_series: int
    requested_tmdb_series: int
    completed_full_library_scan: bool
    stopped_early_reason: Optional[str]
    elapsed_seconds: float


def scan_missing_content(
    emby_client: EmbyClient,
    tmdb_client: TmdbClient,
    library_name: str,
    include_specials: bool = False,
    include_unaired: bool = False,
    max_series: Optional[int] = None,
    max_lookup_errors: Optional[int] = None,
    skip_series_names: Optional[list[str]] = None,
    skip_series_ids: Optional[list[str]] = None,
    concurrency_workers: int = 4,
    emby_web_base_url: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
) -> tuple[list[MissingReport], list[tuple[str, str]], list[tuple[str, str, str]], ScanSummary]:
    started_at = time.time()
    library_id = emby_client.get_library_id(library_name)
    if not library_id:
        raise RuntimeError(f"Library not found in Emby: {library_name}")

    series_items = emby_client.get_series_items(library_id)
    total_series = len(series_items)
    target_series_items = series_items[: max_series if max_series is not None else None]
    target_series = len(target_series_items)
    skip_name_set = {_normalize_name(name) for name in (skip_series_names or []) if name}
    skip_id_set = {str(series_id) for series_id in (skip_series_ids or []) if series_id is not None}
    max_workers = max(1, int(concurrency_workers))

    if logger:
        logger.info(
            (
                "Scan started: library=%s, total_series=%d, target_series=%d, "
                "concurrency_workers=%d, include_unaired=%s"
            ),
            library_name,
            total_series,
            target_series,
            max_workers,
            include_unaired,
        )

    missing_reports: list[MissingReport] = []
    unmatched_series: list[tuple[str, str]] = []
    tmdb_lookup_errors: list[tuple[str, str, str]] = []
    skipped_series = 0
    requested_tmdb_series = 0
    processed_series = 0
    stop_reason: Optional[str] = None

    effective_emby_base = (emby_web_base_url or getattr(emby_client, "base_url", DEFAULT_EMBY_URL)).rstrip("/")
    server_id: Optional[str] = None
    get_server_id_fn = getattr(emby_client, "get_server_id", None)
    if callable(get_server_id_fn):
        try:
            server_id = get_server_id_fn()
        except Exception:
            server_id = None
    cache_image_fn = getattr(tmdb_client, "cache_image", None)

    def resolve_image(image_path: Any) -> Optional[str]:
        if not callable(cache_image_fn):
            return None
        try:
            return cache_image_fn(image_path)
        except Exception:
            return None

    def process_one_series(
        series_item: dict[str, Any],
    ) -> tuple[
        str,
        str,
        Optional[MissingReport],
        bool,
        Optional[str],
        dict[int, str],
        dict[int, dict[int, str]],
        dict[int, dict[int, str]],
    ]:
        series_id = str(series_item.get("Id", ""))
        series_name = str(series_item.get("Name", f"Series-{series_id}"))
        production_year = _as_int(series_item.get("ProductionYear"))
        provider_ids = series_item.get("ProviderIds") if isinstance(series_item.get("ProviderIds"), dict) else {}

        try:
            tmdb_tv_id = tmdb_client.resolve_tv_id(provider_ids, series_name, production_year)
        except RuntimeError as exc:
            return series_name, series_id, None, False, str(exc), {}, {}, {}
        if tmdb_tv_id is None:
            return series_name, series_id, None, True, None, {}, {}, {}

        try:
            expected_data = tmdb_client.get_expected_episode_map(
                tmdb_tv_id,
                include_specials=include_specials,
                include_unaired=include_unaired,
            )
            episode_meta: dict[int, dict[int, dict[str, Any]]] = {}
            series_meta: dict[str, Any] = {}
            translation_fallback_episodes: dict[int, dict[int, str]] = {}
            if isinstance(expected_data, tuple):
                if len(expected_data) >= 6:
                    (
                        expected_map,
                        skipped_unaired_seasons,
                        skipped_placeholder_episodes,
                        translation_fallback_episodes,
                        episode_meta,
                        series_meta,
                    ) = expected_data[:6]
                elif len(expected_data) >= 5:
                    expected_map, skipped_unaired_seasons, skipped_placeholder_episodes, episode_meta, series_meta = (
                        expected_data[:5]
                    )
                elif len(expected_data) >= 3:
                    expected_map, skipped_unaired_seasons, skipped_placeholder_episodes = expected_data[:3]
                elif len(expected_data) == 2:
                    expected_map, skipped_unaired_seasons = expected_data
                    skipped_placeholder_episodes = {}
                else:
                    expected_map = {}
                    skipped_unaired_seasons = {}
                    skipped_placeholder_episodes = {}
            else:
                expected_map = expected_data
                skipped_unaired_seasons = {}
                skipped_placeholder_episodes = {}
                translation_fallback_episodes = {}
                episode_meta = {}
                series_meta = {}
        except RuntimeError as exc:
            return series_name, series_id, None, False, str(exc), {}, {}, {}

        if not expected_map:
            return (
                series_name,
                series_id,
                None,
                False,
                None,
                skipped_unaired_seasons,
                skipped_placeholder_episodes,
                {},
            )

        emby_map = emby_client.get_emby_episode_map(series_id)
        expected_seasons = set(expected_map.keys())
        existing_seasons = set(emby_map.keys())
        missing_seasons = sorted(expected_seasons - existing_seasons)

        missing_episodes: dict[int, list[int]] = {}
        for season_number in sorted(expected_seasons & existing_seasons):
            expected_eps = expected_map[season_number]
            existing_eps = emby_map.get(season_number, set())
            missing_eps = sorted(expected_eps - existing_eps)
            if missing_eps:
                missing_episodes[season_number] = missing_eps

        if not missing_seasons and not missing_episodes:
            return (
                series_name,
                series_id,
                None,
                False,
                None,
                skipped_unaired_seasons,
                skipped_placeholder_episodes,
                {},
            )

        episode_numbers_to_show: dict[int, list[int]] = {}
        for season_number in missing_seasons:
            episode_numbers_to_show[season_number] = sorted(expected_map.get(season_number, set()))
        for season_number, episode_numbers in missing_episodes.items():
            episode_numbers_to_show[season_number] = sorted(
                set(episode_numbers_to_show.get(season_number, [])) | set(episode_numbers)
            )
        relevant_fallback: dict[int, dict[int, str]] = {}
        for season_number, reasons in translation_fallback_episodes.items():
            for episode_number, reason in reasons.items():
                if episode_number in set(episode_numbers_to_show.get(season_number, [])):
                    relevant_fallback.setdefault(season_number, {})[episode_number] = reason

        missing_episode_details: list[MissingEpisodeDetail] = []
        series_poster_relpath = resolve_image(series_meta.get("series_poster_path"))
        season_image_cache: dict[int, Optional[str]] = {}
        for season_number in sorted(episode_numbers_to_show.keys()):
            for episode_number in episode_numbers_to_show[season_number]:
                meta = episode_meta.get(season_number, {}).get(episode_number, {})
                image_relpath = resolve_image(meta.get("still_path"))
                if not image_relpath:
                    if season_number not in season_image_cache:
                        season_image_cache[season_number] = resolve_image(meta.get("season_poster_path"))
                    image_relpath = season_image_cache[season_number] or series_poster_relpath
                missing_episode_details.append(
                    MissingEpisodeDetail(
                        season_number=season_number,
                        episode_number=episode_number,
                        title=str(meta.get("name") or f"E{episode_number}"),
                        overview=str(meta.get("overview") or ""),
                        air_date=str(meta["air_date"]) if meta.get("air_date") else None,
                        image_relpath=image_relpath,
                        emby_url=_build_emby_item_url(effective_emby_base, series_id, server_id),
                        tmdb_url=f"https://www.themoviedb.org/tv/{tmdb_tv_id}/season/{season_number}/episode/{episode_number}",
                    )
                )

        report = MissingReport(
            series_name=series_name,
            emby_series_id=series_id,
            tmdb_tv_id=tmdb_tv_id,
            missing_seasons=missing_seasons,
            missing_episodes=missing_episodes,
            series_overview=str(series_meta.get("series_overview") or ""),
            series_poster_relpath=series_poster_relpath,
            emby_series_url=_build_emby_item_url(effective_emby_base, series_id, server_id),
            tmdb_series_url=f"https://www.themoviedb.org/tv/{tmdb_tv_id}",
            missing_episode_details=missing_episode_details,
        )
        return (
            series_name,
            series_id,
            report,
            False,
            None,
            skipped_unaired_seasons,
            skipped_placeholder_episodes,
            relevant_fallback,
        )

    futures_to_meta: dict[concurrent.futures.Future[Any], tuple[str, str]] = {}
    iterator = iter(target_series_items)

    def submit_next(executor: concurrent.futures.ThreadPoolExecutor) -> bool:
        nonlocal processed_series, skipped_series, requested_tmdb_series
        while True:
            try:
                series_item = next(iterator)
            except StopIteration:
                return False

            series_id = str(series_item.get("Id", ""))
            series_name = str(series_item.get("Name", f"Series-{series_id}")) if series_id else "Unknown"

            if not series_id:
                processed_series += 1
                if progress_callback:
                    progress_callback(processed_series, target_series, series_name)
                if logger:
                    logger.info("Skipped invalid item without Emby ID: %s", series_name)
                continue

            if series_id in skip_id_set or _normalize_name(series_name) in skip_name_set:
                skipped_series += 1
                processed_series += 1
                if progress_callback:
                    progress_callback(processed_series, target_series, series_name)
                if logger:
                    logger.info("Skipped by user config: %s (Emby ID: %s)", series_name, series_id)
                continue

            future = executor.submit(process_one_series, series_item)
            futures_to_meta[future] = (series_name, series_id)
            requested_tmdb_series += 1
            return True

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        while len(futures_to_meta) < max_workers and submit_next(executor):
            pass

        while futures_to_meta:
            done, _ = concurrent.futures.wait(
                futures_to_meta.keys(),
                return_when=concurrent.futures.FIRST_COMPLETED,
            )
            for future in done:
                series_name, series_id = futures_to_meta.pop(future, ("Unknown", ""))
                processed_series += 1
                if progress_callback:
                    progress_callback(processed_series, target_series, series_name)

                try:
                    (
                        result_series_name,
                        result_series_id,
                        report,
                        unmatched,
                        lookup_error,
                        skipped_unaired,
                        skipped_placeholder_episodes,
                        translation_fallback_episodes,
                    ) = future.result()
                except Exception as exc:  # pragma: no cover - defensive
                    tmdb_lookup_errors.append((series_name, series_id, f"Unexpected scan error: {exc}"))
                    if logger:
                        logger.exception("Unexpected failure while scanning %s (%s)", series_name, series_id)
                    continue

                for season_number, reason in skipped_unaired.items():
                    if logger:
                        logger.info(
                            "Ignored unaired/unconfirmed season for %s (%s): S%s (%s)",
                            result_series_name,
                            result_series_id,
                            season_number,
                            reason,
                        )
                for season_number, episode_reasons in skipped_placeholder_episodes.items():
                    for episode_number, reason in sorted(episode_reasons.items()):
                        if logger:
                            logger.info(
                                "Ignored TMDB placeholder episode for %s (%s): S%sE%s (%s)",
                                result_series_name,
                                result_series_id,
                                season_number,
                                episode_number,
                                reason,
                            )
                for season_number, episode_reasons in translation_fallback_episodes.items():
                    for episode_number, reason in sorted(episode_reasons.items()):
                        if logger:
                            logger.info(
                                "Used TMDB en-US fallback overview for %s (%s): S%sE%s (%s)",
                                result_series_name,
                                result_series_id,
                                season_number,
                                episode_number,
                                reason,
                            )

                if lookup_error:
                    tmdb_lookup_errors.append((result_series_name, result_series_id, lookup_error))
                    if logger:
                        logger.warning("TMDB lookup failed for %s (%s): %s", result_series_name, result_series_id, lookup_error)
                    if max_lookup_errors is not None and len(tmdb_lookup_errors) >= max_lookup_errors:
                        stop_reason = f"Reached max lookup errors limit ({max_lookup_errors})"
                        for pending in list(futures_to_meta.keys()):
                            pending.cancel()
                        futures_to_meta.clear()
                        break
                    continue

                if unmatched:
                    unmatched_series.append((result_series_name, result_series_id))
                    if logger:
                        logger.info("TMDB unmatched: %s (%s)", result_series_name, result_series_id)
                    continue

                if report:
                    missing_reports.append(report)
                    if logger:
                        logger.info(
                            "Missing detected: %s (%s), missing_seasons=%s, missing_episode_seasons=%s",
                            report.series_name,
                            report.emby_series_id,
                            report.missing_seasons,
                            sorted(report.missing_episodes.keys()),
                        )

            if stop_reason:
                break
            while len(futures_to_meta) < max_workers and submit_next(executor):
                pass

    if stop_reason is None and max_series is not None and target_series < total_series:
        stop_reason = f"Reached max series limit ({max_series})"
    if stop_reason is None and processed_series < target_series:
        stop_reason = "Stopped early before target series finished"

    completed_full_library_scan = processed_series >= total_series and max_series is None and stop_reason is None
    missing_reports.sort(key=lambda item: (_normalize_name(item.series_name), item.emby_series_id))
    unmatched_series.sort(key=lambda item: (_normalize_name(item[0]), item[1]))
    tmdb_lookup_errors.sort(key=lambda item: (_normalize_name(item[0]), item[1]))
    summary = ScanSummary(
        library_name=library_name,
        total_series_in_library=total_series,
        target_series_to_scan=target_series,
        processed_series=processed_series,
        skipped_series=skipped_series,
        requested_tmdb_series=requested_tmdb_series,
        completed_full_library_scan=completed_full_library_scan,
        stopped_early_reason=stop_reason,
        elapsed_seconds=time.time() - started_at,
    )
    if logger:
        logger.info(
            (
                "Scan completed: processed=%d/%d, completed_full_library_scan=%s, "
                "requested_tmdb=%d, skipped_by_user=%d, missing_series=%d, unmatched_series=%d, "
                "lookup_errors=%d, reason=%s, elapsed=%.2fs"
            ),
            summary.processed_series,
            summary.target_series_to_scan,
            summary.completed_full_library_scan,
            summary.requested_tmdb_series,
            summary.skipped_series,
            len(missing_reports),
            len(unmatched_series),
            len(tmdb_lookup_errors),
            summary.stopped_early_reason or "None",
            summary.elapsed_seconds,
        )

    return missing_reports, unmatched_series, tmdb_lookup_errors, summary


def print_report(
    reports: list[MissingReport],
    unmatched_series: list[tuple[str, str]],
    lookup_errors: list[tuple[str, str, str]],
    summary: ScanSummary,
    log_file: Optional[str] = None,
    stream: Any = sys.stdout,
) -> None:
    print("Scan summary:", file=stream)
    print(
        (
            f"- Library: {summary.library_name}\n"
            f"- Processed: {summary.processed_series}/{summary.target_series_to_scan} "
            f"(library total: {summary.total_series_in_library})\n"
            f"- Requested TMDB checks: {summary.requested_tmdb_series}\n"
            f"- Skipped by user config: {summary.skipped_series}\n"
            f"- Full library scanned: {'YES' if summary.completed_full_library_scan else 'NO'}\n"
            f"- Elapsed: {summary.elapsed_seconds:.2f}s"
        ),
        file=stream,
    )
    if summary.stopped_early_reason:
        print(f"- Stopped early reason: {summary.stopped_early_reason}", file=stream)
    if log_file:
        print(f"- Log file: {log_file}", file=stream)
    print("", file=stream)

    if reports:
        print("Detected missing seasons/episodes:", file=stream)
        for report in reports:
            print(
                f"- {report.series_name} "
                f"(Emby ID: {report.emby_series_id}, TMDB ID: {report.tmdb_tv_id})",
                file=stream,
            )
            if report.missing_seasons:
                season_text = ", ".join(f"S{season}" for season in report.missing_seasons)
                print(f"  Missing seasons: {season_text}", file=stream)
            for season_number, episodes in report.missing_episodes.items():
                episode_text = ", ".join(str(ep) for ep in episodes)
                print(f"  Missing episodes in S{season_number}: {episode_text}", file=stream)
    else:
        print("No missing seasons/episodes were detected.", file=stream)

    if unmatched_series:
        print("\nSeries that could not be matched to TMDB:", file=stream)
        for title, series_id in unmatched_series:
            print(f"- {title} (Emby ID: {series_id})", file=stream)
    if lookup_errors:
        print("\nSeries skipped due TMDB lookup errors:", file=stream)
        for title, series_id, reason in lookup_errors:
            print(f"- {title} (Emby ID: {series_id}) -> {reason}", file=stream)


def _safe_slug(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() else "-" for ch in value.strip().lower())
    cleaned = "-".join(part for part in cleaned.split("-") if part)
    return cleaned[:80] if cleaned else "series"


def _build_emby_item_url(base_url: str, item_id: str, server_id: Optional[str]) -> str:
    params = {"id": item_id}
    if server_id:
        params["serverId"] = server_id
    query = urllib.parse.urlencode(params)
    return f"{base_url.rstrip('/')}/web/index.html#!/item?{query}"


def _public_image_url(image_ref: Optional[str]) -> str:
    value = str(image_ref or "").strip()
    if not value:
        return ""
    lower = value.lower()
    if lower.startswith("http://") or lower.startswith("https://"):
        return value
    return f"/{value.lstrip('/')}"


def _render_web_ui(
    reports: list[MissingReport],
    summary: ScanSummary,
    output_dir: str,
    language: str = "en-US",
    logger: Optional[logging.Logger] = None,
) -> str:
    lang = _normalize_language(language)
    text_en = {
        "details_suffix": "Missing Episodes",
        "no_overview": "No overview available.",
        "unknown_air_date": "Unknown",
        "no_image": "No Image",
        "open_emby": "Open Emby",
        "open_tmdb": "Open TMDB",
        "air_date": "Air Date",
        "back": "Back to Missing List",
        "no_details": "No missing episode details found.",
        "missing_season": "Missing season",
        "missing_in": "missing",
        "index_title": "Emby Missing Scanner",
        "headline": "Missing Seasons / Episodes",
        "library": "Library",
        "processed": "Processed",
        "only_missing": "Only series with missing content are shown.",
        "no_poster": "No Poster",
        "missing_items": "Missing items",
        "empty": "No missing items found.",
        "emby_id": "Emby ID",
        "tmdb_id": "TMDB ID",
    }
    text_zh = {
        "details_suffix": "缺失季集详情",
        "no_overview": "暂无简介。",
        "unknown_air_date": "未知",
        "no_image": "无图片",
        "open_emby": "打开 Emby",
        "open_tmdb": "打开 TMDB",
        "air_date": "播出日期",
        "back": "返回缺失列表",
        "no_details": "未找到缺失集详情。",
        "missing_season": "缺失整季",
        "missing_in": "缺失",
        "index_title": "Emby 缺失扫描",
        "headline": "缺失季 / 缺失集",
        "library": "媒体库",
        "processed": "扫描进度",
        "only_missing": "仅展示存在缺失的作品。",
        "no_poster": "无海报",
        "missing_items": "缺失项",
        "empty": "没有发现缺失项。",
        "emby_id": "Emby ID",
        "tmdb_id": "TMDB ID",
    }
    t = text_zh if lang == "zh-CN" else text_en

    os.makedirs(output_dir, exist_ok=True)
    series_dir = os.path.join(output_dir, "series")
    os.makedirs(series_dir, exist_ok=True)

    sorted_reports = sorted(reports, key=lambda item: (_normalize_name(item.series_name), item.emby_series_id))
    series_links: list[tuple[MissingReport, str, str]] = []

    for report in sorted_reports:
        base_slug = f"{_safe_slug(report.series_name)}-{report.emby_series_id}"
        file_name = f"{base_slug}.html"
        relative_path = f"/series/{file_name}"
        local_path = os.path.join(series_dir, file_name)

        episode_cards: list[str] = []
        for detail in sorted(report.missing_episode_details, key=lambda d: (d.season_number, d.episode_number)):
            image_url = _public_image_url(detail.image_relpath)
            overview = html.escape(detail.overview.strip() or t["no_overview"])
            title = html.escape(detail.title or f"S{detail.season_number}E{detail.episode_number}")
            air_date = html.escape(detail.air_date or t["unknown_air_date"])
            episode_cards.append(
                f"""
                <article class="episode-card">
                  <div class="image-wrap">
                    {'<img src="' + html.escape(image_url) + '" alt="' + title + '">' if image_url else '<div class="image-fallback">' + html.escape(t["no_image"]) + '</div>'}
                    <div class="overlay">
                      <a class="btn" target="_blank" href="{html.escape(detail.emby_url)}">{html.escape(t["open_emby"])}</a>
                      <a class="btn secondary" target="_blank" href="{html.escape(detail.tmdb_url)}">{html.escape(t["open_tmdb"])}</a>
                    </div>
                  </div>
                  <div class="episode-meta">
                    <h3>S{detail.season_number:02d}E{detail.episode_number:02d} · {title}</h3>
                    <p class="air-date">{html.escape(t["air_date"])}: {air_date}</p>
                    <p>{overview}</p>
                  </div>
                </article>
                """
            )

        season_tags = []
        for season in sorted(report.missing_seasons):
            season_tags.append(f"<span class='tag warn'>{html.escape(t['missing_season'])} S{season}</span>")
        for season, episodes in sorted(report.missing_episodes.items()):
            ep_text = ", ".join(str(ep) for ep in episodes)
            season_tags.append(f"<span class='tag'>S{season} {html.escape(t['missing_in'])}: {html.escape(ep_text)}</span>")

        details_html = f"""<!doctype html>
<html lang="{html.escape(lang)}">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{html.escape(report.series_name)} - {html.escape(t["details_suffix"])}</title>
  <style>
    :root {{
      --bg:#0b1016;
      --panel:#121b25;
      --panel-soft:#1b2734;
      --text:#dce7f3;
      --muted:#9fb1c5;
      --accent:#4cc2ff;
      --warn:#ffb347;
    }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; background:linear-gradient(180deg,#0b1016,#0f1721); color:var(--text); font-family:Segoe UI,Helvetica,Arial,sans-serif; }}
    .container {{ max-width:1400px; margin:0 auto; padding:24px; }}
    .topbar {{ display:flex; gap:12px; align-items:center; margin-bottom:16px; }}
    .back {{ color:var(--accent); text-decoration:none; }}
    .title {{ margin:0; font-size:28px; }}
    .subtitle {{ color:var(--muted); margin:6px 0 0; }}
    .tags {{ display:flex; flex-wrap:wrap; gap:8px; margin:14px 0 22px; }}
    .tag {{ background:var(--panel-soft); border:1px solid #2a3a4e; border-radius:999px; padding:6px 10px; font-size:12px; }}
    .tag.warn {{ border-color:#6b4a1f; color:#ffd49a; }}
    .grid {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(260px,1fr)); gap:18px; }}
    .episode-card {{ background:var(--panel); border:1px solid #233142; border-radius:14px; overflow:hidden; }}
    .image-wrap {{ position:relative; aspect-ratio:16/9; background:#162332; }}
    .image-wrap img {{ width:100%; height:100%; object-fit:cover; display:block; }}
    .image-fallback {{ width:100%; height:100%; display:grid; place-items:center; color:#9fb1c5; }}
    .overlay {{ position:absolute; inset:auto 10px 10px 10px; display:flex; gap:8px; opacity:0; transition:opacity .2s ease; }}
    .image-wrap:hover .overlay {{ opacity:1; }}
    .btn {{ flex:1; background:rgba(11,16,22,.88); color:#fff; text-decoration:none; text-align:center; border:1px solid #3f5b78; padding:8px; border-radius:8px; font-size:12px; }}
    .btn.secondary {{ border-color:#5f7389; }}
    .episode-meta {{ padding:12px; }}
    .episode-meta h3 {{ margin:0 0 6px; font-size:14px; }}
    .air-date {{ margin:0 0 8px; color:var(--muted); font-size:12px; }}
    .episode-meta p {{ margin:0; color:#c5d5e6; font-size:13px; line-height:1.4; }}
  </style>
</head>
<body>
  <div class="container">
    <div class="topbar"><a class="back" href="/">← {html.escape(t["back"])}</a></div>
    <h1 class="title">{html.escape(report.series_name)}</h1>
    <p class="subtitle">{html.escape(t["emby_id"])}: {html.escape(report.emby_series_id)} · {html.escape(t["tmdb_id"])}: {report.tmdb_tv_id}</p>
    <div class="tags">{''.join(season_tags)}</div>
    <div class="grid">{''.join(episode_cards) or '<p>' + html.escape(t["no_details"]) + '</p>'}</div>
  </div>
</body>
</html>"""
        with open(local_path, "w", encoding="utf-8-sig") as page_file:
            page_file.write(details_html)
        series_links.append((report, relative_path, local_path))

    poster_cards = []
    for report, page_url, _ in series_links:
        image_url = _public_image_url(report.series_poster_relpath)
        missing_count = len(report.missing_episode_details)
        poster_cards.append(
            f"""
            <a class="poster-card" href="{html.escape(page_url)}">
              <div class="poster-wrap">
                {'<img src="' + html.escape(image_url) + '" alt="' + html.escape(report.series_name) + '">' if image_url else '<div class="image-fallback">' + html.escape(t["no_poster"]) + '</div>'}
              </div>
              <div class="poster-meta">
                <h2>{html.escape(report.series_name)}</h2>
                <p>{html.escape(t["missing_items"])}: {missing_count}</p>
              </div>
            </a>
            """
        )

    index_html = f"""<!doctype html>
<html lang="{html.escape(lang)}">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{html.escape(t["index_title"])}</title>
  <style>
    :root {{
      --bg:#0b1016;
      --panel:#121b25;
      --line:#233142;
      --text:#dce7f3;
      --muted:#9fb1c5;
      --accent:#52c7ff;
    }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; color:var(--text); background:radial-gradient(circle at top,#122232 0,#0b1016 45%); font-family:Segoe UI,Helvetica,Arial,sans-serif; }}
    .container {{ max-width:1500px; margin:0 auto; padding:24px; }}
    .headline {{ display:flex; justify-content:space-between; align-items:flex-end; gap:16px; margin-bottom:22px; }}
    h1 {{ margin:0; font-size:30px; }}
    .meta {{ color:var(--muted); font-size:14px; }}
    .grid {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(200px,1fr)); gap:16px; }}
    .poster-card {{ display:block; background:var(--panel); border:1px solid var(--line); border-radius:14px; overflow:hidden; text-decoration:none; color:inherit; transition:transform .18s ease,border-color .18s ease; }}
    .poster-card:hover {{ transform:translateY(-3px); border-color:#37506b; }}
    .poster-wrap {{ aspect-ratio:2/3; background:#162332; }}
    .poster-wrap img {{ width:100%; height:100%; object-fit:cover; display:block; }}
    .image-fallback {{ width:100%; height:100%; display:grid; place-items:center; color:#9fb1c5; }}
    .poster-meta {{ padding:10px; }}
    .poster-meta h2 {{ margin:0; font-size:14px; line-height:1.35; }}
    .poster-meta p {{ margin:6px 0 0; color:var(--muted); font-size:12px; }}
    .empty {{ margin-top:40px; color:var(--muted); }}
  </style>
</head>
<body>
  <div class="container">
    <div class="headline">
      <div>
        <h1>{html.escape(t["headline"])}</h1>
        <div class="meta">{html.escape(t["library"])}: {html.escape(summary.library_name)} · {html.escape(t["processed"])} {summary.processed_series}/{summary.target_series_to_scan}</div>
      </div>
      <div class="meta">{html.escape(t["only_missing"])}</div>
    </div>
    <div class="grid">{''.join(poster_cards) or '<p class="empty">' + html.escape(t["empty"]) + '</p>'}</div>
  </div>
</body>
</html>"""

    index_path = os.path.join(output_dir, "index.html")
    with open(index_path, "w", encoding="utf-8-sig") as index_file:
        index_file.write(index_html)
    if logger:
        logger.info("Web UI generated: %s", index_path)
    return index_path


def _serve_web_directory(host: str, port: int, directory: str) -> None:
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=directory)
    server = http.server.ThreadingHTTPServer((host, port), handler)
    print(f"Web UI running at http://{host}:{port}/ (Press Ctrl+C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scan an Emby library and list series with missing seasons or episodes (using TMDB as source)."
    )
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        help=f"Config JSON path (default: {DEFAULT_CONFIG_PATH}).",
    )
    parser.add_argument("--timeout", type=float, default=None, help="Override timeout from config.")
    parser.add_argument("--max-series", type=int, default=None, help="Override max_series from config.")
    parser.add_argument(
        "--max-lookup-errors",
        type=int,
        default=None,
        help="Override max_lookup_errors from config.",
    )
    parser.add_argument(
        "--include-specials",
        action="store_true",
        help="Override include_specials from config to true.",
    )
    parser.add_argument(
        "--log-file",
        default=None,
        help="Override log_file from config. Use empty string to disable file logging.",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Override show_progress from config to false.",
    )
    parser.add_argument(
        "--web-port",
        type=int,
        default=None,
        help="Override web_port from config.",
    )
    parser.add_argument(
        "--no-web",
        action="store_true",
        help="Disable web UI server for this run.",
    )
    parser.set_defaults(include_specials=None)
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    config = load_config(args.config)
    cache_dir = str(config.get("cache_dir", r"C:\Users\Public\emby_scan"))
    os.makedirs(cache_dir, exist_ok=True)
    language = _normalize_language(config.get("language", _detect_system_language()))

    timeout = float(args.timeout if args.timeout is not None else config.get("timeout", 15.0))
    max_series = args.max_series if args.max_series is not None else config.get("max_series")
    max_lookup_errors = (
        args.max_lookup_errors if args.max_lookup_errors is not None else config.get("max_lookup_errors")
    )
    include_specials = bool(
        args.include_specials if args.include_specials is not None else config.get("include_specials", False)
    )
    show_progress = bool(config.get("show_progress", True))
    if args.no_progress:
        show_progress = False
    log_file = args.log_file if args.log_file is not None else str(config.get("log_file", "emby_scan.log"))
    log_file = log_file.strip()
    web_enabled = bool(config.get("web_enabled", True)) and not args.no_web
    web_host = str(config.get("web_host", "127.0.0.1"))
    web_port = int(args.web_port if args.web_port is not None else config.get("web_port", 8765))

    logger = _configure_logger(log_file or None)
    progress_bar = ProgressBar(sys.stderr, enabled=show_progress)
    emby_client = EmbyClient(
        str(config.get("emby_url", DEFAULT_EMBY_URL)),
        str(config.get("emby_api_key", DEFAULT_EMBY_API_KEY)),
        timeout=timeout,
    )
    tmdb_client = TmdbClient(
        str(config.get("tmdb_bearer", DEFAULT_TMDB_BEARER)),
        str(config.get("tmdb_api_key", DEFAULT_TMDB_API_KEY)),
        language=language,
        timeout=timeout,
        max_retries=int(config.get("tmdb_max_retries", 4)),
        retry_delay=float(config.get("tmdb_retry_delay", 0.8)),
        cache_dir=cache_dir,
        api_cache_ttl_hours=float(
            config.get("tmdb_api_cache_ttl_hours", config.get("tmdb_cache_ttl_hours", 1))
        ),
        image_cache_ttl_hours=float(config.get("tmdb_image_cache_ttl_hours", 99999)),
        cache_images=bool(config.get("cache_images", True)),
        logger=logger,
    )

    try:
        reports, unmatched_series, lookup_errors, summary = scan_missing_content(
            emby_client=emby_client,
            tmdb_client=tmdb_client,
            library_name=str(config.get("library_name", DEFAULT_LIBRARY_NAME)),
            include_specials=include_specials,
            include_unaired=bool(config.get("include_unaired", False)),
            max_series=_as_int(max_series),
            max_lookup_errors=_as_int(max_lookup_errors),
            skip_series_names=[
                str(item) for item in (config.get("skip_series_names") or []) if isinstance(item, (str, int))
            ],
            skip_series_ids=[
                str(item) for item in (config.get("skip_series_ids") or []) if isinstance(item, (str, int))
            ],
            concurrency_workers=max(1, _as_int(config.get("concurrency_workers")) or 1),
            emby_web_base_url=str(config.get("emby_url", DEFAULT_EMBY_URL)),
            logger=logger,
            progress_callback=progress_bar.update,
        )
    except RuntimeError as exc:
        progress_bar.finish()
        print(f"Scan failed: {exc}", file=sys.stderr)
        return 1

    progress_bar.finish()
    print_report(
        reports,
        unmatched_series,
        lookup_errors,
        summary=summary,
        log_file=(log_file or None),
    )

    if web_enabled:
        index_path = _render_web_ui(reports, summary, cache_dir, language=language, logger=logger)
        print(f"Web index generated: {index_path}")
        _serve_web_directory(web_host, web_port, cache_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
