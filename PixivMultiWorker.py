#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import concurrent.futures
import configparser
import json
import os
import random
import re
import signal
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass
from typing import Iterable, List, Optional, Set, Tuple
from urllib.parse import urlparse

from colorama import Fore, Style
import requests

from PixivDBManager import PixivDBManager
from common.PixivAppApi import PixivAppApiClient, PixivAppApiError
from common.RequestsProxyAdapter import mount_proxy_host_header_adapter


@dataclass
class WorkerSettings:
    refresh_token: str
    use_proxy: bool
    proxy_address: str
    timeout_sec: float
    verify_ssl: bool
    delay_sec: float
    db_path: str
    mode: str
    download_kind: str
    download_concurrency: int
    download_root: str


_stop_requested = False
_db: Optional[PixivDBManager] = None
_api_client: Optional[PixivAppApiClient] = None
_api_client_key: Optional[Tuple[str, str, bool, float]] = None
_cli_mode_default = ""

_worker_id = ""
_worker_config_path = ""
_last_reported_refresh_token = ""
_last_reported_proxy_fail_at = 0.0

_download_session_local = threading.local()
_DOWNLOAD_UA = "PixivAndroidApp/5.0.234 (Android 11; Pixel 5)"


def _log(level: str, message: str) -> None:
    print(f"[{level}] {message}", flush=True)


def _atomic_write_text(path: str, text: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8", newline="\n") as fp:
        fp.write(text)
    os.replace(tmp, path)


def _atomic_write_json(path: str, payload: dict) -> None:
    _atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def _redact(text: str) -> str:
    value = str(text or "")
    return re.sub(r"//[^/@\s:]+:[^@\s]+@", "//***:***@", value)


def _runtime_dir_from_worker_config(config_path: str) -> str:
    cfg_dir = os.path.dirname(os.path.abspath(config_path))
    return os.path.abspath(os.path.join(cfg_dir, os.pardir))


def _events_dir() -> str:
    base = _runtime_dir_from_worker_config(_worker_config_path or ".")
    path = os.path.join(base, "events")
    os.makedirs(path, exist_ok=True)
    return path


def _workers_dir() -> str:
    base = _runtime_dir_from_worker_config(_worker_config_path or ".")
    path = os.path.join(base, "workers")
    os.makedirs(path, exist_ok=True)
    return path


def _progress_path(worker_id: str) -> str:
    wid = (worker_id or "worker").strip()
    safe_id = "".join(ch for ch in wid if ch.isalnum() or ch in "._-")[:64] or "worker"
    return os.path.join(_workers_dir(), f"{safe_id}.json")


def _report_rotated_refresh_token(new_refresh_token: str) -> None:
    global _last_reported_refresh_token

    token = (new_refresh_token or "").strip()
    if not token or token == _last_reported_refresh_token:
        return

    safe_id = "".join(ch for ch in (_worker_id or "worker") if ch.isalnum() or ch in "._-")[:64] or "worker"
    path = os.path.join(_events_dir(), f"refresh_token_{safe_id}.txt")
    _atomic_write_text(path, token + "\n")
    _last_reported_refresh_token = token


def _is_proxy_failure(ex: BaseException, *, use_proxy: bool) -> bool:
    if not use_proxy:
        return False

    if isinstance(ex, PixivAppApiError):
        status = int(ex.status_code or 0)
        if status == 407:
            return True
        body = str(ex.body or "").lower()
        if "proxy" in body and ("407" in body or "authentication" in body or "connect" in body):
            return True
        return False

    msg = str(ex).lower()
    return "proxy" in msg and ("407" in msg or "authentication" in msg or "connect" in msg)


def _report_proxy_failure(ex: BaseException, *, context: str, proxy: str) -> None:
    global _last_reported_proxy_fail_at

    now = time.time()
    if _last_reported_proxy_fail_at and (now - _last_reported_proxy_fail_at) < 20:
        return

    status_code = None
    body = ""
    if isinstance(ex, PixivAppApiError):
        status_code = ex.status_code
        body = ex.body or ""

    safe_id = "".join(ch for ch in (_worker_id or "worker") if ch.isalnum() or ch in "._-")[:64] or "worker"
    path = os.path.join(_events_dir(), f"proxy_fail_{safe_id}.json")

    payload = {
        "worker_id": _worker_id,
        "ts": int(now),
        "context": context,
        "proxy": proxy,
        "error": _redact(str(ex)),
        "status_code": status_code,
        "body": _redact(body),
    }
    _atomic_write_json(path, payload)
    _last_reported_proxy_fail_at = now


def _is_rate_limited(ex: PixivAppApiError) -> bool:
    status = int(ex.status_code or 0)
    if status in {429}:
        return True
    body = str(ex.body or "").lower()
    return "rate" in body and ("limit" in body or "too many" in body)


def _rate_limit_backoff_seconds(attempt: int) -> float:
    step = max(1, int(attempt))
    base = 20.0 * (2 ** (step - 1))
    base = min(base, 6 * 60.0)
    jitter = random.random() * (base * 0.15)
    return base + jitter


def _request_stop(*_args) -> None:
    global _stop_requested
    _stop_requested = True


def _install_signal_handlers() -> None:
    try:
        signal.signal(signal.SIGTERM, _request_stop)
    except Exception:
        pass
    try:
        signal.signal(signal.SIGINT, _request_stop)
    except Exception:
        pass


def _as_bool(raw: str, default: bool = False) -> bool:
    text = str(raw or "").strip().lower()
    if not text:
        return bool(default)
    return text in {"1", "y", "yes", "true", "on"}


def _load_settings(config_path: str) -> WorkerSettings:
    parser = configparser.RawConfigParser()
    parser.optionxform = lambda option: option
    with open(config_path, "r", encoding="utf-8") as fp:
        parser.read_file(fp)

    def getv(section: str, key: str, default: str = "") -> str:
        try:
            if parser.has_section(section) and parser.has_option(section, key):
                return str(parser.get(section, key) or "")
        except Exception:
            return default
        return default

    refresh_token = getv("Authentication", "refresh_token", "").strip()
    use_proxy = _as_bool(getv("Network", "useProxy", "False"), False)
    proxy_addr = getv("Network", "proxyAddress", "").strip()
    timeout_raw = getv("Network", "timeout", "30").strip()
    verify_raw = getv("Network", "enableSSLVerification", "True").strip()
    delay_raw = getv("Network", "downloadDelay", "8").strip()
    db_path = getv("Settings", "dbPath", "db.multi.sqlite").strip() or "db.multi.sqlite"
    mode_raw = getv("Settings", "mode", "").strip().lower()
    download_kind_raw = getv("Settings", "downloadKind", "original").strip().lower()
    download_conc_raw = getv("Settings", "downloadConcurrency", "4").strip()
    download_root_raw = getv("Settings", "downloadRoot", "").strip()

    try:
        timeout_sec = float(timeout_raw)
    except Exception:
        timeout_sec = 30.0
    timeout_sec = max(3.0, min(timeout_sec, 300.0))

    try:
        delay_sec = float(delay_raw)
    except Exception:
        delay_sec = 8.0
    delay_sec = max(0.0, min(delay_sec, 120.0))

    verify_ssl = _as_bool(verify_raw, True)

    cfg_dir = os.path.dirname(os.path.abspath(config_path))
    if not os.path.isabs(db_path):
        db_path = os.path.abspath(os.path.join(cfg_dir, db_path))

    mode = mode_raw or str(_cli_mode_default or "").strip().lower() or "index_urls"
    if mode not in {"index_urls", "download_images"}:
        mode = "index_urls"

    download_kind = download_kind_raw or "original"
    if download_kind not in {"original", "regular"}:
        download_kind = "original"

    try:
        download_concurrency = int(download_conc_raw)
    except Exception:
        download_concurrency = 4
    download_concurrency = max(1, min(download_concurrency, 32))

    runtime_dir = _runtime_dir_from_worker_config(config_path)
    download_root = download_root_raw
    if not download_root:
        download_root = os.path.join(runtime_dir, "downloads")
    if not os.path.isabs(download_root):
        download_root = os.path.abspath(os.path.join(runtime_dir, download_root))

    return WorkerSettings(
        refresh_token=refresh_token,
        use_proxy=bool(use_proxy and proxy_addr),
        proxy_address=proxy_addr,
        timeout_sec=timeout_sec,
        verify_ssl=verify_ssl,
        delay_sec=delay_sec,
        db_path=db_path,
        mode=mode,
        download_kind=download_kind,
        download_concurrency=download_concurrency,
        download_root=download_root,
    )


def _download_session(settings: WorkerSettings) -> requests.Session:
    """
    Thread-local requests session for image downloads.
    """
    key = (
        settings.proxy_address.strip() if settings.use_proxy else "",
        bool(settings.verify_ssl),
    )
    sess = getattr(_download_session_local, "session", None)
    sess_key = getattr(_download_session_local, "key", None)
    if sess is None or sess_key != key:
        sess = requests.Session()
        mount_proxy_host_header_adapter(sess, enabled=True)
        _download_session_local.session = sess
        _download_session_local.key = key
    return sess


def _url_ext(url: str) -> str:
    try:
        path = urlparse(str(url or "")).path or ""
        _, ext = os.path.splitext(path)
        ext = str(ext or "").strip().lower()
        if ext and len(ext) <= 10:
            return ext
    except Exception:
        pass
    return ".jpg"


def _download_dest_path(*, settings: WorkerSettings, member_id: int, image_id: int, page_index: int, url: str) -> str:
    ext = _url_ext(url)
    base = settings.download_root
    kind = settings.download_kind
    directory = os.path.join(base, kind, str(int(member_id)))
    filename = f"{int(image_id)}_p{int(page_index)}{ext}"
    return os.path.join(directory, filename)


def _file_ok(path: str) -> bool:
    try:
        return os.path.isfile(path) and os.path.getsize(path) > 0
    except Exception:
        return False


def _download_one(*, url: str, dest_path: str, referer: str, settings: WorkerSettings) -> bool:
    if _stop_requested:
        return False
    if _file_ok(dest_path):
        return True

    proxies = None
    if settings.use_proxy and settings.proxy_address.strip():
        proxies = {"http": settings.proxy_address.strip(), "https": settings.proxy_address.strip()}

    headers = {"User-Agent": _DOWNLOAD_UA, "Referer": str(referer or "https://www.pixiv.net/"), "Accept": "*/*"}
    sess = _download_session(settings)

    for attempt in range(1, 4):
        if _stop_requested:
            return False
        try:
            if settings.delay_sec > 0:
                time.sleep(random.random() * min(float(settings.delay_sec), 1.5))

            resp = sess.get(
                str(url or ""),
                headers=headers,
                proxies=proxies,
                verify=bool(settings.verify_ssl),
                timeout=float(settings.timeout_sec),
                stream=True,
                allow_redirects=True,
            )
        except requests.RequestException as ex:
            if _is_proxy_failure(ex, use_proxy=bool(settings.use_proxy)):
                raise
            time.sleep(min(2.0 * (2 ** (attempt - 1)), 10.0))
            continue

        status = int(getattr(resp, "status_code", -1))
        if status == 200:
            os.makedirs(os.path.dirname(dest_path) or ".", exist_ok=True)
            tmp = f"{dest_path}.tmp"
            try:
                try:
                    with open(tmp, "wb") as fp:
                        for chunk in resp.iter_content(chunk_size=256 * 1024):
                            if _stop_requested:
                                break
                            if not chunk:
                                continue
                            fp.write(chunk)
                except Exception as ex:  # noqa: BLE001
                    try:
                        os.remove(tmp)
                    except Exception:
                        pass
                    if _is_proxy_failure(ex, use_proxy=bool(settings.use_proxy)):
                        raise
                    time.sleep(min(2.0 * (2 ** (attempt - 1)), 10.0))
                    continue

                if _stop_requested:
                    try:
                        os.remove(tmp)
                    except Exception:
                        pass
                    return False
                if not _file_ok(tmp):
                    try:
                        os.remove(tmp)
                    except Exception:
                        pass
                    return False
                os.replace(tmp, dest_path)
                return True
            finally:
                try:
                    resp.close()
                except Exception:
                    pass

        text = ""
        try:
            if status in {403, 404, 407}:
                text = (resp.text or "")[:200]
        except Exception:
            text = ""
        try:
            resp.close()
        except Exception:
            pass

        if status == 407:
            raise PixivAppApiError("Proxy auth required", status_code=407, body=text)
        if status in {429, 500, 502, 503, 504}:
            time.sleep(min(2.0 * (2 ** (attempt - 1)), 30.0))
            continue
        return False

    return False


def _ensure_db(db_path: str) -> PixivDBManager:
    global _db

    if _db is not None and os.path.abspath(_db.db_path) == os.path.abspath(db_path):
        return _db

    if _db is not None:
        try:
            _db.close()
        except Exception:
            pass

    _db = PixivDBManager(root_directory=".", target=db_path, timeout=10)

    last_exc = None
    for attempt in range(0, 8):
        try:
            _db.createDatabase()
            last_exc = None
            break
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if isinstance(exc, sqlite3.OperationalError):
                msg = str(exc).lower()
                if "locked" in msg:
                    time.sleep(0.05 * (2 ** attempt))
                    continue
            raise
    if last_exc is not None:
        raise last_exc
    return _db


def _ensure_api(settings: WorkerSettings) -> PixivAppApiClient:
    global _api_client, _api_client_key

    token = settings.refresh_token.strip()
    if not token:
        raise ValueError("refresh_token is empty")

    proxy = settings.proxy_address.strip() if settings.use_proxy else ""
    key = (token, proxy, bool(settings.verify_ssl), float(settings.timeout_sec))
    if _api_client is not None and _api_client_key == key:
        return _api_client

    proxies = None
    if proxy:
        proxies = {"http": proxy, "https": proxy}

    _api_client = PixivAppApiClient(
        refresh_token=token,
        proxies=proxies,
        verify_ssl=bool(settings.verify_ssl),
        timeout_sec=float(settings.timeout_sec),
        on_refresh_token_rotated=_report_rotated_refresh_token,
    )
    _api_client_key = key
    return _api_client


def _read_assigned_member_ids(assign_path: str) -> List[int]:
    if not os.path.isfile(assign_path):
        return []
    out: List[int] = []
    with open(assign_path, "r", encoding="utf-8", errors="ignore") as fp:
        for raw in fp:
            s = (raw or "").strip()
            if not s:
                continue
            try:
                out.append(int(s))
            except Exception:
                continue
    return sorted(set(out))


def _index_member_urls(member_id: int, *, prefix: str, settings: WorkerSettings) -> None:
    db = _ensure_db(settings.db_path)
    client = _ensure_api(settings)

    try:
        client.get_access_token()
    except Exception as ex:  # noqa: BLE001
        if _is_proxy_failure(ex, use_proxy=settings.use_proxy):
            _report_proxy_failure(ex, context="oauth_refresh", proxy=settings.proxy_address)
        raise

    try:
        detail = client.fetch_user_detail(user_id=int(member_id))
        info = client.parse_user_detail(detail)
        db.upsertFollowMember(
            member_id=int(info.get("member_id") or member_id),
            name=info.get("name") or None,
            member_token=info.get("member_token") or None,
            avatar_url=info.get("avatar_url") or None,
            background_url=info.get("background_url") or None,
        )
    except Exception as ex:  # noqa: BLE001
        _log("warn", f"{prefix}member detail failed: {ex}")

    try:
        current_ids = list(client.iter_user_illust_ids(user_id=int(member_id), page_sleep_sec=0.0))
    except PixivAppApiError as ex:
        _log("error", f"{prefix}list illusts failed: {ex} (HTTP {ex.status_code})")
        if _is_proxy_failure(ex, use_proxy=settings.use_proxy):
            _report_proxy_failure(ex, context="user_illusts", proxy=settings.proxy_address)
        raise

    current_set = set(int(x) for x in current_ids)
    stored_ids = set(db.selectFollowImageIdsByMember(int(member_id)))

    to_add = sorted(current_set - stored_ids, reverse=True)
    to_remove = sorted(stored_ids - current_set)
    if to_remove:
        _log("info", f"{prefix}remove deleted works: {len(to_remove)}")
        for image_id in to_remove:
            db.deleteFollowImage(int(image_id))

    try:
        to_fix = db.selectFollowImageIdsNeedingUrlIndex(int(member_id))
    except Exception:
        to_fix = []
    to_fix = [int(x) for x in (to_fix or []) if int(x) in current_set]

    to_process: List[int] = list(to_add)
    for image_id in to_fix:
        if image_id not in to_process:
            to_process.append(int(image_id))

    total = len(to_process)
    _log("info", f"{prefix}member {member_id} to_process={total} new={len(to_add)} repair={len(to_fix)}")

    consecutive_proxy_errors = 0
    for idx, illust_id in enumerate(to_process, start=1):
        if _stop_requested:
            break
        if idx == 1 or idx == total or idx % 10 == 0:
            _log("info", f"{prefix}progress {idx}/{total} illust_id={illust_id}")

        try:
            detail = client.fetch_illust_detail(illust_id=int(illust_id))
            parsed = client.parse_illust_detail(detail)

            db.upsertFollowImage(
                image_id=int(parsed.get("image_id") or illust_id),
                member_id=int(member_id),
                title=parsed.get("title") or None,
                caption=parsed.get("caption") or None,
                create_date=parsed.get("create_date") or None,
                page_count=int(parsed.get("page_count") or 1),
                mode=parsed.get("mode") or None,
                bookmark_count=parsed.get("bookmark_count"),
                like_count=None,
                view_count=parsed.get("view_count"),
            )
            url_rows = parsed.get("url_rows") or []
            if url_rows:
                db.upsertFollowImageUrls(int(parsed.get("image_id") or illust_id), url_rows)
            consecutive_proxy_errors = 0
        except PixivAppApiError as ex:
            if _is_rate_limited(ex):
                raise
            if _is_proxy_failure(ex, use_proxy=settings.use_proxy):
                consecutive_proxy_errors += 1
                _report_proxy_failure(ex, context=f"illust_detail:{illust_id}", proxy=settings.proxy_address)
                if consecutive_proxy_errors >= 3:
                    raise
            else:
                consecutive_proxy_errors = 0
            _log("warn", f"{prefix}skip illust_id={illust_id}: {ex} (HTTP {ex.status_code})")
            continue
        except Exception as ex:  # noqa: BLE001
            consecutive_proxy_errors = 0
            _log("warn", f"{prefix}skip illust_id={illust_id}: {ex}")
            continue

        if settings.delay_sec > 0:
            time.sleep(random.random() * settings.delay_sec)

    if str(settings.mode).strip().lower() == "download_images" and not _stop_requested:
        downloaded = 0
        skipped = 0
        failed = 0
        proxy_failure: Optional[BaseException] = None

        rows = db.iterFollowImageUrlRowsByMember(int(member_id))
        max_workers = max(1, int(settings.download_concurrency or 1))
        in_flight_limit = max_workers * 3

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures: Set[concurrent.futures.Future] = set()
            for (image_id, page_index, ori_url, reg_url) in rows:
                if _stop_requested:
                    break
                url = ori_url if settings.download_kind == "original" else reg_url
                url = str(url or "").strip()
                if not url:
                    continue
                dest_path = _download_dest_path(
                    settings=settings,
                    member_id=int(member_id),
                    image_id=int(image_id),
                    page_index=int(page_index),
                    url=url,
                )
                if _file_ok(dest_path):
                    skipped += 1
                    continue

                while futures and len(futures) >= in_flight_limit:
                    done, futures = concurrent.futures.wait(
                        futures, return_when=concurrent.futures.FIRST_COMPLETED, timeout=1.0
                    )
                    for fut in done:
                        try:
                            ok = bool(fut.result())
                            if ok:
                                downloaded += 1
                            else:
                                failed += 1
                        except Exception as ex:  # noqa: BLE001
                            failed += 1
                            if proxy_failure is None and _is_proxy_failure(ex, use_proxy=settings.use_proxy):
                                proxy_failure = ex

                referer = f"https://www.pixiv.net/artworks/{int(image_id)}"
                futures.add(
                    executor.submit(
                        _download_one,
                        url=url,
                        dest_path=dest_path,
                        referer=referer,
                        settings=settings,
                    )
                )

            for fut in concurrent.futures.as_completed(list(futures)):
                try:
                    ok = bool(fut.result())
                    if ok:
                        downloaded += 1
                    else:
                        failed += 1
                except Exception as ex:  # noqa: BLE001
                    failed += 1
                    if proxy_failure is None and _is_proxy_failure(ex, use_proxy=settings.use_proxy):
                        proxy_failure = ex

        _log(
            "info",
            f"{prefix}download kind={settings.download_kind} downloaded={downloaded} skipped={skipped} failed={failed}",
        )
        if proxy_failure is not None:
            raise proxy_failure


def run_worker(*, worker_id: str, config_path: str, assign_path: str, poll_interval: float) -> int:
    global _worker_id, _worker_config_path

    _worker_id = str(worker_id or "").strip()
    _worker_config_path = str(config_path or "").strip()
    poll_sec = max(2.0, float(poll_interval or 10.0))

    processed: Set[int] = set()
    last_assign_snapshot: Set[int] = set()
    last_member_id = 0
    current_member_id = 0
    last_error = ""
    rate_limit_attempt = 0

    progress_file = _progress_path(_worker_id)

    def write_progress(*, force: bool = False) -> None:
        payload = {
            "worker_id": _worker_id,
            "updated_at": int(time.time()),
            "status": "running" if not _stop_requested else "stopping",
            "processed_count": int(len(processed)),
            "current_member_id": int(current_member_id or 0),
            "last_member_id": int(last_member_id or 0),
            "last_error": str(last_error or ""),
            "force": bool(force),
        }
        _atomic_write_json(progress_file, payload)

    write_progress(force=True)

    while not _stop_requested:
        try:
            settings = _load_settings(config_path)
        except Exception as ex:  # noqa: BLE001
            last_error = f"load config failed: {ex}"
            _log("error", f"[worker:{_worker_id}] {last_error}")
            write_progress(force=True)
            time.sleep(poll_sec)
            continue

        if not settings.refresh_token:
            last_error = "refresh_token is empty"
            _log("warn", f"[worker:{_worker_id}] {last_error}")
            write_progress(force=True)
            time.sleep(poll_sec)
            continue

        try:
            _ensure_db(settings.db_path)
            _ensure_api(settings)
        except Exception as ex:  # noqa: BLE001
            last_error = f"bootstrap failed: {ex}"
            _log("error", f"[worker:{_worker_id}] {last_error}")
            write_progress(force=True)
            time.sleep(poll_sec)
            continue

        assigned_ids = _read_assigned_member_ids(assign_path)
        assigned_set = set(int(x) for x in assigned_ids)
        if assigned_set != last_assign_snapshot:
            processed = set()
            last_assign_snapshot = assigned_set

        if not assigned_ids:
            current_member_id = 0
            last_error = ""
            write_progress()
            time.sleep(poll_sec)
            continue

        total = len(assigned_ids)
        for idx, member_id in enumerate([x for x in assigned_ids if x not in processed], start=1):
            if _stop_requested:
                break

            prefix = f"[worker:{_worker_id}] [{idx}/{total}] "
            current_member_id = int(member_id)
            try:
                _index_member_urls(int(member_id), prefix=prefix, settings=settings)
                processed.add(int(member_id))
                last_member_id = int(member_id)
                current_member_id = 0
                last_error = ""
                rate_limit_attempt = 0
                write_progress(force=True)
            except PixivAppApiError as ex:
                last_error = f"PixivAppApiError HTTP {ex.status_code}: {ex}"
                _log("error", f"{prefix}{last_error}")
                if _is_rate_limited(ex):
                    rate_limit_attempt += 1
                    wait_s = _rate_limit_backoff_seconds(rate_limit_attempt)
                    _log("warn", f"{prefix}rate limited, backoff {int(wait_s)}s")
                    write_progress(force=True)
                    time.sleep(min(wait_s, 6 * 60 * 60))
                    break
                if _is_proxy_failure(ex, use_proxy=settings.use_proxy):
                    _report_proxy_failure(ex, context="member", proxy=settings.proxy_address)
                    _log("warn", f"{prefix}proxy failure, waiting runner override")
                    write_progress(force=True)
                    time.sleep(max(2.0, poll_sec))
                    break
                write_progress(force=True)
            except Exception as ex:  # noqa: BLE001
                if _is_proxy_failure(ex, use_proxy=settings.use_proxy):
                    _report_proxy_failure(ex, context="member", proxy=settings.proxy_address)
                    _log("warn", f"{prefix}proxy failure, waiting runner override")
                    last_error = f"proxy failure: {ex}"
                    write_progress(force=True)
                    time.sleep(max(2.0, poll_sec))
                    break
                last_error = f"error: {ex}"
                _log("error", f"{prefix}{last_error}")
                write_progress(force=True)

        write_progress()
        time.sleep(poll_sec)

    try:
        if _db is not None:
            _db.close()
    except Exception:
        pass

    write_progress(force=True)
    return 0


def main() -> int:  # pragma: no cover
    _install_signal_handlers()

    parser = argparse.ArgumentParser(description="Follow-index worker")
    parser.add_argument("--worker-id", required=True, help="Worker id")
    parser.add_argument("--config", required=True, help="Worker config path")
    parser.add_argument("--assign", required=True, help="Assigned member list path")
    parser.add_argument("--poll", type=float, default=10.0, help="Hot-reload polling interval")
    parser.add_argument("--mode", default="index_urls", help="Default mode (overridden by ini Settings/mode)")
    args = parser.parse_args()

    global _cli_mode_default
    _cli_mode_default = str(args.mode or "").strip().lower()

    try:
        return run_worker(
            worker_id=str(args.worker_id),
            config_path=str(args.config),
            assign_path=str(args.assign),
            poll_interval=float(args.poll),
        )
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":  # pragma: no cover
    if not sys.version_info >= (3, 8):
        print("Require Python 3.8+")
        sys.exit(2)
    print(f"{Fore.CYAN}{Style.BRIGHT}FollowIndex Worker starting...{Style.RESET_ALL}")
    sys.exit(main())
