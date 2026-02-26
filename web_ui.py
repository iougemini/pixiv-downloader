#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Follow Index Web UI

仅提供一个目标能力：
- 多账号 + 代理池 + 多线程
- 拉取 Pixiv 关注作者作品 URL 并落库
- 在 Web 页面管理配置、启动/停止 Runner、导出 URL

运行：
  python web_ui.py
访问：
  http://127.0.0.1:5000/
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from collections import deque
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse
from uuid import uuid4

from flask import Flask, Response, jsonify, render_template, request

from PixivDBManager import PixivDBManager
from common.EasyProxiesClient import EasyProxiesClient, EasyProxiesConfig
from common.ProxyTester import rank_proxies
from common.ProxyUtils import ProxyParts, mask_proxy_url, normalize_proxy_url, parse_proxy_url


ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
WEBUI_DIR = os.path.join(ROOT_DIR, "webui")
MULTI_CONFIG_PATH = os.path.join(ROOT_DIR, "multi_config.json")
MULTI_DB_FILENAME = "db.multi.sqlite"
LOG_LIMIT = 2000


app = Flask(
    __name__,
    template_folder=os.path.join(WEBUI_DIR, "templates"),
    static_folder=os.path.join(WEBUI_DIR, "static"),
)
app.config["JSON_AS_ASCII"] = False


_jobs: Dict[str, Dict[str, Any]] = {}
_jobs_lock = threading.Lock()
_multi_lock = threading.Lock()
_multi_runner_job_id: Optional[str] = None


def _now_i() -> int:
    return int(time.time())


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return bool(default)
    text = str(value).strip().lower()
    if not text:
        return bool(default)
    return text in {"1", "y", "yes", "true", "on"}


def _as_int(value: Any, default: int, *, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    try:
        out = int(value)
    except Exception:
        out = int(default)
    if lo is not None:
        out = max(lo, out)
    if hi is not None:
        out = min(hi, out)
    return out


def _atomic_write_text(path: str, text: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8", newline="\n") as fp:
        fp.write(text)
    os.replace(tmp, path)


def _read_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fp:
        return dict(json.load(fp) or {})


def _merge_defaults(raw: Dict[str, Any], defaults: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(defaults)
    for key, value in (raw or {}).items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _merge_defaults(value, out[key])
        else:
            out[key] = value
    return out


def _default_multi_config() -> Dict[str, Any]:
    return {
        "runtime_dir": ".multi_runtime",
        "accounts": [],
        "follow": {
            "bookmark_flag": "n",
            "lang": "en",
            "refresh_interval_sec": 1800,
            "timeout_sec": 20,
        },
        "proxy_pool": {
            "source": "easy_proxies",
            "refresh_interval_sec": 300,
            "max_tokens_per_proxy": 2,
            "bindings_strict": False,
            "easy_proxies": {
                "base_url": "http://127.0.0.1:9090",
                "host_override": "",
                "password": "",
                "timeout_sec": 10,
                "verify_ssl": True,
            },
            "test": {
                "target_url": "https://www.pixiv.net/robots.txt",
                "timeout_sec": 8,
                "concurrency": 20,
                "attempts": 2,
                "top_n": 20,
            },
            "proxies": [],
            "proxies_file": "",
        },
        "worker": {
            "poll_interval_sec": 10,
            "mode": "index_urls",
            "download_kind": "original",
            "download_concurrency": 4,
            "download_root": "",
        },
        "_ui_nonce": 0,
    }


def _load_multi_config_raw() -> Dict[str, Any]:
    defaults = _default_multi_config()
    if not os.path.isfile(MULTI_CONFIG_PATH):
        _atomic_write_text(MULTI_CONFIG_PATH, json.dumps(defaults, ensure_ascii=False, indent=2) + "\n")
        return defaults
    try:
        raw = _read_json(MULTI_CONFIG_PATH)
    except Exception:
        raw = {}
    return _merge_defaults(raw, defaults)


def _save_multi_config_raw(raw: Dict[str, Any]) -> Dict[str, Any]:
    merged = _merge_defaults(raw or {}, _default_multi_config())
    merged["worker"] = merged.get("worker") or {}
    worker = merged["worker"]

    mode = str(worker.get("mode") or "index_urls").strip().lower()
    if mode not in {"index_urls", "download_images"}:
        mode = "index_urls"
    worker["mode"] = mode

    kind = str(worker.get("download_kind") or "original").strip().lower()
    if kind not in {"original", "regular"}:
        kind = "original"
    worker["download_kind"] = kind

    try:
        concurrency = int(worker.get("download_concurrency") or 4)
    except Exception:
        concurrency = 4
    worker["download_concurrency"] = max(1, min(32, concurrency))

    worker["download_root"] = str(worker.get("download_root") or "").strip()
    _atomic_write_text(MULTI_CONFIG_PATH, json.dumps(merged, ensure_ascii=False, indent=2) + "\n")
    return merged


def _serialize_multi_config(raw: Dict[str, Any]) -> Dict[str, Any]:
    raw = _merge_defaults(raw or {}, _default_multi_config())
    accounts_out = []
    for item in (raw.get("accounts") or []):
        if not isinstance(item, dict):
            continue
        account_id = str(item.get("id") or "").strip()
        if not account_id:
            continue
        token = str(item.get("refresh_token") or "")
        accounts_out.append(
            {
                "id": account_id,
                "downloadDelay": item.get("downloadDelay"),
                "enabled": bool(item.get("enabled") if "enabled" in item else True),
                "follow_source": bool(item.get("follow_source") if "follow_source" in item else True),
                "hasRefreshToken": bool(token),
            }
        )

    proxy_pool = raw.get("proxy_pool") or {}
    easy = proxy_pool.get("easy_proxies") or {}

    return {
        "runtime_dir": raw.get("runtime_dir") or ".multi_runtime",
        "accounts": accounts_out,
        "follow": raw.get("follow") or {},
        "worker": raw.get("worker") or {},
        "proxy_pool": {
            **proxy_pool,
            "easy_proxies": {
                **easy,
                "password": "",
                "hasPassword": bool(str(easy.get("password") or "")),
            },
        },
        "_ui_nonce": raw.get("_ui_nonce") or 0,
    }


def _runtime_dir(raw: Dict[str, Any]) -> str:
    path = str((raw.get("runtime_dir") or ".multi_runtime")).strip() or ".multi_runtime"
    if not os.path.isabs(path):
        path = os.path.abspath(os.path.join(ROOT_DIR, path))
    return path


def _multi_db_path(raw: Dict[str, Any]) -> str:
    return os.path.join(_runtime_dir(raw), MULTI_DB_FILENAME)


def _terminate_process_tree(proc: subprocess.Popen) -> None:
    if not proc:
        return
    try:
        if proc.poll() is not None:
            return
    except Exception:
        return

    try:
        pid = int(proc.pid)
    except Exception:
        pid = 0

    if os.name == "nt" and pid > 0:
        try:
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
            return
        except Exception:
            pass

    try:
        proc.terminate()
    except Exception:
        pass


def _run_job(job_id: str, cmd: List[str]) -> None:
    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("PYTHONUTF8", "1")
    env.setdefault("PYTHONUNBUFFERED", "1")

    logs = deque(maxlen=LOG_LIMIT)
    with _jobs_lock:
        _jobs[job_id]["status"] = "running"
        _jobs[job_id]["logs"] = logs
    logs.append(f"[{time.strftime('%H:%M:%S')}] start: {' '.join(cmd)}")

    try:
        proc = subprocess.Popen(
            cmd,
            cwd=ROOT_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
        )
        with _jobs_lock:
            _jobs[job_id]["pid"] = proc.pid
            _jobs[job_id]["process"] = proc

        for line in proc.stdout:  # type: ignore[union-attr]
            logs.append(line.rstrip("\n"))
        proc.wait()
        status = "success" if proc.returncode == 0 else f"failed({proc.returncode})"
        logs.append(f"[done] exit_code={proc.returncode}")
    except Exception as ex:  # noqa: BLE001
        status = "failed(exception)"
        logs.append(f"[error] {ex}")
    finally:
        with _jobs_lock:
            if job_id in _jobs:
                _jobs[job_id]["status"] = status


def _job_is_running(job: Dict[str, Any]) -> bool:
    status = str(job.get("status") or "")
    if status.startswith("running") or status.startswith("pending"):
        proc = job.get("process")
        try:
            return bool(proc and proc.poll() is None)
        except Exception:
            return False
    return False


def _safe_account_id(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    return "".join(ch for ch in raw if ch.isalnum() or ch in "._-")[:64]


def _collect_pool_proxies(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return {proxies, warning, export_error}
    """
    proxy_pool = raw.get("proxy_pool") or {}
    source = str(proxy_pool.get("source") or "easy_proxies").strip().lower()

    all_raw: List[str] = []
    warning = ""
    export_error = ""

    manual = proxy_pool.get("proxies") or []
    if isinstance(manual, str):
        manual = [x.strip() for x in manual.replace("\r", "\n").split("\n") if x.strip()]
    if isinstance(manual, list):
        for item in manual:
            text = str(item or "").strip()
            if text:
                all_raw.append(text)

    proxies_file = str(proxy_pool.get("proxies_file") or "").strip()
    if proxies_file:
        try:
            path = proxies_file
            if not os.path.isabs(path):
                path = os.path.abspath(os.path.join(ROOT_DIR, path))
            if os.path.isfile(path):
                with open(path, "r", encoding="utf-8", errors="ignore") as fp:
                    for line in fp:
                        text = (line or "").strip()
                        if text and not text.startswith("#"):
                            all_raw.append(text)
        except Exception as ex:  # noqa: BLE001
            warning = f"read proxies_file failed: {ex}"

    if source == "easy_proxies":
        easy = proxy_pool.get("easy_proxies") or {}
        base_url = str(easy.get("base_url") or "http://127.0.0.1:9090").strip()
        password = str(easy.get("password") or "")
        timeout_sec = float(easy.get("timeout_sec") or 10)
        verify_ssl = bool(easy.get("verify_ssl") if "verify_ssl" in easy else True)
        host_override = str(easy.get("host_override") or "").strip()

        try:
            client = EasyProxiesClient(
                EasyProxiesConfig(
                    base_url=base_url,
                    password=password,
                    timeout=timeout_sec,
                    verify_ssl=verify_ssl,
                )
            )
            export_lines = client.export_http_proxies()

            replace_host = host_override
            if not replace_host:
                try:
                    replace_host = str(urlparse(base_url).hostname or "").strip()
                except Exception:
                    replace_host = ""

            if replace_host:
                fixed: List[str] = []
                for line in export_lines:
                    try:
                        parts = parse_proxy_url(line)
                        host = str(parts.host or "").strip().lower()
                        if host in {"0.0.0.0", "127.0.0.1", "localhost"}:
                            fixed.append(
                                ProxyParts(
                                    scheme=parts.scheme,
                                    host=replace_host,
                                    port=int(parts.port),
                                    username=parts.username,
                                    password=parts.password,
                                ).to_url()
                            )
                        else:
                            fixed.append(line)
                    except Exception:
                        fixed.append(line)
                export_lines = fixed

            all_raw.extend(export_lines)
        except Exception as ex:  # noqa: BLE001
            export_error = str(ex)

    normalized: List[str] = []
    seen = set()
    for item in all_raw:
        try:
            url = normalize_proxy_url(str(item or ""))
        except Exception:
            continue
        if url in seen:
            continue
        seen.add(url)
        normalized.append(url)

    return {
        "proxies": normalized,
        "warning": warning,
        "export_error": export_error,
        "input_count": len(all_raw),
    }


@app.get("/")
@app.get("/multi")
def page_multi():
    return render_template("multi.html", title="Pixiv Follow URL Indexer", page_name="multi")


@app.get("/api/multi/config")
def api_get_multi_config():
    raw = _load_multi_config_raw()
    return jsonify({"ok": True, "config": _serialize_multi_config(raw)})


@app.post("/api/multi/config")
def api_save_multi_config():
    payload = request.get_json(silent=True) or {}
    raw = _load_multi_config_raw()

    if "runtime_dir" in payload:
        runtime_dir = str(payload.get("runtime_dir") or "").strip()
        if runtime_dir:
            raw["runtime_dir"] = runtime_dir

    if isinstance(payload.get("follow"), dict):
        raw_follow = raw.setdefault("follow", {})
        follow = payload.get("follow") or {}
        if "bookmark_flag" in follow:
            flag = str(follow.get("bookmark_flag") or "n").strip().lower()
            raw_follow["bookmark_flag"] = flag if flag in {"y", "n", "o"} else "n"
        if "lang" in follow:
            lang = str(follow.get("lang") or "en").strip().lower()
            raw_follow["lang"] = lang or "en"
        if "refresh_interval_sec" in follow:
            raw_follow["refresh_interval_sec"] = _as_int(follow.get("refresh_interval_sec"), 1800, lo=30)
        if "timeout_sec" in follow:
            raw_follow["timeout_sec"] = _as_int(follow.get("timeout_sec"), 20, lo=3, hi=120)

    if isinstance(payload.get("worker"), dict):
        raw_worker = raw.setdefault("worker", {})
        worker = payload.get("worker") or {}
        if "poll_interval_sec" in worker:
            raw_worker["poll_interval_sec"] = _as_int(worker.get("poll_interval_sec"), 10, lo=2, hi=60)
        if "mode" in worker:
            mode = str(worker.get("mode") or "index_urls").strip().lower()
            raw_worker["mode"] = mode if mode in {"index_urls", "download_images"} else "index_urls"
        if "download_kind" in worker:
            kind = str(worker.get("download_kind") or "original").strip().lower()
            raw_worker["download_kind"] = kind if kind in {"original", "regular"} else "original"
        if "download_concurrency" in worker:
            raw_worker["download_concurrency"] = _as_int(worker.get("download_concurrency"), 4, lo=1, hi=32)
        if "download_root" in worker:
            raw_worker["download_root"] = str(worker.get("download_root") or "").strip()

    if isinstance(payload.get("proxy_pool"), dict):
        proxy_payload = payload.get("proxy_pool") or {}
        raw_proxy = raw.setdefault("proxy_pool", {})

        if "source" in proxy_payload:
            source = str(proxy_payload.get("source") or "easy_proxies").strip().lower()
            raw_proxy["source"] = source if source in {"easy_proxies", "static", "none"} else "easy_proxies"
        if "refresh_interval_sec" in proxy_payload:
            raw_proxy["refresh_interval_sec"] = _as_int(proxy_payload.get("refresh_interval_sec"), 300, lo=30)
        if "max_tokens_per_proxy" in proxy_payload:
            raw_proxy["max_tokens_per_proxy"] = _as_int(proxy_payload.get("max_tokens_per_proxy"), 2, lo=1, hi=1000)
        if "bindings_strict" in proxy_payload:
            raw_proxy["bindings_strict"] = _as_bool(proxy_payload.get("bindings_strict"), False)

        if "proxies_file" in proxy_payload:
            raw_proxy["proxies_file"] = str(proxy_payload.get("proxies_file") or "").strip()

        if "proxies" in proxy_payload:
            proxies_value = proxy_payload.get("proxies")
            proxies_out: List[str] = []
            if isinstance(proxies_value, str):
                for line in proxies_value.replace("\r", "\n").split("\n"):
                    text = line.strip()
                    if text:
                        proxies_out.append(text)
            elif isinstance(proxies_value, list):
                for item in proxies_value:
                    text = str(item or "").strip()
                    if text:
                        proxies_out.append(text)
            raw_proxy["proxies"] = proxies_out

        if isinstance(proxy_payload.get("test"), dict):
            raw_test = raw_proxy.setdefault("test", {})
            test_payload = proxy_payload.get("test") or {}
            if "target_url" in test_payload:
                raw_test["target_url"] = str(test_payload.get("target_url") or "https://www.pixiv.net/robots.txt").strip()
            if "timeout_sec" in test_payload:
                raw_test["timeout_sec"] = _as_int(test_payload.get("timeout_sec"), 8, lo=1, hi=60)
            if "concurrency" in test_payload:
                raw_test["concurrency"] = _as_int(test_payload.get("concurrency"), 20, lo=1, hi=200)
            if "attempts" in test_payload:
                raw_test["attempts"] = _as_int(test_payload.get("attempts"), 2, lo=1, hi=10)
            if "top_n" in test_payload:
                raw_test["top_n"] = _as_int(test_payload.get("top_n"), 20, lo=1, hi=500)

        if isinstance(proxy_payload.get("easy_proxies"), dict):
            raw_easy = raw_proxy.setdefault("easy_proxies", {})
            easy_payload = proxy_payload.get("easy_proxies") or {}
            if "base_url" in easy_payload:
                raw_easy["base_url"] = str(easy_payload.get("base_url") or "http://127.0.0.1:9090").strip()
            if "host_override" in easy_payload:
                raw_easy["host_override"] = str(easy_payload.get("host_override") or "").strip()
            if "timeout_sec" in easy_payload:
                raw_easy["timeout_sec"] = _as_int(easy_payload.get("timeout_sec"), 10, lo=1, hi=60)
            if "verify_ssl" in easy_payload:
                raw_easy["verify_ssl"] = _as_bool(easy_payload.get("verify_ssl"), True)
            if "password" in easy_payload:
                pwd = str(easy_payload.get("password") or "")
                if pwd != "":
                    raw_easy["password"] = pwd

    raw["_ui_nonce"] = _now_i()
    saved = _save_multi_config_raw(raw)
    return jsonify({"ok": True, "config": _serialize_multi_config(saved)})


@app.post("/api/multi/account")
def api_upsert_multi_account():
    payload = request.get_json(silent=True) or {}
    account_id = _safe_account_id(payload.get("id"))
    if not account_id:
        return jsonify({"ok": False, "message": "id is required"}), 400

    raw = _load_multi_config_raw()
    accounts = raw.setdefault("accounts", [])

    target = None
    for item in accounts:
        if isinstance(item, dict) and str(item.get("id") or "").strip() == account_id:
            target = item
            break
    if target is None:
        target = {
            "id": account_id,
            "refresh_token": "",
            "downloadDelay": 8,
            "enabled": True,
            "follow_source": True,
        }
        accounts.append(target)

    if "refresh_token" in payload:
        token = str(payload.get("refresh_token") or "")
        if token != "":
            target["refresh_token"] = token
    if _as_bool(payload.get("clearRefreshToken")) or _as_bool(payload.get("clear_refresh_token")):
        target["refresh_token"] = ""

    if "downloadDelay" in payload:
        raw_delay = payload.get("downloadDelay")
        if raw_delay in (None, ""):
            target.pop("downloadDelay", None)
        else:
            target["downloadDelay"] = _as_int(raw_delay, 8, lo=0, hi=120)

    if "enabled" in payload:
        target["enabled"] = _as_bool(payload.get("enabled"), True)
    if "follow_source" in payload:
        target["follow_source"] = _as_bool(payload.get("follow_source"), True)

    raw["_ui_nonce"] = _now_i()
    saved = _save_multi_config_raw(raw)
    return jsonify({"ok": True, "config": _serialize_multi_config(saved)})


@app.delete("/api/multi/account/<account_id>")
def api_delete_multi_account(account_id: str):
    target_id = _safe_account_id(account_id)
    raw = _load_multi_config_raw()
    accounts = raw.get("accounts") or []

    before = len(accounts)
    accounts = [
        item
        for item in accounts
        if not (isinstance(item, dict) and str(item.get("id") or "").strip() == target_id)
    ]
    if len(accounts) == before:
        return jsonify({"ok": False, "message": "account not found"}), 404

    raw["accounts"] = accounts
    raw["_ui_nonce"] = _now_i()
    saved = _save_multi_config_raw(raw)
    return jsonify({"ok": True, "config": _serialize_multi_config(saved)})


@app.post("/api/multi/proxy/test")
def api_multi_proxy_test():
    payload = request.get_json(silent=True) or {}
    raw = _load_multi_config_raw()
    merged = _merge_defaults(raw, _default_multi_config())

    # 允许传入临时 test 参数，不落盘
    test_cfg = (merged.get("proxy_pool") or {}).get("test") or {}
    test_url = str(payload.get("target_url") or test_cfg.get("target_url") or "https://www.pixiv.net/robots.txt").strip()
    timeout_sec = float(_as_int(payload.get("timeout_sec") or test_cfg.get("timeout_sec"), 8, lo=1, hi=60))
    attempts = _as_int(payload.get("attempts") or test_cfg.get("attempts"), 2, lo=1, hi=10)
    concurrency = _as_int(payload.get("concurrency") or test_cfg.get("concurrency"), 20, lo=1, hi=200)
    top_n = _as_int(payload.get("top_n") or test_cfg.get("top_n"), 20, lo=1, hi=500)

    pooled = _collect_pool_proxies(merged)
    proxies = pooled.get("proxies") or []
    warning = str(pooled.get("warning") or "")
    export_error = str(pooled.get("export_error") or "")
    input_count = int(pooled.get("input_count") or 0)

    if not proxies:
        msg = "no usable proxies"
        if export_error:
            msg += f"; easy_proxies error: {export_error}"
        return jsonify({"ok": False, "message": msg, "warning": warning, "input_count": input_count}), 400

    ranked = rank_proxies(
        proxies,
        test_url=test_url,
        timeout=float(timeout_sec),
        concurrency=int(concurrency),
        top_n=int(top_n),
        attempts=int(attempts),
    )

    rows = []
    for item in ranked:
        rows.append(
            {
                "proxy": item.masked,
                "avg_ms": item.avg_ms,
                "ok": int(item.ok),
                "fail": int(item.fail),
            }
        )

    return jsonify(
        {
            "ok": True,
            "target_url": test_url,
            "input_count": input_count,
            "normalized_count": len(proxies),
            "warning": warning,
            "export_error": export_error,
            "top": rows,
        }
    )


@app.get("/api/multi/runner")
def api_multi_runner_status():
    with _multi_lock:
        job_id = _multi_runner_job_id
    if not job_id:
        return jsonify({"ok": True, "running": False, "job_id": None, "status": "idle", "logs": []})

    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            return jsonify({"ok": True, "running": False, "job_id": None, "status": "idle", "logs": []})
        running = _job_is_running(job)
        status = str(job.get("status") or "idle")
        logs = list(job.get("logs") or [])
        pid = job.get("pid")
        cmd = job.get("cmd")

    if not running and status.startswith("running"):
        status = "stopped"

    return jsonify(
        {
            "ok": True,
            "running": bool(running),
            "job_id": job_id,
            "status": status,
            "logs": logs,
            "pid": pid,
            "cmd": cmd,
        }
    )


@app.post("/api/multi/runner/start")
def api_multi_runner_start():
    global _multi_runner_job_id

    raw = _load_multi_config_raw()
    _save_multi_config_raw(raw)

    with _multi_lock:
        existing_id = _multi_runner_job_id
        if existing_id:
            with _jobs_lock:
                existing = _jobs.get(existing_id)
            if existing and _job_is_running(existing):
                return jsonify({"ok": True, "running": True, "job_id": existing_id, "status": existing.get("status")})

        job_id = str(uuid4())
        cmd = [sys.executable, os.path.join(ROOT_DIR, "PixivMultiRunner.py"), "--config", MULTI_CONFIG_PATH]
        with _jobs_lock:
            _jobs[job_id] = {
                "id": job_id,
                "type": "multi_runner",
                "status": "pending",
                "logs": deque(maxlen=LOG_LIMIT),
                "created_at": _now_i(),
                "cmd": cmd,
            }
        _multi_runner_job_id = job_id

    thread = threading.Thread(target=_run_job, args=(job_id, cmd), daemon=True)
    thread.start()

    return jsonify({"ok": True, "running": True, "job_id": job_id, "status": "pending"})


@app.post("/api/multi/runner/stop")
def api_multi_runner_stop():
    with _multi_lock:
        job_id = _multi_runner_job_id
    if not job_id:
        return jsonify({"ok": True, "running": False, "message": "runner is not running"})

    with _jobs_lock:
        job = _jobs.get(job_id)
        proc = job.get("process") if isinstance(job, dict) else None

    if proc and proc.poll() is None:
        _terminate_process_tree(proc)

    with _jobs_lock:
        if isinstance(job, dict):
            job["status"] = "stopped"

    return jsonify({"ok": True, "running": False, "message": "runner stopped"})


@app.post("/api/multi/refresh")
def api_multi_refresh_now():
    raw = _load_multi_config_raw()
    raw["_ui_nonce"] = _now_i()
    saved = _save_multi_config_raw(raw)
    return jsonify({"ok": True, "message": "refresh signal sent", "config": _serialize_multi_config(saved)})


@app.get("/api/multi/status")
def api_multi_status():
    raw = _load_multi_config_raw()
    status_path = os.path.join(_runtime_dir(raw), "status.json")
    if not os.path.isfile(status_path):
        return jsonify({"ok": False, "message": "status.json not found, start runner first"}), 404
    try:
        data = _read_json(status_path)
    except Exception as ex:  # noqa: BLE001
        return jsonify({"ok": False, "message": f"read status failed: {ex}"}), 500
    return jsonify({"ok": True, "status": data})


@app.get("/api/multi/db/stats")
def api_multi_db_stats():
    raw = _load_multi_config_raw()
    db_path = _multi_db_path(raw)

    if not os.path.isfile(db_path):
        return jsonify(
            {
                "ok": True,
                "stats": {"member_count": 0, "image_count": 0, "url_count": 0},
                "db_path": db_path,
            }
        )

    db = PixivDBManager(root_directory=".", target=db_path, timeout=10)
    try:
        db.createDatabase()
        stats = {
            "member_count": db.countFollowMembers(),
            "image_count": db.countFollowImagesAll(),
            "url_count": db.countFollowImageUrlsAll(),
        }
    finally:
        db.close()

    return jsonify({"ok": True, "stats": stats, "db_path": db_path})


@app.get("/api/follow/stats")
def api_follow_stats_alias():
    return api_multi_db_stats()


@app.get("/api/multi/export")
def api_multi_export_urls():
    raw = _load_multi_config_raw()
    db_path = _multi_db_path(raw)
    kind = str(request.args.get("kind") or "regular").strip().lower()
    if kind not in {"regular", "original"}:
        kind = "regular"

    if not os.path.isfile(db_path):
        return jsonify({"ok": False, "message": "database not found"}), 404

    db = PixivDBManager(root_directory=".", target=db_path, timeout=10)
    try:
        db.createDatabase()
        lines = db.exportAllUrls(kind=kind)
    finally:
        db.close()

    text = "\n".join(lines) + ("\n" if lines else "")
    filename = f"pixiv_follow_urls_{kind}.txt"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return Response(text, mimetype="text/plain; charset=utf-8", headers=headers)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)
