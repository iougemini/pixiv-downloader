# Pixiv Follow URL Indexer

Pixiv Follow URL Indexer is a WebUI-first service that indexes artwork URLs from followed Pixiv creators.

It has one goal only:
- fetch followed creators and their artworks,
- run reliably with multi-account + multi-thread + proxy pool,
- store and export `regular` / `original` image URLs (and optionally download files).

> Worker mode supports:
> - `index_urls`: only index + export URLs
> - `download_images`: download image files (default `original`, optional `regular`)

## Features

- WebUI-based configuration and runner control
- Multi-account scheduling with `refresh_token`
- Proxy pool support (`easy_proxies`, static list, or disabled)
- Continuous indexing with runtime status tracking
- One-click URL export from SQLite
- Optional image downloading to `runtime_dir/downloads/`

## Project Layout

- `web_ui.py`: WebUI entry point (recommended startup method)
- `PixivMultiRunner.py`: scheduler started by WebUI
- `PixivMultiWorker.py`: worker process for URL indexing
- `PixivDBManager.py`: lightweight SQLite layer
- `common/`: API client, proxy utilities, proxy tester
- `webui/`: templates and frontend assets
- `multi_config.example.json`: config template

## Requirements

- Python `3.10+`
- Network access to Pixiv APIs

Install dependencies:

```bash
python -m pip install -r requirements.txt
```

## Quick Start (WebUI Only)

1) Create local config file:

Windows:

```bash
copy multi_config.example.json multi_config.json
```

Linux/macOS:

```bash
cp multi_config.example.json multi_config.json
```

2) Edit `multi_config.json` and set at least:
- `accounts[].id`
- `accounts[].refresh_token`

3) Start WebUI:

```bash
python web_ui.py
```

4) Open:

`http://127.0.0.1:5000/`

## Typical Workflow

1. Add or update accounts in **Accounts**
2. Configure proxy source in **Proxy Pool**
3. Click **Start Runner**
4. Monitor **Runtime Status** and DB counters
5. Export URLs with:
   - `/api/multi/export?kind=regular`
   - `/api/multi/export?kind=original`

## Key Config Fields

### `accounts`
- `id`: unique account name
- `refresh_token`: Pixiv refresh token
- `downloadDelay`: per-account request delay (seconds)
- `enabled`: enable worker for this account
- `follow_source`: allow this account to fetch follow list

### `follow`
- `bookmark_flag`: follow visibility mode (`n/y/o`)
- `lang`: API language (`en`, etc.)
- `refresh_interval_sec`: follow refresh interval
- `timeout_sec`: follow request timeout

### `proxy_pool`
- `source`: `easy_proxies` / `static` / `none`
- `refresh_interval_sec`: proxy refresh interval
- `max_tokens_per_proxy`: max account bindings per proxy
- `bindings_strict`: strict capacity behavior
- `easy_proxies.*`: Easy Proxies service settings
- `test.*`: proxy probe settings

### `worker`
- `poll_interval_sec`: scheduler poll interval
- `mode`: `index_urls` / `download_images`
- `download_kind`: `original` / `regular`
- `download_concurrency`: per-worker download threads
- `download_root`: output directory (empty => `runtime_dir/downloads`)

## Runtime Data

- Default runtime directory: `.multi_runtime/`
- Default database path: `.multi_runtime/db.multi.sqlite`
- Runtime status file: `.multi_runtime/status.json`

## Security Checklist Before Publishing

- Never commit real `multi_config.json`
- Never commit tokens/passwords/proxy credentials
- Never commit runtime DB/log files
- If secrets were committed in history, rewrite Git history before publishing
