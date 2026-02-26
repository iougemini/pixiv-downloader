async function parseResponseAsJson(response) {
  const text = await response.text();
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch {
    return { ok: false, message: `Invalid JSON (${response.status})` };
  }
}

async function requestJson(url, options = {}) {
  const response = await fetch(url, options);
  const data = await parseResponseAsJson(response);
  if (!response.ok && data && typeof data === "object") {
    return data;
  }
  if (!response.ok) {
    return { ok: false, message: `${response.status} ${response.statusText}` };
  }
  return data;
}

function apiGet(url) {
  return requestJson(url);
}

function apiPost(url, payload) {
  return requestJson(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload ?? {}),
  });
}

function apiDelete(url) {
  return requestJson(url, { method: "DELETE" });
}

function setText(id, value) {
  const element = document.getElementById(id);
  if (element) {
    element.textContent = value == null ? "" : String(value);
  }
}

function setValue(id, value) {
  const element = document.getElementById(id);
  if (element) {
    element.value = value == null ? "" : String(value);
  }
}

function setChecked(id, checked) {
  const element = document.getElementById(id);
  if (element) {
    element.checked = Boolean(checked);
  }
}

function asBool(value, defaultValue = false) {
  if (typeof value === "boolean") return value;
  if (value == null) return defaultValue;
  const text = String(value).trim().toLowerCase();
  if (!text) return defaultValue;
  return ["1", "y", "yes", "true", "on"].includes(text);
}

function toTimestampText(seconds) {
  const value = Number(seconds || 0);
  if (!value) return "-";
  return new Date(value * 1000).toLocaleString();
}

let state = {
  pollRunnerTimer: null,
  pollStatusTimer: null,
};

function renderAccounts(accounts) {
  const tbody = document.getElementById("accounts-body");
  if (!tbody) return;

  const rows = Array.isArray(accounts) ? [...accounts] : [];
  rows.sort((a, b) => String(a.id || "").localeCompare(String(b.id || "")));

  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="5" class="muted">No accounts</td></tr>`;
    return;
  }

  tbody.innerHTML = rows
    .map(
      (item) => `
      <tr data-account-id="${String(item.id || "")}">
        <td>${String(item.id || "")}</td>
        <td>${item.hasRefreshToken ? "configured" : "missing"}</td>
        <td>${item.downloadDelay ?? "-"}</td>
        <td>${item.enabled ? "Y" : "N"}</td>
        <td>${item.follow_source ? "Y" : "N"}</td>
      </tr>
    `
    )
    .join("");

  Array.from(tbody.querySelectorAll("tr[data-account-id]")).forEach((row) => {
    row.style.cursor = "pointer";
    row.addEventListener("click", () => {
      const accountId = row.getAttribute("data-account-id") || "";
      setValue("acc-id", accountId);
    });
  });
}

function fillConfig(config) {
  const cfg = config || {};

  renderAccounts(cfg.accounts || []);

  const worker = cfg.worker || {};
  setValue("worker-mode", worker.mode ?? "index_urls");
  setValue("worker-download-kind", worker.download_kind ?? "original");
  setValue("worker-download-threads", worker.download_concurrency ?? 4);

  const pool = cfg.proxy_pool || {};
  const easy = pool.easy_proxies || {};
  const test = pool.test || {};

  setValue("proxy-source", pool.source ?? "easy_proxies");
  setValue("proxy-refresh", pool.refresh_interval_sec ?? 300);
  setValue("proxy-max-per", pool.max_tokens_per_proxy ?? 2);
  setChecked("proxy-strict", asBool(pool.bindings_strict, false));
  setValue("proxy-file", pool.proxies_file ?? "");

  const manual = Array.isArray(pool.proxies) ? pool.proxies.join("\n") : "";
  setValue("proxy-manual", manual);

  setValue("easy-base-url", easy.base_url ?? "http://127.0.0.1:9090");
  setValue("easy-host-override", easy.host_override ?? "");
  setValue("easy-timeout", easy.timeout_sec ?? 10);
  setChecked("easy-verify-ssl", asBool(easy.verify_ssl, true));

  setValue("proxy-test-url", test.target_url ?? "https://www.pixiv.net/robots.txt");
  setValue("proxy-timeout", test.timeout_sec ?? 8);
  setValue("proxy-concurrency", test.concurrency ?? 20);
  setValue("proxy-attempts", test.attempts ?? 2);
  setValue("proxy-topn", test.top_n ?? 20);
}

function collectProxyPayload() {
  const source = document.getElementById("proxy-source")?.value ?? "easy_proxies";
  const refresh = document.getElementById("proxy-refresh")?.value ?? "300";
  const maxPer = document.getElementById("proxy-max-per")?.value ?? "2";
  const strict = document.getElementById("proxy-strict")?.checked ?? false;
  const proxiesFile = document.getElementById("proxy-file")?.value ?? "";
  const manualText = document.getElementById("proxy-manual")?.value ?? "";

  const easyBaseUrl = document.getElementById("easy-base-url")?.value ?? "";
  const easyPassword = document.getElementById("easy-password")?.value ?? "";
  const easyHostOverride = document.getElementById("easy-host-override")?.value ?? "";
  const easyTimeout = document.getElementById("easy-timeout")?.value ?? "10";
  const easyVerify = document.getElementById("easy-verify-ssl")?.checked ?? true;

  const targetUrl = document.getElementById("proxy-test-url")?.value ?? "";
  const timeoutSec = document.getElementById("proxy-timeout")?.value ?? "8";
  const concurrency = document.getElementById("proxy-concurrency")?.value ?? "20";
  const attempts = document.getElementById("proxy-attempts")?.value ?? "2";
  const topN = document.getElementById("proxy-topn")?.value ?? "20";

  return {
    proxy_pool: {
      source,
      refresh_interval_sec: refresh,
      max_tokens_per_proxy: maxPer,
      bindings_strict: Boolean(strict),
      proxies_file: proxiesFile,
      proxies: manualText,
      easy_proxies: {
        base_url: easyBaseUrl,
        password: easyPassword,
        host_override: easyHostOverride,
        timeout_sec: easyTimeout,
        verify_ssl: Boolean(easyVerify),
      },
      test: {
        target_url: targetUrl,
        timeout_sec: timeoutSec,
        concurrency,
        attempts,
        top_n: topN,
      },
    },
  };
}

function collectWorkerPayload() {
  const mode = document.getElementById("worker-mode")?.value ?? "index_urls";
  const kind = document.getElementById("worker-download-kind")?.value ?? "original";
  const threads = document.getElementById("worker-download-threads")?.value ?? "4";
  return {
    worker: {
      mode,
      download_kind: kind,
      download_concurrency: threads,
    },
  };
}

async function refreshRunnerState() {
  const resp = await apiGet("/api/multi/runner");
  const running = Boolean(resp.running);
  setText("runner-state", resp.status || (running ? "running" : "idle"));

  const startButton = document.getElementById("btn-runner-start");
  const stopButton = document.getElementById("btn-runner-stop");
  if (startButton) startButton.disabled = running;
  if (stopButton) stopButton.disabled = !running;

  const logs = Array.isArray(resp.logs) ? resp.logs : [];
  setText("runner-log", logs.length ? logs.join("\n") : "(empty)");
}

async function refreshRuntimeStatus() {
  const statusResp = await apiGet("/api/multi/status");
  if (!statusResp.ok) {
    setText("status-updated", "status: -");
    setText("status-follow", "follow: -");
    setText("status-work", "work: -");
    return;
  }

  const status = statusResp.status || {};
  const follow = status.follow || {};
  const work = status.work || {};

  setText("status-updated", `status: ${toTimestampText(status.updated_at)}`);
  setText(
    "status-follow",
    `follow: unique=${follow.unique_artists ?? 0}, last=${toTimestampText(follow.last_refresh)}`
  );
  setText(
    "status-work",
    `work: assigned=${work.assigned_artists_total ?? 0}, processed=${work.processed_artists_total ?? 0}`
  );
}

async function refreshDbStats() {
  const dbResp = await apiGet("/api/multi/db/stats");
  if (!dbResp.ok) {
    setText("status-db", "db: -");
    setText("db-path", "db_path: -");
    return;
  }

  const stats = dbResp.stats || {};
  setText("status-db", `db: members=${stats.member_count ?? 0}, images=${stats.image_count ?? 0}, urls=${stats.url_count ?? 0}`);
  setText("db-path", `db_path: ${dbResp.db_path || "-"}`);
}

async function refreshAll() {
  const cfgResp = await apiGet("/api/multi/config");
  if (cfgResp.ok) {
    fillConfig(cfgResp.config || {});
  }
  await refreshRunnerState();
  await refreshRuntimeStatus();
  await refreshDbStats();
}

async function startRunner() {
  const resp = await apiPost("/api/multi/runner/start", {});
  setText("runner-state", resp.ok ? "starting" : resp.message || "start failed");
  await refreshRunnerState();
}

async function stopRunner() {
  const resp = await apiPost("/api/multi/runner/stop", {});
  setText("runner-state", resp.ok ? "stopped" : resp.message || "stop failed");
  await refreshRunnerState();
}

async function forceRefresh() {
  const resp = await apiPost("/api/multi/refresh", {});
  setText("runner-state", resp.ok ? "refresh signal sent" : resp.message || "refresh failed");
}

async function saveAccount() {
  const accountId = (document.getElementById("acc-id")?.value || "").trim();
  if (!accountId) {
    setText("acc-status", "Account ID is required");
    return;
  }

  const payload = {
    id: accountId,
    enabled: document.getElementById("acc-enabled")?.checked ?? true,
    follow_source: document.getElementById("acc-follow-source")?.checked ?? true,
    clearRefreshToken: document.getElementById("acc-clear-refresh-token")?.checked ?? false,
  };

  const tokenRaw = document.getElementById("acc-refresh-token")?.value;
  if (tokenRaw !== undefined && tokenRaw !== "") {
    payload.refresh_token = tokenRaw;
  }

  const delayRaw = document.getElementById("acc-delay")?.value;
  if (delayRaw !== undefined && delayRaw !== "") {
    payload.downloadDelay = delayRaw;
  }

  const resp = await apiPost("/api/multi/account", payload);
  setText("acc-status", resp.ok ? "Saved" : resp.message || "Save failed");

  setValue("acc-refresh-token", "");
  setChecked("acc-clear-refresh-token", false);
  await refreshAll();
}

async function deleteAccount() {
  const accountId = (document.getElementById("acc-id")?.value || "").trim();
  if (!accountId) {
    setText("acc-status", "Account ID is required");
    return;
  }

  const resp = await apiDelete(`/api/multi/account/${encodeURIComponent(accountId)}`);
  setText("acc-status", resp.ok ? "Deleted" : resp.message || "Delete failed");
  await refreshAll();
}

async function saveProxyConfig(event) {
  event.preventDefault();
  const payload = collectProxyPayload();
  const resp = await apiPost("/api/multi/config", payload);
  setText("proxy-status", resp.ok ? "Saved" : resp.message || "Save failed");
  setValue("easy-password", "");
  await refreshAll();
}

async function saveWorkerConfig(event) {
  event.preventDefault();
  const payload = collectWorkerPayload();
  const resp = await apiPost("/api/multi/config", payload);
  setText("worker-status", resp.ok ? "Saved" : resp.message || "Save failed");
  await refreshAll();
}

async function testProxyPool() {
  const payload = {
    target_url: document.getElementById("proxy-test-url")?.value ?? "",
    timeout_sec: document.getElementById("proxy-timeout")?.value ?? "",
    concurrency: document.getElementById("proxy-concurrency")?.value ?? "",
    attempts: document.getElementById("proxy-attempts")?.value ?? "",
    top_n: document.getElementById("proxy-topn")?.value ?? "",
  };

  const resp = await apiPost("/api/multi/proxy/test", payload);
  if (!resp.ok) {
    setText("proxy-test-status", resp.message || "Probe failed");
    setText("proxy-top", "(no result)");
    return;
  }

  setText("proxy-test-status", `ok: ${resp.normalized_count ?? 0} proxies`);
  const rows = Array.isArray(resp.top) ? resp.top : [];
  if (!rows.length) {
    setText("proxy-top", "(no usable proxy)");
    return;
  }

  const text = rows
    .map((item, index) => {
      const lineNo = String(index + 1).padStart(2, "0");
      const avg = item.avg_ms == null ? "-" : Number(item.avg_ms).toFixed(1);
      return `${lineNo}. ${item.proxy}  avg=${avg}ms  ok=${item.ok ?? 0} fail=${item.fail ?? 0}`;
    })
    .join("\n");
  setText("proxy-top", text);
}

function bindEvents() {
  document.getElementById("btn-runner-start")?.addEventListener("click", () => startRunner().catch(() => {}));
  document.getElementById("btn-runner-stop")?.addEventListener("click", () => stopRunner().catch(() => {}));
  document.getElementById("btn-runner-refresh")?.addEventListener("click", () => forceRefresh().catch(() => {}));

  document.getElementById("btn-acc-save")?.addEventListener("click", () => saveAccount().catch(() => {}));
  document.getElementById("btn-acc-delete")?.addEventListener("click", () => deleteAccount().catch(() => {}));

  document.getElementById("worker-form")?.addEventListener("submit", (event) => saveWorkerConfig(event).catch(() => {}));
  document.getElementById("proxy-form")?.addEventListener("submit", (event) => saveProxyConfig(event).catch(() => {}));
  document.getElementById("btn-proxy-test")?.addEventListener("click", () => testProxyPool().catch(() => {}));
}

async function init() {
  bindEvents();
  await refreshAll();

  if (state.pollRunnerTimer) clearInterval(state.pollRunnerTimer);
  if (state.pollStatusTimer) clearInterval(state.pollStatusTimer);

  state.pollRunnerTimer = setInterval(() => refreshRunnerState().catch(() => {}), 1500);
  state.pollStatusTimer = setInterval(() => {
    refreshRuntimeStatus().catch(() => {});
    refreshDbStats().catch(() => {});
  }, 3000);
}

window.addEventListener("DOMContentLoaded", () => {
  init().catch(() => {
    setText("runner-state", "init failed");
  });
});
