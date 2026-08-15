(() => {
  "use strict";

  const TOKEN_KEY = "sooqa.admin.api_token";
  const INGEST_AUTO_REFRESH_MS = 15_000;
  const PAGE_NAMES = new Set(["dashboard", "ingests", "media", "schedule", "settings"]);
  const state = {
    token: readToken(),
    page: "dashboard",
    ingestPageCursor: null,
    ingestCursor: null,
    ingestLoading: false,
    ingestRefreshTimer: null,
    channels: [],
  };

  const $ = (id) => document.getElementById(id);

  class UiError extends Error {
    constructor(message, status) {
      super(message);
      this.name = "UiError";
      this.status = status;
    }
  }

  function readToken() {
    try {
      return window.sessionStorage.getItem(TOKEN_KEY) || "";
    } catch (_error) {
      return "";
    }
  }

  function writeToken(token) {
    try {
      if (token) {
        window.sessionStorage.setItem(TOKEN_KEY, token);
      } else {
        window.sessionStorage.removeItem(TOKEN_KEY);
      }
    } catch (_error) {
      throw new UiError("This browser does not allow session-only token storage.");
    }
  }

  function node(tag, className, value) {
    const element = document.createElement(tag);
    if (className) element.className = className;
    if (value !== undefined && value !== null) element.textContent = String(value);
    return element;
  }

  function clear(element) {
    while (element.firstChild) element.removeChild(element.firstChild);
  }

  function safeHttpUrl(value) {
    if (!value) return null;
    try {
      const url = new URL(value, window.location.origin);
      return url.protocol === "http:" || url.protocol === "https:" ? url.href : null;
    } catch (_error) {
      return null;
    }
  }

  function link(value, label) {
    const url = safeHttpUrl(value);
    if (!url) return node("span", "muted", label || value || "—");
    const anchor = node("a", "safe-link", label || value);
    anchor.href = url;
    anchor.target = "_blank";
    anchor.rel = "noopener noreferrer";
    return anchor;
  }

  function appLink(hash, label) {
    const anchor = node("a", "safe-link", label);
    anchor.href = hash;
    return anchor;
  }

  function formatDate(value) {
    if (!value) return "—";
    const date = new Date(value);
    return Number.isNaN(date.valueOf()) ? String(value) : date.toLocaleString();
  }

  function formatId(value) {
    if (!value) return "—";
    const text = String(value);
    return text.length > 18 ? `${text.slice(0, 8)}…${text.slice(-6)}` : text;
  }

  function showToast(message, error) {
    const toast = $("toast");
    toast.textContent = message;
    toast.classList.toggle("error", Boolean(error));
    toast.hidden = false;
    window.clearTimeout(showToast.timer);
    showToast.timer = window.setTimeout(() => { toast.hidden = true; }, 5200);
  }

  function setAuthView(unlocked) {
    $("token-gate").hidden = unlocked;
    $("admin-shell").hidden = !unlocked;
    $("lock-button").hidden = !unlocked;
    $("session-status").textContent = unlocked ? "Session token active" : "Locked";
  }

  function lock() {
    stopIngestAutoRefresh();
    state.token = "";
    try {
      writeToken("");
    } catch (_error) {
      // The in-memory token is still cleared even if storage is unavailable.
    }
    setAuthView(false);
    $("api-token").value = "";
    $("api-token").focus();
  }

  async function api(path, options) {
    if (!state.token) throw new UiError("Unlock the admin before making requests.");
    const requestOptions = options || {};
    const headers = new Headers(requestOptions.headers || {});
    headers.set("Authorization", `Bearer ${state.token}`);
    headers.set("Accept", "application/json");
    if (requestOptions.body && !headers.has("Content-Type")) {
      headers.set("Content-Type", "application/json");
    }
    const response = await fetch(path, { ...requestOptions, headers, credentials: "same-origin" });
    const contentType = response.headers.get("content-type") || "";
    const payload = contentType.includes("json") ? await response.json() : null;
    if (response.status === 401) {
      lock();
      throw new UiError("The token was rejected. Enter it again.", response.status);
    }
    if (!response.ok) {
      throw new UiError(payload?.error?.message || `Request failed (${response.status}).`, response.status);
    }
    return payload;
  }

  async function withBusy(button, operation) {
    if (!button || button.disabled) return;
    button.disabled = true;
    button.setAttribute("aria-busy", "true");
    try {
      await operation();
    } catch (error) {
      showToast(error instanceof Error ? error.message : "The request failed.", true);
      if (error instanceof UiError && error.status === 409) await route();
    } finally {
      button.disabled = false;
      button.removeAttribute("aria-busy");
    }
  }

  function actionButton(label, operation, className) {
    const button = node("button", className || "button button-small button-secondary", label);
    button.type = "button";
    button.addEventListener("click", () => { void withBusy(button, operation); });
    return button;
  }

  function renderCounts(counts) {
    const labels = [
      ["ready_media", "Stored media"],
      ["future_queued_posts", "Future queued posts"],
      ["active_ingests", "Active ingests"],
      ["technical_jobs_queued", "Jobs queued"],
      ["technical_jobs_running", "Jobs running"],
      ["technical_duplicate_decisions", "Duplicate decisions"],
      ["publication_repeat_decisions", "Repeat decisions"],
      ["caption_sync_failures", "Caption failures"],
    ];
    const container = $("dashboard-counts");
    clear(container);
    for (const [key, label] of labels) {
      const metric = node("div", "metric");
      metric.append(node("div", "metric-label", label), node("div", "metric-value", counts?.[key] ?? 0));
      container.append(metric);
    }
  }

  function renderDuplicates(items) {
    const container = $("duplicate-list");
    clear(container);
    $("duplicate-count").textContent = String(items.length);
    if (!items.length) {
      container.append(node("p", "empty-state", "No duplicate decisions."));
      return;
    }
    for (const item of items) {
      const card = node("article", "decision-card");
      const source = node("p");
      source.append(node("strong", "", "Incoming source"), node("br"));
      source.append(link(item.source_url, item.source_url || "No source URL"));
      card.append(source, node("p", "meta", `Ingest ${formatId(item.ingest_id)}`));
      for (const candidate of item.candidates || []) {
        const row = node("div", "candidate-row");
        const label = node("div", "candidate-label");
        label.append(
          node("strong", "", `${candidate.classification || "Candidate"} · ${candidate.score_bps ?? 0} bps`),
          candidate.storage_url ? link(candidate.storage_url, `Media ${formatId(candidate.media_id)}`) : node("span", "muted", `Media ${formatId(candidate.media_id)}`),
        );
        row.append(label, actionButton("Same — use this", async () => {
          await api(`/api/v1/ingests/${encodeURIComponent(item.ingest_id)}/accept-duplicate`, {
            method: "POST",
            body: JSON.stringify({ media_id: candidate.media_id }),
          });
          showToast("Duplicate decision accepted.");
          await loadDashboard();
        }));
        card.append(row);
      }
      card.append(node("div", "decision-actions", ""));
      const actions = card.lastChild;
      actions.append(actionButton("Different — save as new", async () => {
        await api(`/api/v1/ingests/${encodeURIComponent(item.ingest_id)}/force-save`, { method: "POST" });
        showToast("Force-save resumed.");
        await loadDashboard();
      }, "button button-small button-danger"));
      container.append(card);
    }
  }

  function renderRepeats(items) {
    const container = $("repeat-list");
    clear(container);
    $("repeat-count").textContent = String(items.length);
    if (!items.length) {
      container.append(node("p", "empty-state", "No repeat decisions."));
      return;
    }
    for (const item of items) {
      const card = node("article", "decision-card");
      card.append(
        node("p", "", `${item.requested_action || "Publication"} · ${item.status || "unknown"}`),
        node("p", "meta", `Media ${formatId(item.media_id)} · revision ${item.revision}`),
      );
      if (item.caption) card.append(node("p", "muted", item.caption));
      if (item.requested_publish_at) card.append(node("p", "meta", `Requested ${formatDate(item.requested_publish_at)}`));
      const conflicts = item.repeat_evidence?.conflicts || [];
      if (conflicts.length) {
        const conflictList = node("ul", "conflict-list");
        for (const conflict of conflicts) {
          const detail = node("li");
          detail.append(
            node("span", "state", `${conflict.state || "unknown"} · ${formatDate(conflict.at)}`),
            conflict.target_message_link
              ? link(conflict.target_message_link, `Post ${formatId(conflict.post_id)}`)
              : node("span", "muted", `Post ${formatId(conflict.post_id)}`),
          );
          conflictList.append(detail);
        }
        card.append(conflictList);
      }
      const actions = node("div", "decision-actions");
      const choices = item.requested_action === "post_now"
        ? [["Post now anyway", "post_now_anyway"], ["Queue normally", "queue_anyway"], ["Cancel", "cancel"]]
        : item.requested_publish_at
          ? [["Keep exact time", "keep_exact_time"], ["Queue normally", "queue_anyway"], ["Cancel", "cancel"]]
          : [["Queue anyway", "queue_anyway"], ["Cancel", "cancel"]];
      for (const [label, decision] of choices) {
        actions.append(actionButton(label, async () => {
          await api(`/api/v1/posts/${encodeURIComponent(item.post_id)}/decision`, {
            method: "POST",
            headers: { "Idempotency-Key": `admin-ui:${randomKey()}` },
            body: JSON.stringify({ decision, expected_revision: item.revision }),
          });
          showToast("Publication decision saved.");
          await loadDashboard();
        }, decision === "cancel" ? "button button-small button-danger" : undefined));
      }
      card.append(actions);
      container.append(card);
    }
  }

  function renderCaptionFailures(items) {
    const container = $("caption-failure-list");
    clear(container);
    $("caption-failure-count").textContent = String(items.length);
    if (!items.length) {
      container.append(node("p", "empty-state", "No caption failures."));
      return;
    }
    for (const item of items) {
      const card = node("article", "decision-card");
      const target = node("p");
      target.append(appLink("#media", `Media ${formatId(item.media_id)} · open Media / Retry`));
      card.append(target, node("p", "meta", item.error_message || "Telegram storage caption sync failed."));
      container.append(card);
    }
  }

  async function loadDashboard() {
    const data = await api("/api/v1/dashboard?limit=20");
    renderCounts(data.counts);
    renderDuplicates(data.attention?.technical_duplicates || []);
    renderRepeats(data.attention?.publication_repeats || []);
    renderCaptionFailures(data.attention?.caption_sync_failures || []);
  }

  function appendIngestCell(row, value) {
    const cell = document.createElement("td");
    if (value instanceof Node) cell.append(value); else cell.textContent = value === undefined || value === null || value === "" ? "—" : String(value);
    row.append(cell);
  }

  function formatDuration(start, end) {
    if (!start || !end) return "—";
    const milliseconds = new Date(end).valueOf() - new Date(start).valueOf();
    if (!Number.isFinite(milliseconds) || milliseconds < 0) return "—";
    const seconds = Math.round(milliseconds / 1000);
    if (seconds < 60) return `${seconds}s`;
    const minutes = Math.floor(seconds / 60);
    if (minutes < 60) return `${minutes}m ${seconds % 60}s`;
    return `${Math.floor(minutes / 60)}h ${minutes % 60}m`;
  }

  function renderIngests(data) {
    const rows = $("ingest-rows");
    clear(rows);
    const items = data.items || [];
    if (!items.length) {
      const row = document.createElement("tr");
      const cell = node("td", "empty-state", "No ingests found.");
      cell.colSpan = 7;
      row.append(cell);
      rows.append(row);
    }
    for (const item of items) {
      const row = document.createElement("tr");
      const id = node("span", "mono", formatId(item.id));
      id.title = item.id || "";
      appendIngestCell(row, id);
      const source = item.source_url ? link(item.source_url, item.source_url) : node("span", "muted", "No source URL");
      appendIngestCell(row, source);
      appendIngestCell(row, item.requested_action);
      const status = node("span", "state", item.status);
      status.dataset.state = item.status || "";
      appendIngestCell(row, status);
      const timing = node("div", "timestamp-stack");
      timing.append(
        node("span", "meta", `Created ${formatDate(item.created_at)}`),
        node("span", "meta", `Updated ${formatDate(item.updated_at)}`),
        node("span", "meta", `Duration ${formatDuration(item.created_at, item.completed_at || item.updated_at)}`),
      );
      appendIngestCell(row, timing);
      const result = node("span");
      if (item.media_id) result.append(node("span", "muted", `Media ${formatId(item.media_id)}`));
      if (item.storage_url) result.append(result.firstChild ? node("span", "muted", " · ") : document.createTextNode(""), link(item.storage_url, "Telegram"));
      appendIngestCell(row, result);
      appendIngestCell(row, [item.error_code, item.error_message].filter(Boolean).join(": "));
      rows.append(row);
    }
    const next = $("ingests-next");
    next.hidden = !data.next_cursor;
    state.ingestCursor = data.next_cursor || null;
  }

  async function loadIngests(cursor) {
    if (state.ingestLoading) return;
    state.ingestLoading = true;
    state.ingestPageCursor = cursor || null;
    const suffix = cursor ? `&cursor=${encodeURIComponent(cursor)}` : "";
    try {
      const data = await api(`/api/v1/ingests?limit=50${suffix}`);
      renderIngests(data);
      if (state.ingestPageCursor === null) startIngestAutoRefresh();
      else stopIngestAutoRefresh();
    } finally {
      state.ingestLoading = false;
    }
  }

  function stopIngestAutoRefresh() {
    if (state.ingestRefreshTimer !== null) {
      window.clearInterval(state.ingestRefreshTimer);
      state.ingestRefreshTimer = null;
    }
  }

  function startIngestAutoRefresh() {
    stopIngestAutoRefresh();
    if (!state.token || state.page !== "ingests" || state.ingestPageCursor !== null) return;
    state.ingestRefreshTimer = window.setInterval(() => {
      if (state.page !== "ingests" || state.ingestPageCursor !== null) {
        stopIngestAutoRefresh();
        return;
      }
      void loadIngests(null).catch((error) => {
        showToast(error instanceof Error ? error.message : "The ingests could not be refreshed.", true);
      });
    }, INGEST_AUTO_REFRESH_MS);
  }

  function timeValue(value, fallback) {
    if (!value) return fallback;
    return String(value).slice(0, 5);
  }

  function fillSettings(channel) {
    $("channel-id").value = channel?.id || "";
    $("channel-updated-at").value = channel?.updated_at || "";
    $("channel-name").value = channel?.name || "";
    $("channel-chat-id").value = channel?.telegram_chat_id ?? "";
    $("channel-time-zone").value = channel?.time_zone || "UTC";
    $("channel-window-start").value = timeValue(channel?.window_start, "08:00");
    $("channel-window-end").value = timeValue(channel?.window_end, "22:00");
    $("channel-interval").value = channel?.interval_minutes ?? 30;
    $("channel-parse-mode").value = channel?.default_parse_mode || "";
    $("channel-enabled").checked = channel ? Boolean(channel.is_enabled) : true;
    $("channel-disable-notification").checked = Boolean(channel?.default_disable_notification);
    $("settings-mode").textContent = channel ? `Editing ${channel.name}` : "Create the default channel";
  }

  function renderSettings(channels) {
    state.channels = channels || [];
    const warning = $("settings-warning");
    if (state.channels.length > 1) {
      warning.textContent = "More than one channel exists. Keep one target enabled; the API remains authoritative about ambiguity.";
      warning.hidden = false;
    } else {
      warning.hidden = true;
    }
    const channel = state.channels.find((item) => item.is_enabled) || state.channels[0] || null;
    fillSettings(channel);
  }

  async function loadSettings() {
    const data = await api("/api/v1/channels");
    renderSettings(data.items || []);
  }

  function randomKey() {
    if (window.crypto && typeof window.crypto.randomUUID === "function") return window.crypto.randomUUID();
    return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  }

  async function saveSettings(event) {
    event.preventDefault();
    const form = $("settings-form");
    if (!form.reportValidity()) return;
    const chatId = Number($("channel-chat-id").value);
    if (!Number.isSafeInteger(chatId) || chatId >= 0) {
      showToast("Telegram chat ID must be a negative integer.", true);
      return;
    }
    const channelId = $("channel-id").value;
    const body = {
      name: $("channel-name").value.trim(),
      telegram_chat_id: chatId,
      is_enabled: $("channel-enabled").checked,
      time_zone: $("channel-time-zone").value.trim(),
      window_start: $("channel-window-start").value,
      window_end: $("channel-window-end").value,
      interval_minutes: Number($("channel-interval").value),
      default_parse_mode: $("channel-parse-mode").value || null,
      default_disable_notification: $("channel-disable-notification").checked,
    };
    const button = $("settings-save");
    await withBusy(button, async () => {
      const result = channelId
        ? await api(`/api/v1/channels/${encodeURIComponent(channelId)}`, {
          method: "PATCH",
          body: JSON.stringify({ ...body, expected_updated_at: $("channel-updated-at").value }),
        })
        : await api("/api/v1/channels", { method: "POST", body: JSON.stringify({ ...body, is_enabled: undefined }) });
      showToast(`Settings saved for ${result.name}.`);
      await loadSettings();
    });
  }

  async function route() {
    stopIngestAutoRefresh();
    const requested = window.location.hash.slice(1);
    state.page = PAGE_NAMES.has(requested) ? requested : "dashboard";
    for (const page of document.querySelectorAll("[data-page]")) page.hidden = page.dataset.page !== state.page;
    for (const navigation of document.querySelectorAll("[data-page-link]")) navigation.classList.toggle("active", navigation.dataset.pageLink === state.page);
    if (!state.token) {
      setAuthView(false);
      return;
    }
    setAuthView(true);
    try {
      if (state.page === "dashboard") await loadDashboard();
      if (state.page === "ingests") await loadIngests(null);
      if (state.page === "settings") await loadSettings();
    } catch (error) {
      showToast(error instanceof Error ? error.message : "The page could not be loaded.", true);
    }
  }

  $("token-form").addEventListener("submit", (event) => {
    event.preventDefault();
    const token = $("api-token").value.trim();
    if (!token) return;
    try {
      writeToken(token);
      state.token = token;
      $("token-error").hidden = true;
      $("api-token").value = "";
      void route();
    } catch (error) {
      $("token-error").textContent = error instanceof Error ? error.message : "Token storage failed.";
      $("token-error").hidden = false;
    }
  });
  $("lock-button").addEventListener("click", lock);
  $("dashboard-refresh").addEventListener("click", (event) => { void withBusy(event.currentTarget, loadDashboard); });
  $("ingests-refresh").addEventListener("click", (event) => { void withBusy(event.currentTarget, () => loadIngests(null)); });
  $("ingests-next").addEventListener("click", (event) => {
    const cursor = state.ingestCursor;
    if (cursor) {
      stopIngestAutoRefresh();
      void withBusy(event.currentTarget, () => loadIngests(cursor));
    }
  });
  $("settings-refresh").addEventListener("click", (event) => { void withBusy(event.currentTarget, loadSettings); });
  $("settings-form").addEventListener("submit", (event) => { void saveSettings(event); });
  window.addEventListener("hashchange", () => { void route(); });

  setAuthView(Boolean(state.token));
  void route();
})();
