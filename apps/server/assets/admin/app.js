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
    mediaQuery: "",
    mediaCursor: null,
    mediaItems: [],
    mediaPreviewUrls: new Set(),
    mediaRenderGeneration: 0,
    publication: null,
    scheduleCursor: null,
    scheduleItems: [],
    scheduleEditing: new Set(),
    schedulePreviewEntries: new Map(),
    schedulePreviewUrls: new Set(),
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

  function mediaNavigationLink(mediaId, label) {
    const anchor = appLink("#media", label);
    anchor.addEventListener("click", () => { state.mediaQuery = mediaId || ""; });
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

  function selectText(element) {
    element.focus();
    if (typeof element.select === "function") {
      element.select();
    } else if (typeof element.setSelectionRange === "function") {
      element.setSelectionRange(0, element.value.length);
    }
  }

  function copyWithSelection(value) {
    const source = document.createElement("textarea");
    source.value = value;
    source.readOnly = true;
    source.setAttribute("aria-hidden", "true");
    source.style.position = "fixed";
    source.style.opacity = "0";
    source.style.pointerEvents = "none";
    document.body.append(source);
    selectText(source);
    let copied = false;
    try {
      copied = typeof document.execCommand === "function" && document.execCommand("copy");
    } catch (_error) {
      copied = false;
    }
    document.body.removeChild(source);
    return Boolean(copied);
  }

  async function copyText(value) {
    const clipboard = window.navigator && window.navigator.clipboard;
    if (clipboard && typeof clipboard.writeText === "function") {
      try {
        await clipboard.writeText(value);
        return true;
      } catch (_error) {
        // A denied or unavailable Clipboard API can still work through selection.
      }
    }
    return copyWithSelection(value);
  }

  function copyableId(label, value) {
    const fullValue = value ? String(value) : "";
    if (!fullValue) return node("span", "muted", "—");

    const kind = String(label || "item").toLowerCase();
    const shortValue = formatId(fullValue);
    const copyLabel = `Copy full ${kind} ID`;
    const control = node("span", "copyable-id-control");
    const button = node("button", "copyable-id-button mono", shortValue);
    button.type = "button";
    button.title = copyLabel;
    button.setAttribute("aria-label", copyLabel);
    control.append(button);

    let fallback = null;
    let feedbackTimer = null;
    const ensureFallback = () => {
      if (fallback) return fallback;
      fallback = node("input", "copyable-id-fallback mono");
      fallback.type = "text";
      fallback.value = fullValue;
      fallback.readOnly = true;
      fallback.setAttribute("aria-label", `Full ${kind} ID`);
      control.append(fallback);
      return fallback;
    };
    const restoreButton = () => {
      button.textContent = shortValue;
      button.classList.toggle("copied", false);
      button.setAttribute("aria-label", copyLabel);
    };
    const showCopied = () => {
      if (fallback) fallback.hidden = true;
      button.textContent = "Copied";
      button.classList.toggle("copied", true);
      button.setAttribute("aria-label", `${kind} ID copied`);
      window.clearTimeout(feedbackTimer);
      feedbackTimer = window.setTimeout(restoreButton, 1800);
      showToast(`Copied full ${kind} ID.`);
    };
    const showCopyFailure = () => {
      const visibleFallback = ensureFallback();
      visibleFallback.hidden = false;
      selectText(visibleFallback);
      showToast(`Could not copy the full ${kind} ID. The full value is selected for manual copy.`, true);
    };
    const performCopy = async () => {
      if (await copyText(fullValue)) showCopied();
      else showCopyFailure();
    };

    button.addEventListener("click", () => { void performCopy(); });
    button.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        void performCopy();
      }
    });
    return control;
  }

  function idReference(label, value) {
    const reference = node("span", "id-reference");
    reference.append(node("span", "id-reference-label", `${label} `), copyableId(label, value));
    return reference;
  }

  function formatBytes(value) {
    if (!Number.isFinite(Number(value)) || Number(value) < 0) return "—";
    const bytes = Number(value);
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KiB`;
    if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MiB`;
    return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GiB`;
  }

  function formatMediaDuration(value) {
    if (!Number.isFinite(Number(value)) || Number(value) < 0) return "—";
    const totalSeconds = Math.floor(Number(value) / 1000);
    const hours = Math.floor(totalSeconds / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;
    return hours ? `${hours}h ${String(minutes).padStart(2, "0")}m` : `${minutes}:${String(seconds).padStart(2, "0")}`;
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
    invalidateMediaPreviews();
    invalidateSchedulePreviews();
    discardScheduleEdits();
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

  async function request(path, options) {
    if (!state.token) throw new UiError("Unlock the admin before making requests.");
    const requestOptions = options || {};
    const headers = new Headers(requestOptions.headers || {});
    headers.set("Authorization", `Bearer ${state.token}`);
    headers.set("Accept", "application/json");
    if (requestOptions.body && !headers.has("Content-Type")) {
      headers.set("Content-Type", "application/json");
    }
    if (!headers.has("Accept")) headers.set("Accept", "application/json");
    const response = await fetch(path, { ...requestOptions, headers, credentials: "same-origin" });
    if (response.status === 401) {
      lock();
      throw new UiError("The token was rejected. Enter it again.", response.status);
    }
    if (!response.ok) {
      const contentType = response.headers.get("content-type") || "";
      const payload = contentType.includes("json") ? await response.json() : null;
      throw new UiError(payload?.error?.message || `Request failed (${response.status}).`, response.status);
    }
    return response;
  }

  async function api(path, options) {
    const response = await request(path, options);
    const contentType = response.headers.get("content-type") || "";
    return contentType.includes("json") ? response.json() : null;
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
      const ingest = node("p", "meta");
      ingest.append(idReference("Ingest", item.ingest_id));
      card.append(source, ingest);
      for (const candidate of item.candidates || []) {
        const row = node("div", "candidate-row");
        const label = node("div", "candidate-label");
        label.append(
          node("strong", "", `${candidate.classification || "Candidate"} · ${candidate.score_bps ?? 0} bps`),
          idReference("Media", candidate.media_id),
          candidate.storage_url ? link(candidate.storage_url, "Telegram") : node("span", "muted", "Telegram unavailable"),
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
      const media = node("p", "meta");
      media.append(idReference("Media", item.media_id), node("span", "", ` · revision ${item.revision}`));
      card.append(
        node("p", "", `${item.requested_action || "Publication"} · ${item.status || "unknown"}`),
        media,
      );
      if (item.caption) card.append(node("p", "muted", item.caption));
      if (item.requested_publish_at) card.append(node("p", "meta", `Requested ${formatDate(item.requested_publish_at)}`));
      const conflicts = item.repeat_evidence?.conflicts || [];
      if (conflicts.length) {
        const conflictList = node("ul", "conflict-list");
        for (const conflict of conflicts) {
          const detail = node("li");
          detail.append(node("span", "state", `${conflict.state || "unknown"} · ${formatDate(conflict.at)}`), idReference("Post", conflict.post_id));
          if (conflict.target_message_link) detail.append(link(conflict.target_message_link, "Open post"));
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
      target.append(idReference("Media", item.media_id), node("span", "muted", " · "), mediaNavigationLink(item.media_id, "Open Media / Retry"));
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
      appendIngestCell(row, idReference("Ingest", item.id));
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
      if (item.media_id) result.append(idReference("Media", item.media_id));
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

  function revokeMediaPreviews() {
    for (const objectUrl of state.mediaPreviewUrls) revokePreviewObjectUrl(objectUrl, state.mediaPreviewUrls);
    state.mediaPreviewUrls.clear();
  }

  function revokePreviewObjectUrl(objectUrl, previewUrls) {
    if (!objectUrl) return;
    if (window.URL && typeof window.URL.revokeObjectURL === "function") window.URL.revokeObjectURL(objectUrl);
    previewUrls.delete(objectUrl);
  }

  function invalidateMediaPreviews() {
    state.mediaRenderGeneration += 1;
    revokeMediaPreviews();
  }

  function revokeSchedulePreview(postId) {
    const entry = state.schedulePreviewEntries.get(postId);
    if (!entry) return;
    entry.active = false;
    revokePreviewObjectUrl(entry.objectUrl, state.schedulePreviewUrls);
    entry.objectUrl = null;
    state.schedulePreviewEntries.delete(postId);
  }

  function invalidateSchedulePreviews() {
    for (const postId of state.schedulePreviewEntries.keys()) revokeSchedulePreview(postId);
  }

  function sameOriginApiPath(value) {
    if (!value) return null;
    try {
      const url = new URL(value, window.location.origin);
      if (url.origin !== window.location.origin || !url.pathname.startsWith("/api/v1/")) return null;
      return `${url.pathname}${url.search}`;
    } catch (_error) {
      return null;
    }
  }

  function mediaTagValue(tags) {
    return (tags || []).filter((tag) => typeof tag === "string").join(", ");
  }

  function parseMediaTags(value) {
    const tags = [];
    const seen = new Set();
    for (const part of String(value || "").split(",")) {
      const tag = part.trim();
      const normalized = tag.toLowerCase();
      if (tag && !seen.has(normalized)) {
        seen.add(normalized);
        tags.push(tag);
      }
    }
    return tags;
  }

  function captionSyncLabel(value) {
    return ({
      not_required: "Not required",
      pending: "Syncing",
      syncing: "Syncing",
      synced: "Synced",
      failed: "Sync failed",
    })[value] || "Unknown sync state";
  }

  function mediaPlaceholder(media, message) {
    const placeholder = node("div", "media-placeholder");
    placeholder.append(
      node("span", "media-placeholder-icon", String(media.kind || "media").slice(0, 3).toUpperCase()),
      node("span", "media-kind", media.kind || "MEDIA"),
      node("span", "muted", message || "No bounded preview"),
    );
    return placeholder;
  }

  async function loadMediaPreview(path, image, placeholder, isCurrent, previewUrls, previewEntry) {
    const safePath = sameOriginApiPath(path);
    if (!safePath || !window.URL || typeof window.URL.createObjectURL !== "function") return;
    if (!isCurrent()) return;
    let objectUrl = null;
    try {
      const response = await request(safePath, { headers: { Accept: "image/*" } });
      const blob = await response.blob();
      if (!blob || typeof blob.type !== "string" || !blob.type.startsWith("image/")) {
        throw new UiError("The preview response was not an image.");
      }
      objectUrl = window.URL.createObjectURL(blob);
      if (!isCurrent()) {
        revokePreviewObjectUrl(objectUrl, previewUrls);
        return;
      }
      previewUrls.add(objectUrl);
      previewEntry.objectUrl = objectUrl;
      image.addEventListener("error", () => {
        if (!isCurrent()) return;
        revokePreviewObjectUrl(objectUrl, previewUrls);
        if (previewEntry.objectUrl === objectUrl) previewEntry.objectUrl = null;
        image.hidden = true;
        placeholder.hidden = false;
        placeholder.lastChild.textContent = "Preview unavailable";
      }, { once: true });
      image.src = objectUrl;
      image.hidden = false;
      placeholder.hidden = true;
    } catch (_error) {
      if (objectUrl) revokePreviewObjectUrl(objectUrl, previewUrls);
      if (isCurrent()) {
        image.hidden = true;
        placeholder.hidden = false;
        placeholder.lastChild.textContent = "Preview unavailable";
      }
    }
  }

  function replaceMediaItem(updated) {
    const index = state.mediaItems.findIndex((item) => item.id === updated.id);
    if (index >= 0) state.mediaItems[index] = updated;
    renderMedia({ items: state.mediaItems, next_cursor: state.mediaCursor });
  }

  async function saveMediaMetadata(media, tagsInput, descriptionInput) {
    const updated = await api(`/api/v1/media/${encodeURIComponent(media.id)}`, {
      method: "PATCH",
      body: JSON.stringify({
        description: descriptionInput.value.trim() || null,
        tags: parseMediaTags(tagsInput.value),
        expected_updated_at: media.updated_at,
      }),
    });
    replaceMediaItem(updated);
    showToast("Catalogue edits saved; storage caption sync is durable.");
  }

  async function retryMediaCaptionSync(media) {
    const updated = await api(`/api/v1/media/${encodeURIComponent(media.id)}/caption-sync/retry`, { method: "POST" });
    replaceMediaItem(updated);
    showToast("Caption sync requeued.");
  }

  function renderMediaCard(media, renderGeneration) {
    const card = node("article", "media-card");
    card.dataset.mediaId = media.id || "";
    const visual = node("div", "media-visual");
    const placeholder = mediaPlaceholder(media);
    visual.append(placeholder);
    if (media.preview?.url) {
      const image = node("img");
      image.alt = `Bounded preview for ${media.kind || "media"}`;
      image.loading = "lazy";
      image.decoding = "async";
      image.hidden = true;
      visual.append(image);
      void loadMediaPreview(
        media.preview.url,
        image,
        placeholder,
        () => renderGeneration === state.mediaRenderGeneration,
        state.mediaPreviewUrls,
        { objectUrl: null },
      );
    }

    const main = node("div", "media-main");
    const heading = node("div", "media-heading");
    const title = media.title || `${media.kind || "Media"} item`;
    heading.append(node("h2", "", title), node("span", "media-kind", media.kind || "MEDIA"));
    main.append(heading);
    const meta = node("div", "media-meta");
    meta.append(idReference("Media", media.id), node("span", "meta", media.storage_state || "unknown"));
    if (media.file_size_bytes !== null && media.file_size_bytes !== undefined) meta.append(node("span", "meta", formatBytes(media.file_size_bytes)));
    if (media.duration_ms !== null && media.duration_ms !== undefined) meta.append(node("span", "meta", formatMediaDuration(media.duration_ms)));
    main.append(meta);

    const source = node("p", "media-source");
    source.append(node("strong", "", "Canonical source: "));
    source.append(media.source_url ? link(media.source_url, media.source_url) : node("span", "muted", "Not recorded"));
    main.append(source);
    const storage = node("p", "media-source");
    storage.append(node("strong", "", "Telegram storage: "));
    storage.append(media.storage_url ? link(media.storage_url, "Open in Telegram") : node("span", "muted", "Not ready"));
    main.append(storage);

    const tags = node("ul", "tag-list");
    if (media.tags?.length) {
      for (const tag of media.tags) tags.append(node("li", "tag", tag));
    } else {
      tags.append(node("li", "muted", "No tags"));
    }
    main.append(tags);

    const editor = node("form", "media-editor");
    const tagsLabel = node("label", "", "Tags · comma separated");
    const tagsInput = node("input");
    tagsInput.type = "text";
    tagsInput.value = mediaTagValue(media.tags);
    tagsLabel.append(tagsInput);
    const descriptionLabel = node("label", "", "Internal description");
    const descriptionInput = document.createElement("textarea");
    descriptionInput.rows = 3;
    descriptionInput.value = media.description || "";
    descriptionLabel.append(descriptionInput);
    const editorActions = node("div", "media-editor-actions");
    const sync = node("span", "sync-state", captionSyncLabel(media.caption_sync?.state));
    sync.dataset.state = media.caption_sync?.state || "";
    editorActions.append(sync);
    if (media.caption_sync?.error) editorActions.append(node("span", "muted", media.caption_sync.error));
    const saveButton = actionButton("Save edits", async () => saveMediaMetadata(media, tagsInput, descriptionInput), "button button-small button-primary");
    editorActions.append(saveButton);
    if (media.caption_sync?.state === "failed") {
      editorActions.append(actionButton("Retry sync", async () => retryMediaCaptionSync(media), "button button-small button-secondary"));
    }
    editor.append(tagsLabel, descriptionLabel, editorActions);
    editor.addEventListener("submit", (event) => {
      event.preventDefault();
      if (!saveButton.disabled) saveButton.click();
    });
    main.append(editor);

    const publication = node("div", "publication-actions");
    if (media.storage_state === "ready") {
      publication.append(
        actionButton("Post now", async () => submitPublication(media, "post_now")),
        actionButton("Post now…", () => openPublicationDialog(media, "post_now")),
        actionButton("Queue", async () => submitPublication(media, "queue")),
        actionButton("Queue…", () => openPublicationDialog(media, "queue_exact")),
      );
    } else {
      publication.append(node("span", "muted", "Publication actions appear when storage is ready."));
    }
    main.append(publication);
    card.append(visual, main);
    return card;
  }

  function renderMedia(data) {
    invalidateMediaPreviews();
    const renderGeneration = state.mediaRenderGeneration;
    state.mediaItems = data.items || [];
    state.mediaCursor = data.next_cursor || null;
    const grid = $("media-grid");
    clear(grid);
    if (!state.mediaItems.length) {
      grid.append(node("p", "empty-state media-empty", state.mediaQuery ? "No exact media match." : "No media found."));
    } else {
      for (const media of state.mediaItems) grid.append(renderMediaCard(media, renderGeneration));
    }
    const status = $("media-status");
    if (state.mediaQuery) {
      status.textContent = `Exact lookup: ${state.mediaQuery}`;
      status.hidden = false;
    } else {
      status.hidden = true;
    }
    $("media-next").hidden = !state.mediaCursor;
  }

  async function loadMedia(cursor) {
    let path = "/api/v1/media?limit=50";
    if (state.mediaQuery) path += `&q=${encodeURIComponent(state.mediaQuery)}`;
    if (cursor) path += `&cursor=${encodeURIComponent(cursor)}`;
    const data = await api(path);
    renderMedia(data);
  }

  function scheduleModeLabel(value) {
    return value === "explicit" ? "Exact time" : "Cadence";
  }

  function scheduleLocalInput(value) {
    if (!value) return "";
    const date = new Date(value);
    return Number.isNaN(date.valueOf()) ? "" : localDateTimeValue(date);
  }

  function schedulePlaceholder(item) {
    return mediaPlaceholder({ kind: item.media_kind || "media" }, "Preview on Media");
  }

  function markScheduleEditing(postId) {
    state.scheduleEditing.add(postId);
  }

  async function scheduleMutation(item, operation) {
    try {
      const result = await operation();
      state.scheduleEditing.delete(item.id);
      await loadSchedule(null);
      return result;
    } catch (error) {
      if (error instanceof UiError && error.status === 409) state.scheduleEditing.delete(item.id);
      throw error;
    }
  }

  function discardScheduleEdits() {
    state.scheduleEditing.clear();
    const notice = $("schedule-notice");
    if (notice) notice.hidden = true;
  }

  function scheduleRevisionBody(item) {
    return {
      expected_revision: item.revision,
      expected_updated_at: item.updated_at,
    };
  }

  function scheduleIdempotencyHeaders() {
    return { "Idempotency-Key": `admin-ui:${randomKey()}` };
  }

  function renderScheduleCard(item) {
    const card = node("article", "schedule-card");
    card.dataset.postId = item.id || "";
    const visual = node("div", "media-visual schedule-visual");
    const placeholder = schedulePlaceholder(item);
    visual.append(placeholder);
    const previewEntry = { active: true, objectUrl: null };
    state.schedulePreviewEntries.set(item.id, previewEntry);
    if (item.preview?.url) {
      const image = node("img");
      image.alt = `Bounded preview for ${item.media_kind || "media"}`;
      image.loading = "lazy";
      image.decoding = "async";
      image.hidden = true;
      visual.append(image);
      void loadMediaPreview(
        item.preview.url,
        image,
        placeholder,
        () => state.page === "schedule"
          && previewEntry.active
          && state.schedulePreviewEntries.get(item.id) === previewEntry,
        state.schedulePreviewUrls,
        previewEntry,
      );
    }

    const main = node("div", "schedule-main");
    const heading = node("div", "schedule-heading");
    const title = node("h2");
    title.append(node("span", "", `${item.media_kind || "Media"} · `), idReference("Media", item.media_id));
    heading.append(title, node("span", "schedule-mode", scheduleModeLabel(item.schedule_mode)));
    main.append(heading);

    const statusRow = node("div", "schedule-status-row");
    const status = node("span", "state schedule-state", item.status || "unknown");
    status.dataset.state = item.status || "unknown";
    statusRow.append(status, idReference("Post", item.id));
    if (item.channel_name) statusRow.append(node("span", "meta", item.channel_name));
    main.append(statusRow);

    const source = node("p", "media-source");
    source.append(node("strong", "", "Canonical source: "));
    source.append(item.source_url ? link(item.source_url, item.source_url) : node("span", "muted", "Not recorded"));
    main.append(source);
    const storage = node("p", "media-source");
    storage.append(node("strong", "", "Telegram storage: "));
    storage.append(item.storage_url ? link(item.storage_url, "Open in Telegram") : node("span", "muted", "Not ready"));
    main.append(storage);

    const timing = node("p", "schedule-timing");
    timing.append(
      node("strong", "", `${scheduleModeLabel(item.schedule_mode)}: `),
      node("span", "", formatDate(item.scheduled_at)),
    );
    main.append(timing);

    const editable = ["draft", "queued", "failed"].includes(item.status);
    if (editable) {
      const editor = node("form", "schedule-editor");
      const captionLabel = node("label", "", "Public post text");
      const caption = document.createElement("textarea");
      caption.rows = 4;
      caption.maxLength = 1024;
      caption.value = item.caption || "";
      caption.addEventListener("input", () => markScheduleEditing(item.id));
      captionLabel.append(caption);

      const timeLabel = node("label", "", "Exact future local time");
      const timeInput = document.createElement("input");
      timeInput.type = "datetime-local";
      timeInput.value = item.schedule_mode === "explicit" ? scheduleLocalInput(item.scheduled_at) : "";
      timeInput.min = localDateTimeValue(new Date(Date.now() + 60_000));
      timeInput.addEventListener("input", () => markScheduleEditing(item.id));
      timeLabel.append(timeInput);
      editor.append(
        captionLabel,
        timeLabel,
        node("span", "muted", "Save text separately. Setting an exact time permits collisions and bypasses cadence rules."),
      );

      const actions = node("div", "schedule-actions");
      actions.append(
        actionButton("Save text", async () => {
          await scheduleMutation(item, () => api(`/api/v1/posts/${encodeURIComponent(item.id)}`, {
            method: "PATCH",
            body: JSON.stringify({
              ...scheduleRevisionBody(item),
              caption: caption.value.trim() || null,
            }),
          }));
          showToast("Public post text saved.");
        }, "button button-small button-primary"),
        actionButton("Clear text", async () => {
          caption.value = "";
          markScheduleEditing(item.id);
          await scheduleMutation(item, () => api(`/api/v1/posts/${encodeURIComponent(item.id)}`, {
            method: "PATCH",
            body: JSON.stringify({ ...scheduleRevisionBody(item), caption: null }),
          }));
          showToast("Public post text cleared.");
        }, "button button-small button-secondary"),
        actionButton("Set exact time", async () => {
          const publishAt = localFutureTimeToIso(timeInput.value);
          await scheduleMutation(item, () => api(`/api/v1/posts/${encodeURIComponent(item.id)}/schedule-exact`, {
            method: "POST",
            headers: scheduleIdempotencyHeaders(),
            body: JSON.stringify({ publish_at: publishAt, expected_revision: item.revision }),
          }));
          showToast("Exact publication time saved.");
        }, "button button-small button-secondary"),
        actionButton("Post now", async () => {
          await scheduleMutation(item, () => api(`/api/v1/posts/${encodeURIComponent(item.id)}/publish`, {
            method: "POST",
            headers: scheduleIdempotencyHeaders(),
            body: JSON.stringify({ expected_revision: item.revision }),
          }));
          showToast("Post now requested.");
        }, "button button-small button-primary"),
        actionButton("Remove", async () => {
          await scheduleMutation(item, () => api(`/api/v1/posts/${encodeURIComponent(item.id)}/cancel`, {
            method: "POST",
            body: JSON.stringify({ expected_revision: item.revision }),
          }));
          showToast("Scheduled post removed; media was kept.");
        }, "button button-small button-danger"),
      );
      editor.append(actions);
      main.append(editor);
    } else {
      const caption = node("p", "schedule-caption", item.caption || "No public post text.");
      main.append(caption, node("p", "schedule-readonly", "Read-only while this send state is not safely reversible."));
    }

    card.append(visual, main);
    return card;
  }

  function renderSchedule(data) {
    const items = (data.items || []).filter((item) => !["published", "cancelled"].includes(item.status));
    const list = $("schedule-list");
    state.scheduleItems = items;
    state.scheduleCursor = data.next_cursor || null;
    const existingCards = new Map(
      [...list.children]
        .filter((child) => child.dataset.postId)
        .map((child) => [child.dataset.postId, child]),
    );
    const preservedPostIds = new Set(
      items
        .filter((item) => existingCards.has(item.id) && state.scheduleEditing.has(item.id))
        .map((item) => item.id),
    );
    for (const postId of state.schedulePreviewEntries.keys()) {
      if (!preservedPostIds.has(postId)) revokeSchedulePreview(postId);
    }
    clear(list);
    if (!items.length) {
      list.append(node("p", "empty-state", "No unpublished schedule work."));
    } else {
      for (const item of items) {
        const existingCard = existingCards.get(item.id);
        if (existingCard && state.scheduleEditing.has(item.id)) {
          list.append(existingCard);
        } else {
          state.scheduleEditing.delete(item.id);
          list.append(renderScheduleCard(item));
        }
      }
    }
    for (const postId of state.scheduleEditing) {
      if (!items.some((item) => item.id === postId)) state.scheduleEditing.delete(postId);
    }
    const notice = $("schedule-notice");
    const preservedEdit = items.some((item) => state.scheduleEditing.has(item.id));
    notice.textContent = "Schedule refreshed without replacing active forms. Save their edits or leave the page to reload them.";
    notice.hidden = !preservedEdit;
    $("schedule-next").hidden = !state.scheduleCursor;
  }

  async function loadSchedule(cursor) {
    let path = "/api/v1/posts?limit=50";
    if (cursor) path += `&cursor=${encodeURIComponent(cursor)}`;
    const data = await api(path);
    renderSchedule(data);
  }

  function localDateTimeValue(date) {
    const pad = (value) => String(value).padStart(2, "0");
    return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
  }

  function localFutureTimeToIso(value) {
    const date = new Date(value);
    if (!value || Number.isNaN(date.valueOf()) || date.getTime() <= Date.now()) {
      throw new UiError("Choose a future local time for exact queueing.");
    }
    return date.toISOString();
  }

  function openPublicationDialog(media, action) {
    state.publication = { media, action };
    const dialog = $("publication-dialog");
    const exact = action === "queue_exact";
    $("publication-dialog-title").textContent = exact ? "Queue at an exact time" : "Post now with public text";
    const context = $("publication-dialog-context");
    clear(context);
    context.append(idReference("Media", media.id), node("span", "", " · internal description and tags stay separate."));
    $("publication-caption").value = "";
    $("publication-time").value = "";
    $("publication-time").min = localDateTimeValue(new Date(Date.now() + 60_000));
    $("publication-time").required = exact;
    $("publication-time-field").hidden = !exact;
    $("publication-error").hidden = true;
    $("publication-submit").textContent = exact ? "Queue intent" : "Post intent";
    if (!dialog.open) dialog.showModal();
    $("publication-caption").focus();
  }

  function closePublicationDialog() {
    state.publication = null;
    const dialog = $("publication-dialog");
    if (dialog.open) dialog.close();
  }

  async function submitPublication(media, action, caption, requestedPublishAt) {
    const body = { requested_action: action === "queue_exact" ? "queue" : action };
    if (caption) body.requested_post_caption = caption;
    if (requestedPublishAt) body.requested_publish_at = requestedPublishAt;
    const result = await api(`/api/v1/media/${encodeURIComponent(media.id)}/publication-intent`, {
      method: "POST",
      headers: { "Idempotency-Key": `admin-ui:${randomKey()}` },
      body: JSON.stringify(body),
    });
    if (result?.state === "draft" && result.repeat_evidence) {
      showToast("Publication repeat needs a decision on the Dashboard.");
      window.location.hash = "#dashboard";
      await route();
    } else {
      showToast("Publication intent saved.");
    }
  }

  async function submitPublicationDialog(event) {
    event.preventDefault();
    const current = state.publication;
    if (!current) return;
    const caption = $("publication-caption").value.trim() || undefined;
    let requestedPublishAt;
    try {
      if (current.action === "queue_exact") requestedPublishAt = localFutureTimeToIso($("publication-time").value);
    } catch (error) {
      $("publication-error").textContent = error instanceof Error ? error.message : "The exact time is invalid.";
      $("publication-error").hidden = false;
      return;
    }
    const button = $("publication-submit");
    closePublicationDialog();
    await withBusy(button, () => submitPublication(current.media, current.action, caption, requestedPublishAt));
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
    if (state.page === "media") invalidateMediaPreviews();
    const requested = window.location.hash.slice(1);
    const nextPage = PAGE_NAMES.has(requested) ? requested : "dashboard";
    if (state.page === "schedule" && nextPage !== "schedule") {
      invalidateSchedulePreviews();
      discardScheduleEdits();
    }
    state.page = nextPage;
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
      if (state.page === "media") {
        $("media-search").value = state.mediaQuery;
        await loadMedia(null);
      }
      if (state.page === "schedule") await loadSchedule(null);
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
  $("media-refresh").addEventListener("click", (event) => { void withBusy(event.currentTarget, () => loadMedia(null)); });
  $("media-search-form").addEventListener("submit", (event) => {
    event.preventDefault();
    state.mediaQuery = $("media-search").value.trim();
    void withBusy($("media-search-form").querySelector("button"), () => loadMedia(null));
  });
  $("media-clear-search").addEventListener("click", (event) => {
    state.mediaQuery = "";
    $("media-search").value = "";
    void withBusy(event.currentTarget, () => loadMedia(null));
  });
  $("media-next").addEventListener("click", (event) => {
    const cursor = state.mediaCursor;
    if (cursor) void withBusy(event.currentTarget, () => loadMedia(cursor));
  });
  $("schedule-refresh").addEventListener("click", (event) => { void withBusy(event.currentTarget, () => loadSchedule(null)); });
  $("schedule-next").addEventListener("click", (event) => {
    if (state.scheduleEditing.size) {
      showToast("Save active schedule edits before opening another page.", true);
      return;
    }
    const cursor = state.scheduleCursor;
    if (cursor) void withBusy(event.currentTarget, () => loadSchedule(cursor));
  });
  $("settings-refresh").addEventListener("click", (event) => { void withBusy(event.currentTarget, loadSettings); });
  $("settings-form").addEventListener("submit", (event) => { void saveSettings(event); });
  $("publication-form").addEventListener("submit", (event) => { void submitPublicationDialog(event); });
  $("publication-cancel").addEventListener("click", closePublicationDialog);
  window.addEventListener("hashchange", () => { void route(); });
  window.addEventListener("pagehide", () => {
    stopIngestAutoRefresh();
    invalidateMediaPreviews();
    invalidateSchedulePreviews();
    discardScheduleEdits();
  });

  setAuthView(Boolean(state.token));
  void route();
})();
