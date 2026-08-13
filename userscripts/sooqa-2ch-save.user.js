// ==UserScript==
// @name         sooqa: 2ch media actions
// @namespace    sooqa
// @version      0.2.0
// @description  Add save, queue, and post-now controls to direct 2ch media.
// @match        https://2ch.su/*
// @match        https://2ch.org/*
// @match        https://2ch.life/*
// @updateURL    https://raw.githubusercontent.com/godlikedh/sooqa/main/userscripts/sooqa-2ch-save.user.js
// @downloadURL  https://raw.githubusercontent.com/godlikedh/sooqa/main/userscripts/sooqa-2ch-save.user.js
// @grant        GM_getValue
// @grant        GM_setValue
// @grant        GM_xmlhttpRequest
// @connect      127.0.0.1
// @run-at       document-idle
// ==/UserScript==

(function (root, factory) {
  if (typeof module === "object" && module.exports) {
    module.exports = factory();
  } else {
    factory().boot(root);
  }
})(typeof globalThis === "object" ? globalThis : this, function () {
  "use strict";

  const COMPANION_ENDPOINT = "http://127.0.0.1:47831/v1/submit";
  const TOKEN_STORAGE_KEY = "sooqa_companion_token";
  const HISTORY_STORAGE_KEY = "sooqa_accepted_actions_v1";
  const HISTORY_CONTROL_KEY = "sooqa_history_control";
  const MAX_HISTORY_ENTRIES = 200;
  const MAX_HISTORY_KEY_CHARS = 2_048;
  const SUPPORTED_MEDIA = /\.(?:mp4|webm)$/i;
  const MIRROR_HOSTS = new Set(["2ch.su", "2ch.org", "2ch.life"]);

  const ACTIONS = [
    { key: "post_now", requestAction: "post_now", label: "Post now", detailed: false },
    { key: "post_now_detailed", requestAction: "post_now", label: "Post now…", detailed: true, publicText: true },
    { key: "queue", requestAction: "queue", label: "Queue", detailed: false },
    { key: "queue_exact", requestAction: "queue", label: "Queue…", detailed: true, publicText: true, exactTime: true },
    { key: "save", requestAction: "save", label: "Save", detailed: false },
    { key: "save_detailed", requestAction: "save", label: "Save…", detailed: true },
  ];

  function normalizeDirectMediaUrl(value, baseHref) {
    if (!value) return null;
    try {
      const url = new URL(value, baseHref);
      if (url.protocol !== "http:" && url.protocol !== "https:") return null;
      if (!SUPPORTED_MEDIA.test(url.pathname)) return null;
      return url.href;
    } catch (_error) {
      return null;
    }
  }

  function canonicalizeUrl(value) {
    try {
      const url = new URL(value);
      if (MIRROR_HOSTS.has(url.hostname.toLowerCase())) url.hostname = "2ch.org";
      url.hash = "";
      return url.href;
    } catch (_error) {
      return String(value || "");
    }
  }

  function extractDirectAttachmentUrls(nodes, baseHref) {
    const urls = [];
    for (const node of Array.from(nodes || [])) {
      const value = node.href || node.src || (node.getAttribute && node.getAttribute("href"));
      const url = normalizeDirectMediaUrl(value, baseHref);
      if (url && !urls.includes(url)) urls.push(url);
    }
    return urls;
  }

  function parseTags(value) {
    const tags = [];
    for (const raw of String(value || "").split(",")) {
      const tag = raw.trim().toLowerCase();
      if (tag && !tags.includes(tag)) tags.push(tag);
    }
    return tags;
  }

  function localDateTimeToRfc3339(value, now = new Date()) {
    const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})$/.exec(String(value || ""));
    if (!match) return null;
    const [, yearText, monthText, dayText, hourText, minuteText] = match;
    const year = Number(yearText);
    const month = Number(monthText);
    const day = Number(dayText);
    const hour = Number(hourText);
    const minute = Number(minuteText);
    const date = new Date(year, month - 1, day, hour, minute, 0, 0);
    if (Number.isNaN(date.getTime()) || date <= now) return null;
    if (
      date.getFullYear() !== year ||
      date.getMonth() !== month - 1 ||
      date.getDate() !== day ||
      date.getHours() !== hour ||
      date.getMinutes() !== minute
    ) {
      return null;
    }
    return date.toISOString();
  }

  function buildPayload({
    actionId,
    mediaUrl,
    pageUrl,
    pageTitle,
    action,
    metadata = null,
  }) {
    const payload = {
      action_id: actionId,
      url: mediaUrl,
      page_url: pageUrl || null,
      page_title: pageTitle || null,
      requested_action: action.requestAction,
    };
    if (metadata) {
      const description = String(metadata.description || "").trim();
      const tags = Array.isArray(metadata.tags) ? metadata.tags.filter(Boolean) : [];
      if (description) payload.description = description;
      if (tags.length) payload.tags = tags;
      if (action.publicText) {
        const publicText = String(metadata.publicText || "").trim();
        if (publicText) payload.requested_post_caption = publicText;
      }
      if (action.exactTime && metadata.requestedPublishAt) {
        payload.requested_publish_at = metadata.requestedPublishAt;
      }
    }
    return payload;
  }

  function findPostContainer(node) {
    const post = node.closest && node.closest(
      "article, .post, .thread[data-num], .thread, [data-num], [id^='p']"
    );
    if (post) return post;
    const parent = node.parentElement;
    if (parent && (parent.tagName === "VIDEO" || parent.tagName === "SOURCE")) {
      return parent.parentElement;
    }
    return parent;
  }

  function findAttachmentTarget(node) {
    if (
      node &&
      node.tagName === "SOURCE" &&
      node.parentElement &&
      node.parentElement.tagName === "VIDEO"
    ) {
      return node.parentElement;
    }
    return node;
  }

  function findThreadContainer(node) {
    return node && node.closest
      ? node.closest(".thread[data-num], .thread, [data-thread-id], [data-thread-url]")
      : null;
  }

  function threadNumber(thread) {
    if (!thread || !thread.getAttribute) return null;
    return (
      thread.getAttribute("data-num") ||
      thread.getAttribute("data-thread-id") ||
      thread.getAttribute("data-thread-number") ||
      null
    );
  }

  function threadUrlFromElement(thread, baseHref) {
    if (!thread) return null;
    const explicit = [
      thread.getAttribute && thread.getAttribute("data-thread-url"),
      thread.getAttribute && thread.getAttribute("data-url"),
    ].find(Boolean);
    if (explicit) return new URL(explicit, baseHref).href;

    const links = thread.querySelectorAll ? Array.from(thread.querySelectorAll("a[href]")) : [];
    for (const link of links) {
      const href = link.getAttribute && link.getAttribute("href");
      if (!href || normalizeDirectMediaUrl(href, baseHref)) continue;
      try {
        const url = new URL(href, baseHref);
        if (/\/(?:res|thread)\//i.test(url.pathname)) return url.href;
      } catch (_error) {
        // Ignore malformed links and continue to the data-number fallback.
      }
    }
    return null;
  }

  function deriveThreadUrl(node, env) {
    const thread = findThreadContainer(node);
    if (!thread) return canonicalizeUrl(env.location.href);

    const explicit = threadUrlFromElement(thread, env.location.href);
    if (explicit) return canonicalizeUrl(explicit);

    const number = threadNumber(thread);
    if (!number) return canonicalizeUrl(env.location.href);

    try {
      const url = new URL(env.location.href);
      const encodedNumber = encodeURIComponent(number);
      const resourceMatch = /^(.*\/res\/)[^/]+(?:\.html)?\/?$/i.exec(url.pathname);
      if (resourceMatch) {
        url.pathname = resourceMatch[1] + encodedNumber + ".html";
      } else {
        const directory = url.pathname.endsWith("/")
          ? url.pathname
          : url.pathname.replace(/[^/]*$/, "");
        url.pathname = directory + "res/" + encodedNumber + ".html";
      }
      url.search = "";
      url.hash = "";
      return canonicalizeUrl(url.href);
    } catch (_error) {
      return canonicalizeUrl(env.location.href);
    }
  }

  function figureMediaUrl(figure, baseHref) {
    const preferred = figure.querySelectorAll
      ? Array.from(figure.querySelectorAll("a.post__image-link"))
      : [];
    const linkedPreview = figure.querySelectorAll
      ? Array.from(figure.querySelectorAll("a[href]")).filter(
        (link) => link.querySelector && link.querySelector("img, video, source")
      )
      : [];
    const mediaNodes = figure.querySelectorAll
      ? Array.from(figure.querySelectorAll("video[src], source[src]"))
      : [];
    return extractDirectAttachmentUrls(
      [...preferred, ...linkedPreview, ...mediaNodes],
      baseHref
    )[0] || null;
  }

  function attachmentCandidates(root, env) {
    const candidates = [];
    const figures = [];
    if (root.matches && root.matches("figure.post__image")) figures.push(root);
    figures.push(...Array.from(root.querySelectorAll("figure.post__image")));
    for (const figure of figures) {
      const mediaUrl = figureMediaUrl(figure, env.location.href);
      if (mediaUrl) candidates.push({ node: figure, target: figure, mediaUrl });
    }

    const nodes = [];
    if (root.matches && root.matches("a[href], video[src], source[src]")) nodes.push(root);
    nodes.push(...Array.from(root.querySelectorAll("a[href], video[src], source[src]")));
    for (const node of nodes) {
      if (node.closest && node.closest("figure.post__image")) continue;
      const target = findAttachmentTarget(node);
      const mediaUrl = extractDirectAttachmentUrls([node], env.location.href)[0];
      if (target && mediaUrl) candidates.push({ node, target, mediaUrl });
    }
    return candidates;
  }

  function makeButton(document, label, className, type = "button") {
    const button = document.createElement("button");
    button.type = type;
    button.className = className;
    button.textContent = label;
    return button;
  }

  function dialogSupported(document) {
    const dialog = document.createElement("dialog");
    return typeof dialog.showModal === "function";
  }

  function collectMetadata(env, action) {
    const wantsPublicText = Boolean(action.publicText);
    const wantsExactTime = Boolean(action.exactTime);
    if (!dialogSupported(env.document)) {
      const promptParts = ["Tags (comma-separated)", "Internal description"];
      if (wantsPublicText) promptParts.push("Public post text");
      if (wantsExactTime) promptParts.push("Local date/time (YYYY-MM-DDTHH:MM)");
      const value = env.prompt(promptParts.join(" | ") + ":", "");
      if (value === null) return Promise.resolve(null);
      const parts = value.split("|");
      const metadata = {
        tags: parseTags(parts.shift() || ""),
        description: String(parts.shift() || "").trim(),
      };
      if (wantsPublicText) metadata.publicText = String(parts.shift() || "").trim();
      if (wantsExactTime) {
        metadata.requestedPublishAt = localDateTimeToRfc3339(
          String(parts.shift() || "").trim(),
          env.now ? env.now() : new Date()
        );
        if (!metadata.requestedPublishAt) {
          if (env.setStatus) env.setStatus("Enter a future local date/time");
          return Promise.resolve(null);
        }
      }
      return Promise.resolve(metadata);
    }

    return new Promise((resolve) => {
      const dialog = env.document.createElement("dialog");
      dialog.className = "sooqa-metadata-dialog";
      const form = env.document.createElement("form");
      form.method = "dialog";
      const fields = [];

      const addField = (labelText, type, name, multiline = false) => {
        const label = env.document.createElement("label");
        label.textContent = labelText;
        const field = env.document.createElement(multiline ? "textarea" : "input");
        field.type = type;
        field.name = name;
        field.autocomplete = "off";
        if (multiline) field.rows = 3;
        label.append(field);
        form.append(label);
        fields.push(field);
        return field;
      };

      const tags = addField("Tags (comma-separated)", "text", "tags");
      const description = addField("Internal description", "text", "description", true);
      const publicText = wantsPublicText
        ? addField("Public post text", "text", "publicText", true)
        : null;
      const requestedPublishAt = wantsExactTime
        ? addField("Local date/time", "datetime-local", "requestedPublishAt")
        : null;
      if (requestedPublishAt) requestedPublishAt.required = true;

      const error = env.document.createElement("div");
      error.className = "sooqa-dialog-error";
      const actions = env.document.createElement("div");
      const cancel = makeButton(env.document, "Cancel", "sooqa-cancel");
      cancel.value = "cancel";
      const confirm = makeButton(env.document, "Send", "sooqa-confirm", "submit");
      confirm.value = "send";
      actions.append(cancel, confirm);
      form.append(error, actions);
      dialog.append(form);

      const finish = (value) => {
        dialog.remove();
        resolve(value);
      };
      form.addEventListener("submit", (event) => {
        event.preventDefault();
        const metadata = {
          description: String(description.value || "").trim(),
          tags: parseTags(tags.value),
        };
        if (publicText) metadata.publicText = String(publicText.value || "").trim();
        if (requestedPublishAt) {
          metadata.requestedPublishAt = localDateTimeToRfc3339(
            requestedPublishAt.value,
            env.now ? env.now() : new Date()
          );
          if (!metadata.requestedPublishAt) {
            error.textContent = "Enter a future local date/time";
            requestedPublishAt.focus();
            return;
          }
        }
        finish(metadata);
        dialog.close();
      });
      cancel.addEventListener("click", (event) => {
        event.preventDefault();
        finish(null);
        dialog.close();
      });
      dialog.addEventListener("cancel", (event) => {
        event.preventDefault();
        finish(null);
        dialog.close();
      });
      env.document.body.append(dialog);
      dialog.showModal();
      fields[0].focus();
    });
  }

  function loadAcceptedHistory(env) {
    if (env.history) return env.history;
    let value = [];
    if (typeof env.GM_getValue === "function") {
      try {
        const stored = env.GM_getValue(HISTORY_STORAGE_KEY, []);
        if (Array.isArray(stored)) value = stored;
        else if (typeof stored === "string") value = JSON.parse(stored);
      } catch (_error) {
        value = [];
      }
    }
    env.history = value
      .filter((entry) => (
        entry &&
        typeof entry.threadKey === "string" &&
        typeof entry.mediaKey === "string" &&
        typeof entry.actionId === "string" &&
        entry.threadKey.length <= MAX_HISTORY_KEY_CHARS &&
        entry.mediaKey.length <= MAX_HISTORY_KEY_CHARS
      ))
      .slice(-MAX_HISTORY_ENTRIES);
    return env.history;
  }

  function saveAcceptedHistory(env) {
    if (typeof env.GM_setValue !== "function") return;
    try {
      env.GM_setValue(HISTORY_STORAGE_KEY, loadAcceptedHistory(env));
    } catch (_error) {
      // History is advisory; storage failures must not block capture.
    }
  }

  function renderHistory(panel, env) {
    const historyElement = panel.querySelector(".sooqa-history");
    if (!historyElement) return;
    const threadKey = panel.dataset.sooqaThreadKey;
    const mediaKey = panel.dataset.sooqaMediaKey;
    const entries = loadAcceptedHistory(env).filter(
      (entry) => entry.threadKey === threadKey && entry.mediaKey === mediaKey
    );
    if (!entries.length) {
      historyElement.textContent = "No accepted actions yet";
      return;
    }
    const recent = entries.slice(-3).map((entry) => {
      const label = entry.actionLabel || entry.requestedAction || "action";
      return label + " (" + entry.acceptedAt + ")";
    });
    historyElement.textContent = "Accepted requests: " + recent.join(", ");
  }

  function renderAllHistory(env) {
    for (const panel of Array.from(env.document.querySelectorAll(".sooqa-action-panel"))) {
      renderHistory(panel, env);
    }
  }

  function recordAcceptedAction(env, panel, action, actionId) {
    const history = loadAcceptedHistory(env);
    const marker = {
      threadKey: panel.dataset.sooqaThreadKey,
      mediaKey: panel.dataset.sooqaMediaKey,
      actionId,
      requestedAction: action.requestAction,
      actionLabel: action.label,
      acceptedAt: new Date().toISOString(),
    };
    const remaining = history.filter(
      (entry) => !(
        entry.threadKey === marker.threadKey &&
        entry.mediaKey === marker.mediaKey &&
        entry.actionId === marker.actionId
      )
    );
    remaining.push(marker);
    env.history = remaining.slice(-MAX_HISTORY_ENTRIES);
    saveAcceptedHistory(env);
    renderAllHistory(env);
  }

  function clearAcceptedHistory(env) {
    env.history = [];
    saveAcceptedHistory(env);
    renderAllHistory(env);
  }

  function ensureHistoryControl(env) {
    const existing = env.document.body.querySelector("." + HISTORY_CONTROL_KEY);
    if (existing) return;
    const button = makeButton(env.document, "Clear accepted history", HISTORY_CONTROL_KEY);
    button.addEventListener("click", () => {
      clearAcceptedHistory(env);
      button.textContent = "Accepted history cleared";
    });
    env.document.body.append(button);
  }

  function ensureStyles(env) {
    const existing = Array.from(env.document.querySelectorAll("style")).some(
      (style) => style.dataset && style.dataset.sooqaStyles === "true"
    );
    if (existing) return;
    const style = env.document.createElement("style");
    style.dataset.sooqaStyles = "true";
    style.textContent = [
      ".sooqa-attachment-row {",
      "  box-sizing: border-box;",
      "  display: flex;",
      "  flex: 0 0 100%;",
      "  align-items: flex-start;",
      "  gap: 0.75rem;",
      "  width: 100%;",
      "  margin: 0.5rem 0;",
      "  grid-column: 1 / -1;",
      "}",
      ".sooqa-attachment-preview { flex: 0 1 auto; min-width: 0; }",
      ".sooqa-action-panel {",
      "  display: grid;",
      "  grid-template-columns: repeat(2, minmax(7rem, max-content));",
      "  flex: 0 0 auto;",
      "  gap: 0.35rem;",
      "  align-items: start;",
      "}",
      ".sooqa-action-panel button { min-height: 2rem; padding: 0.25rem 0.5rem; }",
      ".sooqa-action-status, .sooqa-history { grid-column: 1 / -1; font-size: 0.85rem; }",
      ".sooqa-dialog-error { min-height: 1.2em; color: #a00; }",
      ".sooqa-history-control { margin: 0.5rem 0; }",
      "@media (max-width: 42rem) { .sooqa-attachment-row { flex-wrap: wrap; } }",
    ].join("\n");
    (env.document.head || env.document.body).append(style);
  }

  function setButtonsDisabled(panel, disabled) {
    for (const button of Array.from(panel.querySelectorAll("button[data-sooqa-action]"))) {
      button.disabled = disabled;
    }
  }

  function findExistingPanel(container, mediaKey) {
    return Array.from(container.querySelectorAll(".sooqa-action-panel")).find(
      (panel) => panel.dataset.sooqaMediaKey === mediaKey
    );
  }

  function createActionPanel(env, mediaUrl, threadUrl) {
    const mediaKey = canonicalizeUrl(mediaUrl);
    const threadKey = canonicalizeUrl(threadUrl);
    const panel = env.document.createElement("span");
    panel.className = "sooqa-action-panel";
    panel.dataset.sooqaMediaUrl = mediaUrl;
    panel.dataset.sooqaMediaKey = mediaKey;
    panel.dataset.sooqaThreadKey = threadKey;

    const status = env.document.createElement("span");
    status.className = "sooqa-action-status";
    const historyElement = env.document.createElement("span");
    historyElement.className = "sooqa-history";
    const state = { retry: null, sending: false, collecting: false };

    const setStatus = (value) => {
      status.textContent = value;
    };
    const makeAttempt = (action, metadata) => {
      const actionId = env.crypto.randomUUID();
      const payload = buildPayload({
        actionId,
        mediaUrl,
        pageUrl: threadUrl,
        pageTitle: env.document.title,
        action,
        metadata,
      });
      return { actionKey: action.key, actionId, payload };
    };

    const submit = async (action) => {
      if (state.sending || state.collecting) return;
      if (state.retry && state.retry.actionKey !== action.key) state.retry = null;
      if (!state.retry) {
        let metadata = null;
        if (action.detailed) {
          state.collecting = true;
          setButtonsDisabled(panel, true);
          try {
            metadata = await collectMetadata({ ...env, setStatus }, action);
          } finally {
            state.collecting = false;
            if (!state.sending) setButtonsDisabled(panel, false);
          }
          if (!metadata || state.sending || state.retry) return;
        }
        if (state.sending || state.collecting || state.retry) return;
        state.retry = makeAttempt(action, metadata);
      }

      const attempt = state.retry;
      state.sending = true;
      setButtonsDisabled(panel, true);
      const token = typeof env.GM_getValue === "function"
        ? env.GM_getValue(TOKEN_STORAGE_KEY, "")
        : "";
      if (!token) {
        state.sending = false;
        setButtonsDisabled(panel, false);
        setStatus("Configure companion token");
        return;
      }
      if (typeof env.GM_xmlhttpRequest !== "function") {
        state.sending = false;
        setButtonsDisabled(panel, false);
        setStatus("Tampermonkey request API unavailable");
        return;
      }

      setStatus("Sending…");
      const finishFailure = (message) => {
        state.sending = false;
        setButtonsDisabled(panel, false);
        setStatus(message);
      };
      try {
        env.GM_xmlhttpRequest({
          method: "POST",
          url: COMPANION_ENDPOINT,
          headers: {
            Authorization: "Bearer " + token,
            "Content-Type": "application/json",
          },
          data: JSON.stringify(attempt.payload),
          timeout: 15_000,
          onload(response) {
            state.sending = false;
            setButtonsDisabled(panel, false);
            state.retry = null;
            if (response.status >= 200 && response.status < 300) {
              setStatus("Accepted request");
              recordAcceptedAction(env, panel, action, attempt.actionId);
              return;
            }
            setStatus("Request failed; try again");
          },
          onerror() {
            finishFailure("Request failed; retry");
          },
          ontimeout() {
            finishFailure("Request timed out; retry");
          },
        });
      } catch (_error) {
        finishFailure("Request failed; retry");
      }
    };

    for (const action of ACTIONS) {
      const button = makeButton(env.document, action.label, "sooqa-action-" + action.key);
      button.dataset.sooqaAction = action.key;
      button.addEventListener("click", () => submit(action));
      panel.append(button);
    }
    panel.append(status, historyElement);
    renderHistory(panel, env);
    return panel;
  }

  function decorate(root, env) {
    loadAcceptedHistory(env);
    ensureStyles(env);
    const decorated = new Set();
    for (const { target, mediaUrl } of attachmentCandidates(root, env)) {
      if (!target || !mediaUrl || decorated.has(target)) continue;
      const container = findPostContainer(target);
      if (!container) continue;
      const mediaKey = canonicalizeUrl(mediaUrl);
      if (findExistingPanel(container, mediaKey)) {
        decorated.add(target);
        continue;
      }

      const row = env.document.createElement("div");
      row.className = "sooqa-attachment-row";
      row.dataset.sooqaMediaKey = mediaKey;
      const preview = env.document.createElement("div");
      preview.className = "sooqa-attachment-preview";
      const panel = createActionPanel(env, mediaUrl, deriveThreadUrl(target, env));
      const targetParent = target.parentElement;
      if (targetParent) targetParent.insertBefore(row, target);
      else container.append(row);
      preview.append(target);
      row.append(preview, panel);
      decorated.add(target);
    }
  }

  function boot(root) {
    const document = root.document;
    if (!document || !document.body) return;
    if (!root.crypto || typeof root.crypto.randomUUID !== "function") return;

    const env = {
      document,
      location: root.location,
      crypto: root.crypto,
      prompt: typeof root.prompt === "function" ? root.prompt.bind(root) : () => null,
      GM_getValue: root.GM_getValue,
      GM_setValue: root.GM_setValue,
      GM_xmlhttpRequest: root.GM_xmlhttpRequest,
    };
    loadAcceptedHistory(env);
    ensureStyles(env);
    ensureHistoryControl(env);
    decorate(document, env);
    const observer = new root.MutationObserver((mutations) => {
      for (const mutation of mutations) {
        for (const node of Array.from(mutation.addedNodes)) {
          if (node.nodeType === 1) decorate(node, env);
        }
      }
    });
    observer.observe(document.body, { childList: true, subtree: true });

    if (typeof root.GM_getValue === "function" && typeof root.GM_setValue === "function") {
      const token = root.GM_getValue(TOKEN_STORAGE_KEY, "");
      if (!token) {
        const configured = root.prompt("Enter the sooqa localhost companion token:", "");
        if (configured) root.GM_setValue(TOKEN_STORAGE_KEY, configured.trim());
      }
    }
  }

  return {
    ACTIONS,
    buildPayload,
    canonicalizeUrl,
    clearAcceptedHistory,
    collectMetadata,
    decorate,
    extractDirectAttachmentUrls,
    localDateTimeToRfc3339,
    normalizeDirectMediaUrl,
    parseTags,
    boot,
  };
});
