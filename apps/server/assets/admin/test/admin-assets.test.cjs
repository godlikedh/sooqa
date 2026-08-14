const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

const root = path.join(__dirname, "..");
const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
const script = fs.readFileSync(path.join(root, "app.js"), "utf8");
const styles = fs.readFileSync(path.join(root, "styles.css"), "utf8");

test("admin shell uses local assets and has no inline executable content", () => {
  assert.match(html, /<script src="\/admin\/assets\/app\.js" defer><\/script>/);
  assert.doesNotMatch(html, /<script(?! src=)/);
  assert.doesNotMatch(html, /<style[\s>]/);
  assert.match(html, /id="token-form"/);
  assert.match(html, /id="dashboard-page"/);
  assert.match(html, /id="ingests-page"/);
  assert.match(html, /id="media-page"/);
  assert.match(html, /id="schedule-page"/);
  assert.match(html, /id="settings-page"/);
  assert.match(html, /id="publication-dialog"/);
});

test("admin client keeps the token in session storage and renders backend text safely", () => {
  assert.match(script, /sessionStorage/);
  assert.doesNotMatch(script, /localStorage/);
  assert.doesNotMatch(script, /innerHTML/);
  assert.match(script, /textContent/);
  assert.match(script, /credentials: "same-origin"/);
  assert.match(script, /\/api\/v1\/dashboard/);
  assert.match(script, /\/api\/v1\/ingests\?limit=50/);
  assert.match(script, /\/api\/v1\/channels/);
  assert.match(script, /accept-duplicate/);
  assert.match(script, /force-save/);
  assert.match(script, /repeat_evidence\?\.conflicts/);
  assert.match(script, /formatDuration/);
  assert.match(script, /appLink\("#media"/);
  assert.match(script, /URL\.createObjectURL/);
  assert.match(script, /URL\.revokeObjectURL/);
  assert.match(script, /localFutureTimeToIso/);
  assert.match(script, /caption-sync\/retry/);
  assert.match(script, /publication-intent/);
  assert.match(script, /schedule-exact/);
  assert.match(script, /scheduleEditing/);
  assert.match(script, /posts\?limit=50/);
  assert.match(html, /<th>ID<\/th>/);
  assert.match(html, /colspan="7"/);
});

test("admin visual system is dark, local, and keyboard-focused", () => {
  assert.match(styles, /color-scheme: dark/);
  assert.match(styles, /background: var\(--bg\)/);
  assert.match(styles, /:focus-visible/);
  assert.doesNotMatch(styles, /https?:\/\//);
});

class FakeNode {
  constructor(tagName, ownerDocument, nodeType = 1) {
    this.tagName = tagName.toUpperCase();
    this.ownerDocument = ownerDocument;
    this.nodeType = nodeType;
    this.parentElement = null;
    this.children = [];
    this.attributes = new Map();
    this.dataset = {};
    this.listeners = new Map();
    this.className = "";
    this._text = "";
    this.hidden = false;
    this.disabled = false;
    this.value = "";
    this.checked = false;
    this.href = "";
    this.open = false;
  }

  get firstChild() {
    return this.children[0] || null;
  }

  get lastChild() {
    return this.children[this.children.length - 1] || null;
  }

  get textContent() {
    return this.children.length ? this.children.map((child) => child.textContent).join("") : this._text;
  }

  set textContent(value) {
    this.children = [];
    this._text = String(value ?? "");
  }

  get classList() {
    return {
      toggle: (name, force) => {
        const classes = new Set(this.className.split(/\s+/).filter(Boolean));
        const enabled = force === undefined ? !classes.has(name) : Boolean(force);
        if (enabled) classes.add(name); else classes.delete(name);
        this.className = [...classes].join(" ");
        return enabled;
      },
    };
  }

  append(...items) {
    for (const item of items) {
      const child = typeof item === "string" ? new FakeNode("#text", this.ownerDocument, 3) : item;
      if (typeof item === "string") child._text = item;
      if (child.parentElement) child.parentElement.removeChild(child);
      child.parentElement = this;
      this.children.push(child);
    }
  }

  removeChild(child) {
    this.children = this.children.filter((item) => item !== child);
    child.parentElement = null;
    return child;
  }

  setAttribute(name, value) {
    this.attributes.set(name, String(value));
  }

  removeAttribute(name) {
    this.attributes.delete(name);
  }

  addEventListener(type, listener) {
    const listeners = this.listeners.get(type) || [];
    listeners.push(listener);
    this.listeners.set(type, listeners);
  }

  dispatchEvent(event) {
    event.target = this;
    event.currentTarget = this;
    event.preventDefault ||= () => {};
    for (const listener of this.listeners.get(event.type) || []) listener(event);
  }

  focus() {}

  showModal() {
    this.open = true;
  }

  close() {
    this.open = false;
  }

  reportValidity() {
    return true;
  }

  querySelectorAll(selector) {
    const result = [];
    const visit = (node) => {
      for (const child of node.children) {
        if (child.nodeType !== 1) continue;
        if (selector === child.tagName.toLowerCase() || selector === child.tagName) result.push(child);
        visit(child);
      }
    };
    visit(this);
    return result;
  }

  querySelector(selector) {
    return this.querySelectorAll(selector)[0] || null;
  }
}

class FakeDocument extends FakeNode {
  constructor() {
    super("document", null);
    this.ownerDocument = this;
    this.body = new FakeNode("body", this);
    this.elements = new Map();
  }

  createElement(tagName) {
    return new FakeNode(tagName, this);
  }

  createTextNode(value) {
    const node = new FakeNode("#text", this, 3);
    node._text = String(value);
    return node;
  }

  getElementById(id) {
    return this.elements.get(id) || null;
  }

  querySelectorAll(selector) {
    if (selector === "[data-page]") return [...this.elements.values()].filter((element) => element.dataset.page);
    if (selector === "[data-page-link]") return [...this.elements.values()].filter((element) => element.dataset.pageLink);
    return this.body.querySelectorAll(selector);
  }

  add(tagName, id, parent = this.body, dataset = {}) {
    const element = this.createElement(tagName);
    element.id = id;
    element.dataset = dataset;
    this.elements.set(id, element);
    parent.append(element);
    return element;
  }
}

function makeAdminDocument() {
  const document = new FakeDocument();
  const ids = [
    ["section", "token-gate"], ["div", "admin-shell"], ["button", "lock-button"],
    ["span", "session-status"], ["form", "token-form"], ["input", "api-token"],
    ["p", "token-error"], ["div", "toast"], ["div", "dashboard-counts"],
    ["span", "duplicate-count"], ["div", "duplicate-list"], ["span", "repeat-count"],
    ["div", "repeat-list"], ["span", "caption-failure-count"], ["div", "caption-failure-list"],
    ["button", "dashboard-refresh"], ["tbody", "ingest-rows"], ["button", "ingests-refresh"],
    ["button", "ingests-next"], ["button", "media-refresh"], ["form", "media-search-form"],
    ["input", "media-search"], ["button", "media-search-button"], ["button", "media-clear-search"],
    ["div", "media-status"], ["div", "media-grid"], ["button", "media-next"],
    ["button", "schedule-refresh"], ["div", "schedule-notice"], ["div", "schedule-list"],
    ["button", "schedule-next"],
    ["button", "settings-refresh"], ["form", "settings-form"],
    ["input", "channel-id"], ["input", "channel-updated-at"], ["input", "channel-name"],
    ["input", "channel-chat-id"], ["input", "channel-time-zone"], ["input", "channel-window-start"],
    ["input", "channel-window-end"], ["input", "channel-interval"], ["select", "channel-parse-mode"],
    ["input", "channel-enabled"], ["input", "channel-disable-notification"], ["button", "settings-save"],
    ["span", "settings-mode"], ["div", "settings-warning"],
    ["dialog", "publication-dialog"], ["form", "publication-form"], ["h2", "publication-dialog-title"],
    ["p", "publication-dialog-context"], ["textarea", "publication-caption"], ["div", "publication-time-field"],
    ["input", "publication-time"], ["p", "publication-error"], ["button", "publication-cancel"],
    ["button", "publication-submit"],
  ];
  for (const [tagName, id] of ids) document.add(tagName, id);
  document.getElementById("media-search-form").append(document.getElementById("media-search-button"));
  for (const page of ["dashboard", "ingests", "media", "schedule", "settings"]) {
    document.add("section", `${page}-page`, document.body, { page });
    document.add("a", `${page}-nav`, document.body, { pageLink: page });
  }
  document.getElementById("token-form").reportValidity = () => true;
  document.getElementById("settings-form").reportValidity = () => true;
  return document;
}

function jsonResponse(payload, status = 200) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: () => "application/json" },
    json: async () => payload,
  };
}

function binaryResponse(mimeType = "image/jpeg", status = 200) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: () => mimeType },
    blob: async () => ({ type: mimeType }),
  };
}

function createAdminRuntime({ token = "", handler }) {
  const document = makeAdminDocument();
  const storage = new Map(token ? [["sooqa.admin.api_token", token]] : []);
  const objectUrls = new Set();
  let objectUrlNumber = 0;
  const windowListeners = new Map();
  const window = {
    document,
    location: { origin: "http://sooqa.test", hash: "" },
    sessionStorage: {
      getItem: (key) => storage.get(key) || null,
      setItem: (key, value) => storage.set(key, String(value)),
      removeItem: (key) => storage.delete(key),
    },
    URL: {
      createObjectURL: () => {
        const value = `blob:test-${++objectUrlNumber}`;
        objectUrls.add(value);
        return value;
      },
      revokeObjectURL: (value) => objectUrls.delete(value),
    },
    crypto: { randomUUID: () => "admin-test-key" },
    addEventListener: (type, listener) => windowListeners.set(type, listener),
    clearTimeout: () => {},
    setTimeout: () => 0,
  };
  const fetch = (pathName, options) => handler(pathName, options);
  vm.runInNewContext(script, {
    window,
    document,
    fetch,
    Headers,
    Node: FakeNode,
    URL,
    console,
  });
  return {
    document,
    window,
    storage,
    objectUrls,
    dispatchWindow(type) {
      windowListeners.get(type)?.({ type });
    },
  };
}

async function settle() {
  await new Promise((resolve) => setImmediate(resolve));
  await new Promise((resolve) => setImmediate(resolve));
}

function buttonWithText(element, text) {
  return element.querySelectorAll("button").find((button) => button.textContent === text);
}

test("admin runtime handles token lifecycle, safe rendering, and both decisions", async () => {
  const calls = [];
  const dashboard = {
    counts: { ready_media: 3 },
    attention: {
      technical_duplicates: [{
        ingest_id: "ingest-1",
        source_url: "https://example.test/<script>",
        candidates: [{ media_id: "media-1", classification: "strong_match", score_bps: 9900, storage_url: "https://t.me/c/1/2" }],
      }],
      publication_repeats: [{
        post_id: "post-1", media_id: "media-2", requested_action: "post_now", status: "draft",
        caption: "<img src=x>", revision: 4,
        repeat_evidence: { conflicts: [{ post_id: "post-old", state: "published", at: "2026-01-01T00:00:00Z", target_message_link: "https://t.me/c/1/3" }] },
      }],
      caption_sync_failures: [{ media_id: "media-3", error_message: "<b>unsafe</b>" }],
    },
  };
  const runtime = createAdminRuntime({
    handler: async (pathName, options) => {
      calls.push({ pathName, options });
      assert.equal(options.credentials, "same-origin");
      assert.equal(options.headers.get("Authorization"), "Bearer secret");
      if (pathName.includes("accept-duplicate")) return jsonResponse({});
      if (pathName.includes("/decision")) return jsonResponse({});
      return jsonResponse(dashboard);
    },
  });

  assert.equal(calls.length, 0);
  const tokenInput = runtime.document.getElementById("api-token");
  tokenInput.value = "secret";
  runtime.document.getElementById("token-form").dispatchEvent({ type: "submit" });
  await settle();

  assert.equal(runtime.storage.get("sooqa.admin.api_token"), "secret");
  assert.equal(runtime.document.getElementById("admin-shell").hidden, false);
  assert.match(runtime.document.getElementById("duplicate-list").textContent, /<script>/);
  assert.match(runtime.document.getElementById("repeat-list").textContent, /<img src=x>/);
  assert.equal(runtime.document.getElementById("duplicate-list").querySelector("img"), null);
  assert.match(runtime.document.getElementById("repeat-list").textContent, /published/);

  const duplicateButton = buttonWithText(runtime.document.getElementById("duplicate-list"), "Same — use this");
  duplicateButton.dispatchEvent({ type: "click" });
  assert.equal(duplicateButton.disabled, true);
  await settle();
  assert.equal(calls.filter(({ pathName }) => pathName.includes("accept-duplicate")).length, 1);

  const repeatButton = buttonWithText(runtime.document.getElementById("repeat-list"), "Post now anyway");
  repeatButton.dispatchEvent({ type: "click" });
  await settle();
  const decision = calls.find(({ pathName }) => pathName.includes("/decision"));
  assert.deepEqual(JSON.parse(decision.options.body), { decision: "post_now_anyway", expected_revision: 4 });
  assert.match(decision.options.headers.get("Idempotency-Key"), /^admin-ui:/);

  runtime.document.getElementById("lock-button").dispatchEvent({ type: "click" });
  assert.equal(runtime.storage.has("sooqa.admin.api_token"), false);
  assert.equal(runtime.document.getElementById("admin-shell").hidden, true);
});

test("admin runtime paginates ingests and refreshes only the first page", async () => {
  const paths = [];
  const runtime = createAdminRuntime({
    token: "secret",
    handler: async (pathName, options) => {
      paths.push(pathName);
      assert.equal(options.headers.get("Authorization"), "Bearer secret");
      if (pathName.startsWith("/api/v1/ingests?")) {
        return jsonResponse(pathName.includes("cursor=")
          ? { items: [], next_cursor: null }
          : { items: [{ id: "ingest-123", source_url: "https://example.test", requested_action: "save", status: "failed_terminal", created_at: "2026-01-01T00:00:00Z", updated_at: "2026-01-01T00:01:00Z", completed_at: "2026-01-01T00:01:00Z", error_code: "bad_input", error_message: "<error>" }], next_cursor: "next:cursor" });
      }
      return jsonResponse({ counts: {}, attention: {} });
    },
  });
  await settle();
  runtime.window.location.hash = "#ingests";
  runtime.dispatchWindow("hashchange");
  await settle();

  assert.equal(paths.filter((pathName) => pathName === "/api/v1/ingests?limit=50").length, 1);
  assert.match(runtime.document.getElementById("ingest-rows").textContent, /ingest-1/);
  assert.match(runtime.document.getElementById("ingest-rows").textContent, /bad_input: <error>/);
  const next = runtime.document.getElementById("ingests-next");
  assert.equal(next.hidden, false);
  next.dispatchEvent({ type: "click" });
  await settle();
  assert.equal(paths.at(-1), "/api/v1/ingests?limit=50&cursor=next%3Acursor");
  runtime.document.getElementById("ingests-refresh").dispatchEvent({ type: "click" });
  await settle();
  assert.equal(paths.at(-1), "/api/v1/ingests?limit=50");
});

test("admin runtime sends the settings fence and reloads after a stale save", async () => {
  const paths = [];
  const channel = {
    id: "channel-1", name: "Main", telegram_chat_id: -1001, is_enabled: true,
    time_zone: "UTC", window_start: "08:00:00", window_end: "22:00:00", interval_minutes: 30,
    default_parse_mode: null, default_disable_notification: false, updated_at: "2026-01-01T00:00:00Z",
  };
  const runtime = createAdminRuntime({
    token: "secret",
    handler: async (pathName, options) => {
      paths.push({ pathName, options });
      if (pathName === "/api/v1/channels") return jsonResponse({ items: [channel] });
      if (pathName === "/api/v1/channels/channel-1") return jsonResponse({ error: { message: "stale" } }, 409);
      return jsonResponse({ counts: {}, attention: {} });
    },
  });
  await settle();
  runtime.window.location.hash = "#settings";
  runtime.dispatchWindow("hashchange");
  await settle();
  runtime.document.getElementById("channel-name").value = "Renamed";
  runtime.document.getElementById("settings-form").dispatchEvent({ type: "submit" });
  await settle();

  const patch = paths.find(({ pathName }) => pathName === "/api/v1/channels/channel-1");
  assert.equal(JSON.parse(patch.options.body).expected_updated_at, channel.updated_at);
  assert.equal(paths.filter(({ pathName }) => pathName === "/api/v1/channels").length, 2);
  assert.match(runtime.document.getElementById("toast").textContent, /stale/);
});

test("admin runtime keeps media lookup, preview fetches, edits, retry, and publication fields separate", async () => {
  const calls = [];
  const media = {
    id: "media-1", kind: "video", status: "active", title: "<unsafe title>", description: "internal",
    storage_state: "ready", storage_url: "https://t.me/c/1/2", source_url: "https://example.test/<img>",
    preview: { url: "/api/v1/media/media-1/preview", mime_type: "image/jpeg" },
    caption_sync: { state: "failed", error: "<unsafe error>" },
    tags: [{ normalized_name: "cats", display_name: "Cats" }], file_size_bytes: 2048, duration_ms: 61_000,
    updated_at: "2026-01-01T00:00:00Z",
  };
  const runtime = createAdminRuntime({
    token: "secret",
    handler: async (pathName, options) => {
      calls.push({ pathName, options });
      assert.equal(options.headers.get("Authorization"), "Bearer secret");
      if (pathName === "/api/v1/media/media-1/preview") return binaryResponse();
      if (pathName === "/api/v1/media/media-1/caption-sync/retry") {
        return jsonResponse({ ...media, caption_sync: { state: "pending", error: null } });
      }
      if (pathName === "/api/v1/media/media-1") {
        return jsonResponse({ ...media, description: "updated", tags: [{ normalized_name: "dogs", display_name: "Dogs" }], caption_sync: { state: "pending", error: null } });
      }
      if (pathName.includes("/publication-intent")) return jsonResponse({ state: "queued" });
      if (pathName.startsWith("/api/v1/media?")) return jsonResponse({ items: [media], next_cursor: null });
      return jsonResponse({ counts: {}, attention: {} });
    },
  });
  await settle();
  runtime.window.location.hash = "#media";
  runtime.dispatchWindow("hashchange");
  await settle();

  const grid = runtime.document.getElementById("media-grid");
  assert.match(grid.textContent, /<unsafe title>/);
  assert.match(grid.textContent, /<unsafe error>/);
  assert.equal(grid.querySelector("script"), null);
  const previewCall = calls.find(({ pathName }) => pathName.endsWith("/preview"));
  assert.equal(previewCall.pathName, "/api/v1/media/media-1/preview");
  assert.equal([...runtime.objectUrls].length, 1);
  const firstObjectUrl = [...runtime.objectUrls][0];
  assert.match(firstObjectUrl, /^blob:/);

  const retry = buttonWithText(grid, "Retry sync");
  retry.dispatchEvent({ type: "click" });
  await settle();
  assert.equal(calls.filter(({ pathName }) => pathName.endsWith("/caption-sync/retry")).length, 1);

  runtime.document.getElementById("media-search").value = "https://2ch.org/thread/<exact>";
  runtime.document.getElementById("media-search-form").dispatchEvent({ type: "submit" });
  await settle();
  const lookupCall = calls.filter(({ pathName }) => pathName.startsWith("/api/v1/media?" )).at(-1);
  assert.match(lookupCall.pathName, /\/api\/v1\/media\?limit=50&q=https%3A%2F%2F2ch.org%2Fthread%2F%3Cexact%3E/);
  const card = runtime.document.getElementById("media-grid").querySelector("article");
  const tagInput = card.querySelectorAll("input")[0];
  const description = card.querySelectorAll("textarea")[0];
  tagInput.value = "Dogs, Cats, dogs";
  description.value = "<new internal>";
  buttonWithText(card, "Save edits").dispatchEvent({ type: "click" });
  await settle();
  const patch = calls.find(({ pathName }) => pathName === "/api/v1/media/media-1");
  assert.deepEqual(JSON.parse(patch.options.body), {
    description: "<new internal>",
    tags: ["Dogs", "Cats"],
    expected_updated_at: media.updated_at,
  });

  buttonWithText(runtime.document.getElementById("media-grid"), "Post now").dispatchEvent({ type: "click" });
  await settle();
  const plainPublication = calls.filter(({ pathName }) => pathName.includes("/publication-intent")).at(-1);
  assert.deepEqual(JSON.parse(plainPublication.options.body), { requested_action: "post_now" });
  assert.match(plainPublication.options.headers.get("Idempotency-Key"), /^admin-ui:/);

  buttonWithText(runtime.document.getElementById("media-grid"), "Queue…").dispatchEvent({ type: "click" });
  const future = new Date(Date.now() + 3_600_000);
  const pad = (value) => String(value).padStart(2, "0");
  runtime.document.getElementById("publication-caption").value = "<public text>";
  runtime.document.getElementById("publication-time").value = `${future.getFullYear()}-${pad(future.getMonth() + 1)}-${pad(future.getDate())}T${pad(future.getHours())}:${pad(future.getMinutes())}`;
  runtime.document.getElementById("publication-form").dispatchEvent({ type: "submit" });
  await settle();
  const exactPublication = calls.filter(({ pathName }) => pathName.includes("/publication-intent")).at(-1);
  const exactBody = JSON.parse(exactPublication.options.body);
  assert.equal(exactBody.requested_action, "queue");
  assert.equal(exactBody.requested_post_caption, "<public text>");
  assert.equal(exactBody.requested_publish_at, new Date(runtime.document.getElementById("publication-time").value).toISOString());

  runtime.document.getElementById("media-refresh").dispatchEvent({ type: "click" });
  await settle();
  assert.equal(runtime.objectUrls.has(firstObjectUrl), false);
});

test("admin runtime keeps schedule forms editable across refresh and fences every mutation", async () => {
  const calls = [];
  let caption = "queued text";
  let requestedPublishAt = null;
  let revision = 3;
  let updatedAt = "2026-01-01T00:00:00Z";
  let removed = false;
  const item = () => ({
    id: "post-1", media_id: "media-1", channel_id: "channel-1", status: removed ? "cancelled" : "queued",
    requested_action: "queue", requested_publish_at: requestedPublishAt, schedule_mode: requestedPublishAt ? "explicit" : "cadence",
    scheduled_at: requestedPublishAt || new Date(Date.now() + 7_200_000).toISOString(), caption,
    revision, updated_at: updatedAt, media_kind: "video", channel_name: "Main",
    source_url: "https://example.test/clip.webm", storage_url: "https://t.me/c/1/2",
  });
  const runtime = createAdminRuntime({
    token: "secret",
    handler: async (pathName, options) => {
      calls.push({ pathName, options });
      if (pathName === "/api/v1/posts?limit=50") return jsonResponse({ items: removed ? [] : [item()], next_cursor: null });
      if (pathName === "/api/v1/posts/post-1" && options.method === "PATCH") {
        const body = JSON.parse(options.body);
        caption = body.caption;
        revision += 1;
        updatedAt = `2026-01-01T00:00:0${revision}Z`;
        return jsonResponse(item());
      }
      if (pathName === "/api/v1/posts/post-1/schedule-exact") {
        const body = JSON.parse(options.body);
        requestedPublishAt = body.publish_at;
        revision += 1;
        updatedAt = `2026-01-01T00:00:0${revision}Z`;
        return jsonResponse(item());
      }
      if (pathName === "/api/v1/posts/post-1/publish") {
        revision += 1;
        return jsonResponse(item());
      }
      if (pathName === "/api/v1/posts/post-1/cancel") {
        removed = true;
        return jsonResponse({ ...item(), status: "cancelled" });
      }
      return jsonResponse({ counts: {}, attention: {} });
    },
  });
  await settle();
  runtime.window.location.hash = "#schedule";
  runtime.dispatchWindow("hashchange");
  await settle();

  const list = runtime.document.getElementById("schedule-list");
  assert.match(list.textContent, /queued/);
  assert.match(list.textContent, /Cadence/);
  assert.match(list.textContent, /Open in Telegram/);
  const card = list.querySelector("article");
  const captionInput = card.querySelectorAll("textarea")[0];
  captionInput.value = "locally edited";
  captionInput.dispatchEvent({ type: "input" });
  runtime.document.getElementById("schedule-refresh").dispatchEvent({ type: "click" });
  await settle();
  assert.equal(captionInput.value, "locally edited");
  assert.equal(runtime.document.getElementById("schedule-notice").hidden, false);

  buttonWithText(card, "Save text").dispatchEvent({ type: "click" });
  await settle();
  const save = calls.find(({ pathName, options }) => pathName === "/api/v1/posts/post-1" && options.method === "PATCH");
  assert.deepEqual(JSON.parse(save.options.body), {
    expected_revision: 3,
    expected_updated_at: "2026-01-01T00:00:00Z",
    caption: "locally edited",
  });
  assert.equal(list.querySelector("article").querySelectorAll("textarea")[0].value, "locally edited");

  const refreshedCard = list.querySelector("article");
  buttonWithText(refreshedCard, "Clear text").dispatchEvent({ type: "click" });
  await settle();
  const clearCall = calls.filter(({ pathName, options }) => pathName === "/api/v1/posts/post-1" && options.method === "PATCH").at(-1);
  assert.equal(JSON.parse(clearCall.options.body).caption, null);

  const exactCard = list.querySelector("article");
  const timeInput = exactCard.querySelectorAll("input")[0];
  const future = new Date(Date.now() + 3_600_000);
  const pad = (value) => String(value).padStart(2, "0");
  timeInput.value = `${future.getFullYear()}-${pad(future.getMonth() + 1)}-${pad(future.getDate())}T${pad(future.getHours())}:${pad(future.getMinutes())}`;
  buttonWithText(exactCard, "Set exact time").dispatchEvent({ type: "click" });
  await settle();
  const exact = calls.find(({ pathName }) => pathName.endsWith("/schedule-exact"));
  const exactBody = JSON.parse(exact.options.body);
  assert.equal(exactBody.expected_revision, 5);
  assert.equal(exact.options.headers.get("Idempotency-Key").startsWith("admin-ui:"), true);
  assert.equal(exactBody.publish_at, new Date(timeInput.value).toISOString());
  assert.match(list.textContent, /Exact time/);

  const pastCard = list.querySelector("article");
  pastCard.querySelectorAll("input")[0].value = "2000-01-01T00:00";
  buttonWithText(pastCard, "Set exact time").dispatchEvent({ type: "click" });
  await settle();
  assert.equal(calls.filter(({ pathName }) => pathName.endsWith("/schedule-exact")).length, 1);
  assert.match(runtime.document.getElementById("toast").textContent, /future local time/);

  buttonWithText(pastCard, "Post now").dispatchEvent({ type: "click" });
  await settle();
  const publish = calls.find(({ pathName }) => pathName.endsWith("/publish"));
  assert.deepEqual(JSON.parse(publish.options.body), { expected_revision: 6 });
  assert.equal(publish.options.headers.get("Idempotency-Key").startsWith("admin-ui:"), true);

  const removeCard = list.querySelector("article");
  buttonWithText(removeCard, "Remove").dispatchEvent({ type: "click" });
  await settle();
  assert.equal(calls.some(({ pathName }) => pathName.endsWith("/cancel")), true);
  assert.match(list.textContent, /No unpublished schedule work/);
});

test("admin runtime leaves sending and unknown schedule rows read-only", async () => {
  const runtime = createAdminRuntime({
    token: "secret",
    handler: async (pathName) => {
      if (pathName === "/api/v1/posts?limit=50") {
        return jsonResponse({ items: [{
          id: "post-unknown", media_id: "media-1", status: "unknown", schedule_mode: "explicit",
          scheduled_at: "2026-01-01T00:00:00Z", requested_publish_at: "2026-01-01T00:00:00Z",
          caption: "Do not resend", media_kind: "video", storage_url: "https://t.me/c/1/2",
        }], next_cursor: null });
      }
      return jsonResponse({ counts: {}, attention: {} });
    },
  });
  await settle();
  runtime.window.location.hash = "#schedule";
  runtime.dispatchWindow("hashchange");
  await settle();
  const card = runtime.document.getElementById("schedule-list").querySelector("article");
  assert.equal(card.querySelectorAll("textarea").length, 0);
  assert.equal(buttonWithText(card, "Post now"), undefined);
  assert.equal(buttonWithText(card, "Remove"), undefined);
  assert.match(card.textContent, /not safely reversible/);
});
