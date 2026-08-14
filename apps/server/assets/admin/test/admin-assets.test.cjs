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
  assert.match(html, /id="settings-page"/);
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
    ["button", "ingests-next"], ["button", "settings-refresh"], ["form", "settings-form"],
    ["input", "channel-id"], ["input", "channel-updated-at"], ["input", "channel-name"],
    ["input", "channel-chat-id"], ["input", "channel-time-zone"], ["input", "channel-window-start"],
    ["input", "channel-window-end"], ["input", "channel-interval"], ["select", "channel-parse-mode"],
    ["input", "channel-enabled"], ["input", "channel-disable-notification"], ["button", "settings-save"],
    ["span", "settings-mode"], ["div", "settings-warning"],
  ];
  for (const [tagName, id] of ids) document.add(tagName, id);
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

function createAdminRuntime({ token = "", handler }) {
  const document = makeAdminDocument();
  const storage = new Map(token ? [["sooqa.admin.api_token", token]] : []);
  const windowListeners = new Map();
  const window = {
    document,
    location: { origin: "http://sooqa.test", hash: "" },
    sessionStorage: {
      getItem: (key) => storage.get(key) || null,
      setItem: (key, value) => storage.set(key, String(value)),
      removeItem: (key) => storage.delete(key),
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
