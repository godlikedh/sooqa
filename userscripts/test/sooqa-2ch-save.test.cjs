const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const assert = require("node:assert/strict");

const userscript = require("../sooqa-2ch-save.user.js");

class FakeElement {
  constructor(tagName, ownerDocument) {
    this.tagName = tagName.toUpperCase();
    this.nodeType = 1;
    this.ownerDocument = ownerDocument;
    this.children = [];
    this.parentElement = null;
    this.attributes = new Map();
    this.dataset = {};
    this.listeners = new Map();
    this.className = "";
    this.textContent = "";
    this.value = "";
    this.type = "";
    this.disabled = false;
    this.open = false;
    this.style = {};
  }

  append(...items) {
    for (const item of items) {
      if (typeof item === "string") {
        this.children.push(item);
        continue;
      }
      if (item.parentElement) {
        item.parentElement.children = item.parentElement.children.filter((child) => child !== item);
      }
      item.parentElement = this;
      this.children.push(item);
      if (this === this.ownerDocument.body) this.ownerDocument.notifyAdded(item);
    }
  }

  insertBefore(item, reference) {
    if (item.parentElement) {
      item.parentElement.children = item.parentElement.children.filter((child) => child !== item);
    }
    item.parentElement = this;
    const index = this.children.indexOf(reference);
    if (index < 0) this.children.push(item);
    else this.children.splice(index, 0, item);
  }

  remove() {
    if (!this.parentElement) return;
    this.parentElement.children = this.parentElement.children.filter((child) => child !== this);
    this.parentElement = null;
  }

  setAttribute(name, value) {
    this.attributes.set(name, String(value));
  }

  getAttribute(name) {
    return this.attributes.get(name) || null;
  }

  matches(selector) {
    return selector.split(",").some((part) => this.matchesSingle(part.trim()));
  }

  matchesSingle(selector) {
    if (selector === "a[href]") return this.tagName === "A" && Boolean(this.href);
    if (selector === "video[src]") return this.tagName === "VIDEO" && Boolean(this.src);
    if (selector === "source[src]") return this.tagName === "SOURCE" && Boolean(this.src);
    if (selector === "[data-num]") return this.attributes.has("data-num");
    if (selector === "[id^='p']") return String(this.id || "").startsWith("p");
    if (selector === "[data-sooqa-media-url]") return Boolean(this.dataset.sooqaMediaUrl);
    if (selector.startsWith("button[")) {
      return this.tagName === "BUTTON" && Boolean(this.dataset.sooqaAction);
    }
    if (selector.startsWith(".")) return this.className.split(/\s+/).includes(selector.slice(1));
    if (selector === "style" || selector === "dialog" || selector === "input" ||
        selector === "textarea" || selector === "button" || selector === "form" ||
        selector === "label" || selector === "div" || selector === "span" ||
        selector === "article" || selector === "video" || selector === "source" ||
        selector === "a") {
      return this.tagName === selector.toUpperCase();
    }
    return false;
  }

  querySelectorAll(selector) {
    const result = [];
    const visit = (element) => {
      for (const child of element.children) {
        if (typeof child === "string") continue;
        if (child.matches(selector)) result.push(child);
        visit(child);
      }
    };
    visit(this);
    return result;
  }

  querySelector(selector) {
    return this.querySelectorAll(selector)[0] || null;
  }

  closest(selector) {
    let current = this;
    while (current) {
      if (current.matches(selector)) return current;
      current = current.parentElement;
    }
    return null;
  }

  addEventListener(type, listener) {
    const listeners = this.listeners.get(type) || [];
    listeners.push(listener);
    this.listeners.set(type, listeners);
  }

  dispatchEvent(event) {
    event.target = this;
    event.preventDefault = event.preventDefault || (() => {});
    for (const listener of this.listeners.get(event.type) || []) listener(event);
  }

  click() {
    this.dispatchEvent({ type: "click" });
    if (this.type === "submit") {
      let form = this.parentElement;
      while (form && form.tagName !== "FORM") form = form.parentElement;
      if (form) form.dispatchEvent({ type: "submit" });
    }
  }

  focus() {}

  showModal() {
    this.open = true;
  }

  close() {
    this.open = false;
  }
}

class FakeDocument extends FakeElement {
  constructor(url) {
    super("document", null);
    this.ownerDocument = this;
    this.location = { href: url };
    this.title = "Fixture thread";
    this.body = new FakeElement("body", this);
    this.observers = [];
  }

  createElement(tagName) {
    return new FakeElement(tagName, this);
  }

  querySelectorAll(selector) {
    return this.body.querySelectorAll(selector);
  }

  notifyAdded(node) {
    for (const observer of this.observers) observer.callback([{ addedNodes: [node] }]);
  }
}

function createPost(document, mediaUrls) {
  const post = document.createElement("article");
  post.setAttribute("data-num", mediaUrls[0]);
  for (const mediaUrl of mediaUrls) {
    const link = document.createElement("a");
    link.href = mediaUrl;
    link.setAttribute("href", mediaUrl);
    post.append(link);
  }
  return post;
}

function createDuplicateMediaPost(document, mediaUrl) {
  const post = document.createElement("article");
  post.setAttribute("data-num", mediaUrl);
  const link = document.createElement("a");
  link.href = mediaUrl;
  link.setAttribute("href", mediaUrl);
  const video = document.createElement("video");
  video.src = mediaUrl;
  video.setAttribute("src", mediaUrl);
  const source = document.createElement("source");
  source.src = mediaUrl;
  source.setAttribute("src", mediaUrl);
  video.append(source);
  link.append(video);
  post.append(link);
  return post;
}

function createBrowser(url, requests, storage = new Map(), requestHandler = null) {
  const document = new FakeDocument(url);
  let sequence = 0;
  const root = {
    document,
    location: document.location,
    crypto: { randomUUID: () => "action-" + (++sequence) },
    MutationObserver: class {
      constructor(callback) {
        this.callback = callback;
      }

      observe(target) {
        target.ownerDocument.observers.push(this);
      }
    },
    prompt: () => null,
    GM_getValue: (key, fallback) => {
      if (key === "sooqa_companion_token") return storage.has(key) ? storage.get(key) : "local-token";
      return storage.has(key) ? storage.get(key) : fallback;
    },
    GM_setValue: (key, value) => storage.set(key, value),
    GM_xmlhttpRequest: (request) => {
      requests.push(request);
      if (requestHandler) requestHandler(request);
      else request.onload({ status: 202 });
    },
  };
  return { document, root, storage };
}

function actionButtons(post) {
  const panels = post.querySelectorAll(".sooqa-action-panel");
  assert.equal(panels.length, 1);
  return panels[0].querySelectorAll("button[data-sooqa-action]");
}

function actionButton(post, key) {
  return actionButtons(post).find((button) => button.dataset.sooqaAction === key);
}

function requestPayload(request) {
  return JSON.parse(request.data);
}

function futureLocalDateTime() {
  const date = new Date(Date.now() + 60 * 60 * 1000);
  const pad = (value) => String(value).padStart(2, "0");
  return [
    date.getFullYear(),
    "-",
    pad(date.getMonth() + 1),
    "-",
    pad(date.getDate()),
    "T",
    pad(date.getHours()),
    ":",
    pad(date.getMinutes()),
  ].join("");
}

test("extracts only direct MP4 and WebM attachment URLs", () => {
  const nodes = [
    { href: "https://2ch.org/b/src/1/clip.webm?download=1" },
    { href: "https://2ch.org/b/src/2/clip.mp4" },
    { href: "https://2ch.org/b/res/1.html" },
    { href: "javascript:alert(1)" },
    { href: "https://2ch.org/b/src/1/clip.webm?download=1" },
  ];

  assert.deepEqual(userscript.extractDirectAttachmentUrls(nodes, "https://2ch.org/b/res/1.html"), [
    "https://2ch.org/b/src/1/clip.webm?download=1",
    "https://2ch.org/b/src/2/clip.mp4",
  ]);
});

test("builds typed plain and detailed payloads with separated public text", () => {
  const save = userscript.ACTIONS.find((action) => action.key === "save");
  const postNow = userscript.ACTIONS.find((action) => action.key === "post_now_detailed");
  const plain = userscript.buildPayload({
    actionId: "action-1",
    mediaUrl: "https://2ch.org/b/src/1/clip.webm",
    pageUrl: "https://2ch.org/b/res/1.html",
    pageTitle: "Thread",
    action: save,
  });
  assert.deepEqual(plain, {
    action_id: "action-1",
    url: "https://2ch.org/b/src/1/clip.webm",
    page_url: "https://2ch.org/b/res/1.html",
    page_title: "Thread",
    requested_action: "save",
  });

  const detailed = userscript.buildPayload({
    actionId: "action-2",
    mediaUrl: "https://2ch.org/b/src/1/clip.webm",
    pageUrl: "https://2ch.org/b/res/1.html",
    pageTitle: "Thread",
    action: postNow,
    metadata: {
      description: "Internal note",
      tags: ["cats", "reaction"],
      publicText: "Public caption",
    },
  });
  assert.deepEqual(detailed, {
    action_id: "action-2",
    url: "https://2ch.org/b/src/1/clip.webm",
    page_url: "https://2ch.org/b/res/1.html",
    page_title: "Thread",
    requested_action: "post_now",
    description: "Internal note",
    tags: ["cats", "reaction"],
    requested_post_caption: "Public caption",
  });
});

test("normalizes tags and converts future browser-local time to RFC3339", () => {
  assert.deepEqual(userscript.parseTags(" Cats, reaction, cats "), ["cats", "reaction"]);
  const now = new Date(2026, 0, 1, 12, 0, 0);
  const expected = new Date(2026, 0, 1, 15, 30, 0).toISOString();
  assert.equal(userscript.localDateTimeToRfc3339("2026-01-01T15:30", now), expected);
  assert.equal(userscript.localDateTimeToRfc3339("2026-01-01T11:59", now), null);
  assert.equal(userscript.localDateTimeToRfc3339("not-a-date", now), null);
});

test("fixture keeps the supported page surface narrow", () => {
  const fixture = fs.readFileSync(
    path.join(__dirname, "fixtures", "2ch-direct-attachments.html"),
    "utf8"
  );
  for (const host of ["2ch.su", "2ch.org", "2ch.life"]) {
    const escapedHost = host.replace(".", "\\.");
    assert.match(fixture, new RegExp("https://" + escapedHost + "/.*clip\\.webm"));
    assert.match(fixture, new RegExp("https://" + escapedHost + "/.*clip\\.mp4"));
  }
  assert.doesNotMatch(fixture, /youtube|yt-dlp/i);
});

test("each supported host gets vertical six-action rows for initial and dynamic media", () => {
  for (const host of ["2ch.su", "2ch.org", "2ch.life"]) {
    const requests = [];
    const browser = createBrowser("https://" + host + "/b/res/1.html", requests);
    const initialPost = createPost(browser.document, [
      "https://" + host + "/b/src/1/initial.webm",
      "https://" + host + "/b/src/2/second.mp4",
    ]);
    browser.document.body.append(initialPost);
    userscript.boot(browser.root);

    assert.equal(initialPost.querySelectorAll(".sooqa-attachment-row").length, 2);
    const initialPanels = initialPost.querySelectorAll(".sooqa-action-panel");
    assert.equal(initialPanels.length, 2);
    for (const panel of initialPanels) {
      assert.equal(panel.querySelectorAll("button[data-sooqa-action]").length, 6);
    }
    assert.equal(initialPost.querySelectorAll(".sooqa-attachment-preview").length, 2);

    const dynamicPost = createPost(
      browser.document,
      ["https://" + host + "/b/src/3/dynamic.mp4"]
    );
    browser.document.body.append(dynamicPost);
    browser.document.notifyAdded(dynamicPost);
    assert.equal(dynamicPost.querySelectorAll(".sooqa-attachment-row").length, 1);
    assert.equal(actionButtons(dynamicPost).length, 6);
  }
});

test("the same link, video, and source attachment gets one row", () => {
  const requests = [];
  const browser = createBrowser("https://2ch.org/b/res/1.html", requests);
  const post = createDuplicateMediaPost(browser.document, "https://2ch.org/b/src/1/clip.webm");
  browser.document.body.append(post);
  userscript.boot(browser.root);
  assert.equal(post.querySelectorAll(".sooqa-attachment-row").length, 1);
  assert.equal(actionButtons(post).length, 6);
  assert.equal(post.querySelectorAll("video").length, 1);
  assert.equal(post.querySelectorAll("source").length, 1);
});

test("plain actions forward each typed intent without metadata", () => {
  for (const actionKey of ["post_now", "queue", "save"]) {
    const requests = [];
    const browser = createBrowser("https://2ch.org/b/res/1.html", requests);
    const post = createPost(browser.document, ["https://2ch.org/b/src/1/clip.webm"]);
    browser.document.body.append(post);
    userscript.boot(browser.root);
    actionButton(post, actionKey).click();
    assert.equal(requests.length, 1);
    const payload = requestPayload(requests[0]);
    assert.equal(payload.requested_action, actionKey);
    assert.equal("tags" in payload, false);
    assert.equal("description" in payload, false);
    assert.equal("requested_post_caption" in payload, false);
    assert.equal("requested_publish_at" in payload, false);
  }
});

test("Post now… asks for public text and Queue… adds an exact future time", async () => {
  const requests = [];
  const browser = createBrowser("https://2ch.org/b/res/1.html", requests);
  const post = createPost(browser.document, ["https://2ch.org/b/src/1/clip.webm"]);
  browser.document.body.append(post);
  userscript.boot(browser.root);

  actionButton(post, "post_now_detailed").click();
  let dialog = browser.document.body.querySelector("dialog");
  assert.equal(dialog.querySelectorAll("input").length, 1);
  assert.equal(dialog.querySelectorAll("textarea").length, 2);
  dialog.querySelector("input").value = "Cats";
  dialog.querySelectorAll("textarea")[0].value = "Internal";
  dialog.querySelectorAll("textarea")[1].value = "Public";
  dialog.querySelectorAll("button")[1].click();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(requestPayload(requests[0]).requested_action, "post_now");
  assert.equal(requestPayload(requests[0]).requested_post_caption, "Public");
  assert.equal("requested_publish_at" in requestPayload(requests[0]), false);

  actionButton(post, "queue_exact").click();
  dialog = browser.document.body.querySelector("dialog");
  assert.equal(dialog.querySelectorAll("input").length, 2);
  assert.equal(dialog.querySelectorAll("textarea").length, 2);
  dialog.querySelectorAll("input")[0].value = "Cats";
  dialog.querySelectorAll("textarea")[0].value = "Internal";
  dialog.querySelectorAll("textarea")[1].value = "Public";
  dialog.querySelectorAll("input")[1].value = futureLocalDateTime();
  dialog.querySelectorAll("button")[1].click();
  await new Promise((resolve) => setImmediate(resolve));
  const exact = requestPayload(requests[1]);
  assert.equal(exact.requested_action, "queue");
  assert.equal(exact.requested_post_caption, "Public");
  assert.match(exact.requested_publish_at, /^\d{4}-\d{2}-\d{2}T.*Z$/);
});

test("Save… asks only for internal metadata", async () => {
  const requests = [];
  const browser = createBrowser("https://2ch.life/b/res/1.html", requests);
  const post = createPost(browser.document, ["https://2ch.life/b/src/1/clip.webm"]);
  browser.document.body.append(post);
  userscript.boot(browser.root);
  actionButton(post, "save_detailed").click();
  const dialog = browser.document.body.querySelector("dialog");
  assert.equal(dialog.querySelectorAll("input").length, 1);
  assert.equal(dialog.querySelectorAll("textarea").length, 1);
  dialog.querySelector("input").value = "cats";
  dialog.querySelector("textarea").value = "internal";
  dialog.querySelectorAll("button")[1].click();
  await new Promise((resolve) => setImmediate(resolve));
  const payload = requestPayload(requests[0]);
  assert.equal(payload.requested_action, "save");
  assert.deepEqual(payload.tags, ["cats"]);
  assert.equal(payload.description, "internal");
  assert.equal("requested_post_caption" in payload, false);
});

test("buttons suppress in-flight duplicates, reuse timeout IDs, and reset after response", () => {
  const requests = [];
  const browser = createBrowser(
    "https://2ch.org/b/res/1.html",
    requests,
    new Map(),
    () => {}
  );
  const post = createPost(browser.document, ["https://2ch.org/b/src/1/clip.webm"]);
  browser.document.body.append(post);
  userscript.boot(browser.root);
  const save = actionButton(post, "save");

  save.click();
  save.click();
  assert.equal(requests.length, 1);
  const firstId = requestPayload(requests[0]).action_id;
  requests[0].ontimeout();
  save.click();
  assert.equal(requests.length, 2);
  assert.equal(requestPayload(requests[1]).action_id, firstId);
  requests[1].onload({ status: 202 });
  save.click();
  assert.equal(requests.length, 3);
  assert.notEqual(requestPayload(requests[2]).action_id, firstId);
});

test("accepted history is mirror-canonical, thread-local, bounded, and clearable", () => {
  const storage = new Map();
  const firstRequests = [];
  const first = createBrowser(
    "https://2ch.org/b/res/42.html#media",
    firstRequests,
    storage
  );
  const firstPost = createPost(first.document, ["https://2ch.org/b/src/1/clip.webm"]);
  first.document.body.append(firstPost);
  userscript.boot(first.root);
  actionButton(firstPost, "save").click();
  assert.match(firstPost.querySelector(".sooqa-history").textContent, /Accepted requests/);

  const reloadRequests = [];
  const reload = createBrowser(
    "https://2ch.su/b/res/42.html",
    reloadRequests,
    storage
  );
  const reloadPost = createPost(reload.document, ["https://2ch.su/b/src/1/clip.webm"]);
  reload.document.body.append(reloadPost);
  userscript.boot(reload.root);
  assert.match(reloadPost.querySelector(".sooqa-history").textContent, /Accepted requests/);

  const otherThread = createBrowser(
    "https://2ch.life/b/res/43.html",
    [],
    storage
  );
  const otherPost = createPost(otherThread.document, ["https://2ch.life/b/src/1/clip.webm"]);
  otherThread.document.body.append(otherPost);
  userscript.boot(otherThread.root);
  assert.match(otherPost.querySelector(".sooqa-history").textContent, /No accepted/);

  const clear = reload.document.body.querySelector(".sooqa_history_control");
  clear.click();
  assert.equal(storage.get("sooqa_accepted_actions_v1").length, 0);
  assert.match(reloadPost.querySelector(".sooqa-history").textContent, /No accepted/);

  const boundedStorage = new Map();
  const boundedRequests = [];
  const bounded = createBrowser(
    "https://2ch.org/b/res/99.html",
    boundedRequests,
    boundedStorage
  );
  const boundedPost = createPost(bounded.document, ["https://2ch.org/b/src/1/clip.webm"]);
  bounded.document.body.append(boundedPost);
  userscript.boot(bounded.root);
  const boundedButton = actionButton(boundedPost, "save");
  for (let index = 0; index < 205; index += 1) boundedButton.click();
  assert.equal(boundedStorage.get("sooqa_accepted_actions_v1").length, 200);
});

test("metadata contains no backend secrets, polling, or stale update metadata", () => {
  const source = fs.readFileSync(path.join(__dirname, "..", "sooqa-2ch-save.user.js"), "utf8");
  assert.match(source, /@version\s+0\.2\.0/);
  assert.match(source, /@updateURL\s+https:\/\/raw\.githubusercontent\.com\/godlikedh\/sooqa\/main/);
  assert.match(source, /@downloadURL\s+https:\/\/raw\.githubusercontent\.com\/godlikedh\/sooqa\/main/);
  assert.match(source, /GM_xmlhttpRequest/);
  assert.doesNotMatch(source, /setInterval|setTimeout|\/api\/v1\/(ingests|media)/);
  assert.doesNotMatch(source, /telegram|bot.?token|backend.?token/i);
});
