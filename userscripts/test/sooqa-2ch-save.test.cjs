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
    if (selector === "[data-sooqa-media-url]") return Boolean(this.dataset.sooqaMediaUrl);
    if (selector === "[data-num]") return this.attributes.has("data-num");
    if (selector === "[id^='p']") return String(this.id || "").startsWith("p");
    if (selector.startsWith(".")) return this.className.split(/\s+/).includes(selector.slice(1));
    return this.tagName === selector.toUpperCase();
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

function createPost(document, mediaUrl) {
  const post = document.createElement("article");
  post.setAttribute("data-num", mediaUrl);
  const link = document.createElement("a");
  link.href = mediaUrl;
  link.setAttribute("href", mediaUrl);
  post.append(link);
  return post;
}

function createBrowser(url, requests) {
  const document = new FakeDocument(url);
  let sequence = 0;
  const root = {
    document,
    location: document.location,
    crypto: { randomUUID: () => `action-${++sequence}` },
    MutationObserver: class {
      constructor(callback) {
        this.callback = callback;
      }

      observe(target) {
        target.ownerDocument.observers.push(this);
      }
    },
    prompt: () => null,
    GM_getValue: () => "local-token",
    GM_setValue: () => {},
    GM_xmlhttpRequest: (request) => {
      requests.push(request);
      request.onload({ status: 202 });
    },
  };
  return { document, root };
}

function controlButtons(post) {
  const controls = post.querySelectorAll("[data-sooqa-media-url]");
  assert.equal(controls.length, 1);
  return controls[0].children.filter((child) => typeof child !== "string");
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

test("normalizes comma-separated tags and builds an internal metadata payload", () => {
  const payload = userscript.buildPayload({
    actionId: "action-1",
    mediaUrl: "https://2ch.org/b/src/1/clip.webm",
    pageUrl: "https://2ch.org/b/res/1.html",
    pageTitle: "Thread",
    description: "Internal note",
    tags: userscript.parseTags(" Cats, reaction, cats "),
  });

  assert.deepEqual(payload, {
    action_id: "action-1",
    url: "https://2ch.org/b/src/1/clip.webm",
    page_url: "https://2ch.org/b/res/1.html",
    page_title: "Thread",
    description: "Internal note",
    tags: ["cats", "reaction"],
  });
});

test("fixture keeps the supported page surface narrow", () => {
  const fixture = fs.readFileSync(
    path.join(__dirname, "fixtures", "2ch-direct-attachments.html"),
    "utf8"
  );
  for (const host of ["2ch.su", "2ch.org", "2ch.life"]) {
    assert.match(fixture, new RegExp(`https://${host.replace(".", "\\.")}/.*clip\\.webm`));
    assert.match(fixture, new RegExp(`https://${host.replace(".", "\\.")}/.*clip\\.mp4`));
  }
  assert.doesNotMatch(fixture, /youtube|yt-dlp/i);
});

test("each supported host extracts MP4 and WebM attachments", () => {
  const hosts = ["2ch.su", "2ch.org", "2ch.life"];
  const nodes = hosts.flatMap((host, index) => [
    { href: `https://${host}/b/src/${index}/clip.webm` },
    { href: `https://${host}/b/src/${index}/clip.mp4` },
  ]);

  assert.equal(
    userscript.extractDirectAttachmentUrls(nodes, "https://2ch.org/b/res/1.html").length,
    6
  );
  const source = fs.readFileSync(path.join(__dirname, "..", "sooqa-2ch-save.user.js"), "utf8");
  for (const host of hosts) {
    assert.match(source, new RegExp(`@match\\s+https://${host.replace(".", "\\.")}/\\*`));
  }
});

test("initial and dynamically inserted attachments get one control pair per supported host", () => {
  for (const host of ["2ch.su", "2ch.org", "2ch.life"]) {
    const requests = [];
    const { document, root } = createBrowser(`https://${host}/b/res/1.html`, requests);
    document.body.append(createPost(document, `https://${host}/b/src/1/initial.webm`));
    userscript.boot(root);

    const initialPost = document.body.children.find((child) => child.tagName === "ARTICLE");
    assert.equal(controlButtons(initialPost).filter((child) => child.tagName === "BUTTON").length, 2);

    const dynamicPost = createPost(document, `https://${host}/b/src/2/dynamic.mp4`);
    document.body.append(dynamicPost);
    document.notifyAdded(dynamicPost);
    assert.equal(controlButtons(dynamicPost).filter((child) => child.tagName === "BUTTON").length, 2);
  }
});

test("Save... opens one dialog and submits its metadata exactly once", async () => {
  const requests = [];
  const { document, root } = createBrowser("https://2ch.org/b/res/1.html", requests);
  const post = createPost(document, "https://2ch.org/b/src/1/detail.webm");
  document.body.append(post);
  userscript.boot(root);

  const buttons = controlButtons(post).filter((child) => child.tagName === "BUTTON");
  assert.equal(buttons.length, 2);
  buttons[1].click();

  const dialog = document.body.querySelector("dialog");
  assert.ok(dialog);
  dialog.querySelector("input").value = "Cats, reaction, cats";
  dialog.querySelector("textarea").value = "Internal note";
  dialog.querySelectorAll("button")[1].click();
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(requests.length, 1);
  assert.deepEqual(JSON.parse(requests[0].data), {
    action_id: "action-1",
    url: "https://2ch.org/b/src/1/detail.webm",
    page_url: "https://2ch.org/b/res/1.html",
    page_title: "Fixture thread",
    description: "Internal note",
    tags: ["cats", "reaction"],
  });
});

test("Save... uses one metadata dialog and no workflow polling", () => {
  const source = fs.readFileSync(path.join(__dirname, "..", "sooqa-2ch-save.user.js"), "utf8");
  assert.match(source, /createElement\("dialog"\)/);
  assert.match(source, /GM_xmlhttpRequest/);
  assert.doesNotMatch(source, /setInterval|setTimeout|\/api\/v1\/(ingests|media)/);
});
