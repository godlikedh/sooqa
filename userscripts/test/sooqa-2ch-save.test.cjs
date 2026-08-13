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
    const attributePattern = /\[([^\]=^]+)(?:\^?=['"]?([^'"]*)['"]?)?\]/;
    const attributeMatch = attributePattern.exec(selector);
    const selectorWithoutAttribute = attributeMatch
      ? selector.replace(attributeMatch[0], "")
      : selector;
    const classMatch = /\.([\w-]+)/.exec(selectorWithoutAttribute);
    const tagName = selectorWithoutAttribute.replace(/\.[\w-]+/, "").trim();
    if (tagName && tagName !== "*" && this.tagName !== tagName.toUpperCase()) return false;
    if (classMatch && !this.className.split(/\s+/).includes(classMatch[1])) return false;
    if (!attributeMatch) return true;

    const name = attributeMatch[1].trim();
    const expected = attributeMatch[2];
    let value = this.getAttribute(name);
    if (name === "href" && !value) value = this.href || null;
    if (name === "src" && !value) value = this.src || null;
    if (name.startsWith("data-") && value === null) {
      const datasetKey = name.slice(5).replace(/-([a-z])/g, (_match, letter) => letter.toUpperCase());
      value = this.dataset[datasetKey] || null;
    }
    if (name === "id" && value === null) value = this.id || null;
    if (expected !== undefined) {
      return attributeMatch[0].includes("^=")
        ? String(value || "").startsWith(expected)
        : String(value || "") === expected;
    }
    return Boolean(value);
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

  focus() {
    if (this.ownerDocument && this.ownerDocument.throwOnFocus) {
      throw new Error("focus failed");
    }
  }

  showModal() {
    if (this.ownerDocument && this.ownerDocument.throwOnShowModal) {
      throw new Error("showModal failed");
    }
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
    this.throwOnFocus = false;
    this.throwOnShowModal = false;
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
  post.className = "post";
  post.setAttribute("data-num", mediaUrls[0]);
  const images = document.createElement("div");
  images.className = "post__images";
  for (const mediaUrl of mediaUrls) {
    const link = document.createElement("a");
    link.href = mediaUrl;
    link.setAttribute("href", mediaUrl);
    images.append(link);
  }
  post.append(images);
  return post;
}

function createFigureAttachment(document, mediaUrl) {
  const figure = document.createElement("figure");
  figure.className = "post__image";

  const caption = document.createElement("figcaption");
  const filename = document.createElement("a");
  filename.href = mediaUrl;
  filename.setAttribute("href", mediaUrl);
  filename.textContent = "clip.webm";
  caption.append(filename);

  const preview = document.createElement("a");
  preview.className = "post__image-link";
  preview.href = mediaUrl;
  preview.setAttribute("href", mediaUrl);
  const image = document.createElement("img");
  image.src = mediaUrl.replace(/\.(?:mp4|webm)(?:\?.*)?$/i, ".jpg");
  image.setAttribute("src", image.src);
  image.className = "post__file-preview";
  preview.append(image);

  figure.append(caption, preview);
  return figure;
}

function createRealThread(document, threadNumber, mediaUrls) {
  const thread = document.createElement("div");
  thread.className = "thread";
  thread.setAttribute("data-num", threadNumber);
  const post = document.createElement("article");
  post.className = "post";
  post.setAttribute("data-num", threadNumber + "-post");
  const images = document.createElement("div");
  images.className = "post__images";
  for (const mediaUrl of mediaUrls) images.append(createFigureAttachment(document, mediaUrl));
  post.append(images);
  thread.append(post);
  return { thread, post };
}

function createDuplicateMediaPost(document, mediaUrl) {
  const post = document.createElement("article");
  post.className = "post";
  post.setAttribute("data-num", mediaUrl);
  const images = document.createElement("div");
  images.className = "post__images";
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
  images.append(link);
  post.append(images);
  return post;
}

function createViewer(document, mediaUrl) {
  const viewer = document.createElement("div");
  viewer.className = "mv";
  const main = document.createElement("div");
  main.className = "mv__main";
  main.setAttribute("id", "js-mv-main");
  const video = document.createElement("video");
  video.className = "mv__player";
  video.setAttribute("id", "js-mv-player");
  video.src = mediaUrl;
  video.setAttribute("src", mediaUrl);
  const source = document.createElement("source");
  source.src = mediaUrl;
  source.setAttribute("src", mediaUrl);
  return { viewer, main, video, source };
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

function assertActionButtonsState(post, disabled) {
  for (const button of actionButtons(post)) assert.equal(button.disabled, disabled);
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

test("omits blank optional metadata while preserving tags and multiline public text", () => {
  const save = userscript.ACTIONS.find((action) => action.key === "save_detailed");
  const postNow = userscript.ACTIONS.find((action) => action.key === "post_now_detailed");
  const tagsOnly = userscript.buildPayload({
    actionId: "action-tags-only",
    mediaUrl: "https://2ch.org/b/src/1/clip.webm",
    pageUrl: "https://2ch.org/b/res/1.html",
    pageTitle: "Thread",
    action: save,
    metadata: { description: " \n\t ", tags: ["cats"] },
  });
  assert.deepEqual(tagsOnly, {
    action_id: "action-tags-only",
    url: "https://2ch.org/b/src/1/clip.webm",
    page_url: "https://2ch.org/b/res/1.html",
    page_title: "Thread",
    requested_action: "save",
    tags: ["cats"],
  });

  const blankPublic = userscript.buildPayload({
    actionId: "action-blank-public",
    mediaUrl: "https://2ch.org/b/src/1/clip.webm",
    pageUrl: "https://2ch.org/b/res/1.html",
    pageTitle: "Thread",
    action: postNow,
    metadata: { description: "Internal", tags: [], publicText: " \n\t " },
  });
  assert.equal("requested_post_caption" in blankPublic, false);
  assert.equal(blankPublic.description, "Internal");

  const multiline = userscript.buildPayload({
    actionId: "action-multiline-public",
    mediaUrl: "https://2ch.org/b/src/1/clip.webm",
    pageUrl: "https://2ch.org/b/res/1.html",
    pageTitle: "Thread",
    action: postNow,
    metadata: { description: "line one\nline two", tags: [], publicText: "caption one\ncaption two\tready" },
  });
  assert.equal(multiline.description, "line one\nline two");
  assert.equal(multiline.requested_post_caption, "caption one\ncaption two\tready");
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
  const realFixture = fs.readFileSync(
    path.join(__dirname, "fixtures", "2ch-real-attachments.html"),
    "utf8"
  );
  const viewerFixture = fs.readFileSync(
    path.join(__dirname, "fixtures", "2ch-viewer-regression.html"),
    "utf8"
  );
  for (const host of ["2ch.su", "2ch.org", "2ch.life"]) {
    const escapedHost = host.replace(".", "\\.");
    assert.match(fixture, new RegExp("https://" + escapedHost + "/.*clip\\.webm"));
    assert.match(fixture, new RegExp("https://" + escapedHost + "/.*clip\\.mp4"));
  }
  assert.match(realFixture, /figure class="post__image"/);
  assert.match(realFixture, /class="post__images"/);
  assert.match(realFixture, /class="post__image-link"/);
  assert.match(realFixture, /figcaption>[\s\S]*clip\.webm/);
  assert.match(viewerFixture, /class="mv"/);
  assert.match(viewerFixture, /id="js-mv-main"/);
  assert.match(viewerFixture, /class="mv__player"/);
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

test("real 2ch figures wrap the preview, not the filename link, across galleries and mirrors", () => {
  for (const host of ["2ch.su", "2ch.org", "2ch.life"]) {
    const requests = [];
    const browser = createBrowser("https://" + host + "/b/", requests);
    const initialUrls = Array.from({ length: 4 }, (_value, index) =>
      "https://" + host + "/b/src/" + (index + 1) + "/initial.webm"
    );
    const initial = createRealThread(browser.document, "4100", initialUrls);
    browser.document.body.append(initial.thread);
    userscript.boot(browser.root);

    assert.equal(initial.post.querySelectorAll(".sooqa-attachment-row").length, 4);
    assert.equal(initial.post.querySelectorAll(".sooqa-action-panel").length, 4);
    assert.equal(initial.post.querySelectorAll("figcaption .sooqa-attachment-row").length, 0);
    for (const row of initial.post.querySelectorAll(".sooqa-attachment-row")) {
      assert.equal(row.querySelectorAll("figure.post__image").length, 1);
      assert.equal(row.querySelectorAll("a.post__image-link").length, 1);
      assert.equal(row.querySelectorAll("button[data-sooqa-action]").length, 6);
    }

    const dynamicUrls = Array.from({ length: 8 }, (_value, index) =>
      "https://" + host + "/b/src/" + (index + 20) + "/gallery.mp4"
    );
    const dynamic = createRealThread(browser.document, "4200", dynamicUrls);
    browser.document.body.append(dynamic.thread);
    assert.equal(dynamic.post.querySelectorAll(".sooqa-attachment-row").length, 8);
    assert.equal(dynamic.post.querySelectorAll(".sooqa-action-panel").length, 8);
  }
});

test("native media viewer mutations stay untouched while new posts still decorate", () => {
  const requests = [];
  const browser = createBrowser("https://2ch.org/b/res/335710210.html", requests);
  const initialUrls = Array.from({ length: 4 }, (_value, index) =>
    "https://2ch.org/b/src/" + (index + 1) + "/initial.webm"
  );
  const initial = createRealThread(browser.document, "335710210", initialUrls);
  browser.document.body.append(initial.thread);
  userscript.boot(browser.root);
  assert.equal(initial.post.querySelectorAll(".sooqa-attachment-row").length, 4);
  const initialRow = initial.post.querySelector(".sooqa-attachment-row");
  browser.document.notifyAdded(initialRow);
  browser.document.notifyAdded(initialRow.querySelector(".sooqa-action-panel"));
  assert.equal(initial.post.querySelectorAll(".sooqa-attachment-row").length, 4);

  const pageVideo = browser.document.createElement("video");
  pageVideo.src = "https://2ch.org/b/src/8/page-level.webm";
  pageVideo.setAttribute("src", pageVideo.src);
  browser.document.body.append(pageVideo);
  assert.equal(browser.document.body.querySelectorAll(".sooqa-attachment-row").length, 4);

  const viewer = createViewer(browser.document, "https://2ch.org/b/src/9/clip.webm");
  browser.document.body.append(viewer.viewer);
  viewer.viewer.append(viewer.main);
  browser.document.notifyAdded(viewer.main);
  viewer.main.append(viewer.video);
  browser.document.notifyAdded(viewer.video);
  viewer.video.append(viewer.source);
  browser.document.notifyAdded(viewer.source);

  const viewerChildren = viewer.viewer.children.slice();
  const mainChildren = viewer.main.children.slice();
  const videoChildren = viewer.video.children.slice();
  assert.equal(viewer.viewer.querySelectorAll(".sooqa-attachment-row").length, 0);
  assert.equal(viewer.viewer.querySelectorAll(".sooqa-action-panel").length, 0);
  assert.equal(viewer.main.parentElement, viewer.viewer);
  assert.equal(viewer.video.parentElement, viewer.main);
  assert.equal(viewer.source.parentElement, viewer.video);

  for (let index = 0; index < 3; index += 1) {
    const update = createViewer(browser.document, "https://2ch.org/b/src/" + (10 + index) + "/clip.webm");
    viewer.viewer.append(update.main);
    browser.document.notifyAdded(update.main);
    update.main.append(update.video);
    browser.document.notifyAdded(update.video);
    update.video.append(update.source);
    browser.document.notifyAdded(update.source);
    assert.equal(update.main.parentElement, viewer.viewer);
    assert.equal(update.video.parentElement, update.main);
    assert.equal(update.source.parentElement, update.video);
    assert.equal(update.main.querySelectorAll(".sooqa-attachment-row").length, 0);
    update.main.remove();
  }

  assert.deepEqual(viewer.viewer.children, viewerChildren);
  assert.deepEqual(viewer.main.children, mainChildren);
  assert.deepEqual(viewer.video.children, videoChildren);
  assert.equal(viewer.viewer.querySelectorAll(".sooqa-attachment-row").length, 0);
  assert.equal(viewer.viewer.querySelectorAll(".sooqa-action-panel").length, 0);

  const dynamicUrls = Array.from({ length: 8 }, (_value, index) =>
    "https://2ch.org/b/src/" + (20 + index) + "/dynamic.mp4"
  );
  const dynamic = createRealThread(browser.document, "335710211", dynamicUrls);
  browser.document.body.append(dynamic.thread);
  assert.equal(dynamic.post.querySelectorAll(".sooqa-attachment-row").length, 8);
  assert.equal(dynamic.post.querySelectorAll(".sooqa-action-panel").length, 8);
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
  assert.equal(browser.document.body.querySelectorAll("dialog").length, 1);
  assert.match(browser.document.body.querySelector("style").textContent, /sooqa-metadata-dialog/);
  assert.match(browser.document.body.querySelector("style").textContent, /::backdrop/);
  assertActionButtonsState(post, true);
  assert.equal(dialog.querySelectorAll("input").length, 1);
  assert.equal(dialog.querySelectorAll("textarea").length, 2);
  dialog.querySelector("input").value = "Cats";
  dialog.querySelectorAll("textarea")[0].value = "Internal";
  dialog.querySelectorAll("textarea")[1].value = "Public";
  dialog.querySelectorAll("button")[1].click();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(browser.document.body.querySelectorAll("dialog").length, 0);
  assertActionButtonsState(post, false);
  assert.equal(requestPayload(requests[0]).requested_action, "post_now");
  assert.equal(requestPayload(requests[0]).requested_post_caption, "Public");
  assert.equal("requested_publish_at" in requestPayload(requests[0]), false);

  actionButton(post, "queue_exact").click();
  dialog = browser.document.body.querySelector("dialog");
  assert.equal(browser.document.body.querySelectorAll("dialog").length, 1);
  assertActionButtonsState(post, true);
  assert.equal(dialog.querySelectorAll("input").length, 2);
  assert.equal(dialog.querySelectorAll("textarea").length, 2);
  dialog.querySelectorAll("input")[0].value = "Cats";
  dialog.querySelectorAll("textarea")[0].value = "Internal";
  dialog.querySelectorAll("textarea")[1].value = "Public";
  dialog.querySelectorAll("input")[1].value = futureLocalDateTime();
  dialog.querySelectorAll("button")[1].click();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(browser.document.body.querySelectorAll("dialog").length, 0);
  assertActionButtonsState(post, false);
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
  assert.equal(browser.document.body.querySelectorAll("dialog").length, 1);
  assertActionButtonsState(post, true);
  assert.equal(dialog.querySelectorAll("input").length, 1);
  assert.equal(dialog.querySelectorAll("textarea").length, 1);
  dialog.querySelector("input").value = "cats";
  dialog.querySelector("textarea").value = "internal";
  dialog.querySelectorAll("button")[1].click();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(browser.document.body.querySelectorAll("dialog").length, 0);
  assertActionButtonsState(post, false);
  const payload = requestPayload(requests[0]);
  assert.equal(payload.requested_action, "save");
  assert.deepEqual(payload.tags, ["cats"]);
  assert.equal(payload.description, "internal");
  assert.equal("requested_post_caption" in payload, false);
});

test("detailed dialog cancel and Escape finish once and restore controls", async () => {
  const requests = [];
  const browser = createBrowser("https://2ch.org/b/res/1.html", requests);
  const post = createPost(browser.document, ["https://2ch.org/b/src/1/clip.webm"]);
  browser.document.body.append(post);
  userscript.boot(browser.root);
  const saveDetailed = actionButton(post, "save_detailed");

  saveDetailed.click();
  let dialog = browser.document.body.querySelector("dialog");
  let closeCalls = 0;
  const close = dialog.close.bind(dialog);
  const remove = dialog.remove.bind(dialog);
  dialog.close = () => {
    closeCalls += 1;
    close();
  };
  dialog.remove = () => {
    assert.equal(closeCalls, 1);
    remove();
  };
  dialog.querySelector(".sooqa-cancel").click();
  dialog.dispatchEvent({ type: "cancel" });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(closeCalls, 1);
  assert.equal(browser.document.body.querySelectorAll("dialog").length, 0);
  assert.equal(requests.length, 0);
  assertActionButtonsState(post, false);

  saveDetailed.click();
  dialog = browser.document.body.querySelector("dialog");
  closeCalls = 0;
  const escapeClose = dialog.close.bind(dialog);
  const escapeRemove = dialog.remove.bind(dialog);
  dialog.close = () => {
    closeCalls += 1;
    escapeClose();
  };
  dialog.remove = () => {
    assert.equal(closeCalls, 1);
    escapeRemove();
  };
  dialog.dispatchEvent({ type: "cancel" });
  dialog.dispatchEvent({ type: "cancel" });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(closeCalls, 1);
  assert.equal(browser.document.body.querySelectorAll("dialog").length, 0);
  assertActionButtonsState(post, false);

  saveDetailed.click();
  assert.equal(browser.document.body.querySelectorAll("dialog").length, 1);
  browser.document.body.querySelector("dialog").querySelector(".sooqa-cancel").click();
  await new Promise((resolve) => setImmediate(resolve));
  assertActionButtonsState(post, false);
});

test("detailed dialog open and focus failures fall back without dead buttons", async () => {
  for (const failure of ["showModal", "focus"]) {
    const requests = [];
    const browser = createBrowser("https://2ch.org/b/res/1.html", requests);
    browser.root.prompt = () => "cats|internal|Public fallback";
    browser.document.throwOnShowModal = failure === "showModal";
    browser.document.throwOnFocus = failure === "focus";
    const post = createPost(browser.document, ["https://2ch.org/b/src/1/clip.webm"]);
    browser.document.body.append(post);
    userscript.boot(browser.root);

    actionButton(post, "post_now_detailed").click();
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(browser.document.body.querySelectorAll("dialog").length, 0);
    assertActionButtonsState(post, false);
    assert.equal(requests.length, 1);
    const payload = requestPayload(requests[0]);
    assert.equal(payload.requested_action, "post_now");
    assert.deepEqual(payload.tags, ["cats"]);
    assert.equal(payload.description, "internal");
    assert.equal(payload.requested_post_caption, "Public fallback");
  }
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

test("detailed interaction is fenced while metadata collection is open", async () => {
  const requests = [];
  const browser = createBrowser("https://2ch.org/b/res/1.html", requests);
  const post = createPost(browser.document, ["https://2ch.org/b/src/1/clip.webm"]);
  browser.document.body.append(post);
  userscript.boot(browser.root);

  actionButton(post, "save_detailed").click();
  actionButton(post, "post_now_detailed").click();
  actionButton(post, "queue").click();
  assert.equal(browser.document.body.querySelectorAll("dialog").length, 1);
  assert.equal(requests.length, 0);

  const dialog = browser.document.body.querySelector("dialog");
  dialog.querySelector("textarea").value = "internal";
  dialog.querySelectorAll("button")[1].click();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(requests.length, 1);
  assert.equal(requestPayload(requests[0]).requested_action, "save");
});

test("derives a canonical thread URL from a board thread container", () => {
  const storage = new Map();
  const firstRequests = [];
  const first = createBrowser("https://2ch.org/b/", firstRequests, storage);
  const firstThread = createRealThread(
    first.document,
    "42",
    ["https://2ch.org/b/src/1/clip.webm"]
  );
  first.document.body.append(firstThread.thread);
  userscript.boot(first.root);
  actionButton(firstThread.post, "save").click();
  assert.equal(requestPayload(firstRequests[0]).page_url, "https://2ch.org/b/res/42.html");
  assert.equal(
    firstThread.post.querySelector(".sooqa-action-panel").dataset.sooqaThreadKey,
    "https://2ch.org/b/res/42.html"
  );

  const reloadRequests = [];
  const reload = createBrowser("https://2ch.su/b/", reloadRequests, storage);
  const reloadThread = createRealThread(
    reload.document,
    "42",
    ["https://2ch.su/b/src/1/clip.webm"]
  );
  reload.document.body.append(reloadThread.thread);
  userscript.boot(reload.root);
  assert.match(reloadThread.post.querySelector(".sooqa-history").textContent, /Accepted requests/);
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
