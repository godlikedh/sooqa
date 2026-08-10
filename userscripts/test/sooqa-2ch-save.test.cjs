const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const assert = require("node:assert/strict");

const userscript = require("../sooqa-2ch-save.user.js");

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
  assert.match(fixture, /clip\.webm/);
  assert.match(fixture, /clip\.mp4/);
  assert.doesNotMatch(fixture, /youtube|yt-dlp/i);
});

test("Save... uses one metadata dialog and no workflow polling", () => {
  const source = fs.readFileSync(path.join(__dirname, "..", "sooqa-2ch-save.user.js"), "utf8");
  assert.match(source, /createElement\("dialog"\)/);
  assert.match(source, /GM_xmlhttpRequest/);
  assert.doesNotMatch(source, /setInterval|setTimeout|\/api\/v1\/(ingests|media)/);
});
