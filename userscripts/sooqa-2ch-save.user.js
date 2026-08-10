// ==UserScript==
// @name         sooqa: save 2ch media
// @namespace    sooqa
// @version      0.1.0
// @description  Add one-click Save controls to direct MP4/WebM attachments.
// @match        https://2ch.org/*
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
  const SUPPORTED_MEDIA = /\.(?:mp4|webm)$/i;

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

  function buildPayload({ actionId, mediaUrl, pageUrl, pageTitle, description, tags }) {
    return {
      action_id: actionId,
      url: mediaUrl,
      page_url: pageUrl || null,
      page_title: pageTitle || null,
      description: description || null,
      tags: Array.isArray(tags) ? tags : parseTags(tags),
    };
  }

  function findPostContainer(node) {
    const post = node.closest("article, .post, [data-num], [id^='p']");
    if (post) return post;
    const parent = node.parentElement;
    if (parent && (parent.tagName === "VIDEO" || parent.tagName === "SOURCE")) {
      return parent.parentElement;
    }
    return parent;
  }

  function makeButton(document, label, className) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = className;
    button.textContent = label;
    return button;
  }

  function collectMetadata(env) {
    if (!env.document.createElement("dialog").showModal) {
      const value = env.prompt("Tags (comma-separated) | Internal description:", "");
      if (value === null) return Promise.resolve(null);
      const [tagText = "", ...descriptionParts] = value.split("|");
      return Promise.resolve({
        description: descriptionParts.join("|").trim(),
        tags: parseTags(tagText),
      });
    }

    return new Promise((resolve) => {
      const dialog = env.document.createElement("dialog");
      dialog.className = "sooqa-metadata-dialog";
      const form = env.document.createElement("form");
      form.method = "dialog";
      const tagsLabel = env.document.createElement("label");
      tagsLabel.textContent = "Tags (comma-separated)";
      const tags = env.document.createElement("input");
      tags.type = "text";
      tags.name = "tags";
      tags.autocomplete = "off";
      tagsLabel.append(tags);
      const descriptionLabel = env.document.createElement("label");
      descriptionLabel.textContent = "Internal description";
      const description = env.document.createElement("textarea");
      description.name = "description";
      description.rows = 3;
      descriptionLabel.append(description);
      const actions = env.document.createElement("div");
      const cancel = makeButton(env.document, "Cancel", "sooqa-cancel");
      cancel.value = "cancel";
      const save = makeButton(env.document, "Save", "sooqa-confirm");
      save.value = "save";
      actions.append(cancel, save);
      form.append(tagsLabel, descriptionLabel, actions);
      dialog.append(form);

      const finish = (value) => {
        dialog.remove();
        resolve(value);
      };
      form.addEventListener("submit", (event) => {
        event.preventDefault();
        finish({ description: description.value.trim(), tags: parseTags(tags.value) });
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
      tags.focus();
    });
  }

  function decorate(root, env) {
    const nodes = [];
    if (root.matches && root.matches("a[href], video[src], source[src]")) nodes.push(root);
    nodes.push(...Array.from(root.querySelectorAll("a[href], video[src], source[src]")));
    for (const node of Array.from(nodes)) {
      const mediaUrl = extractDirectAttachmentUrls([node], env.location.href)[0];
      if (!mediaUrl) continue;
      const container = findPostContainer(node);
      if (!container) continue;
      const alreadyDecorated = Array.from(
        container.querySelectorAll("[data-sooqa-media-url]")
      ).some((control) => control.dataset.sooqaMediaUrl === mediaUrl);
      if (alreadyDecorated) continue;

      const controls = env.document.createElement("span");
      controls.className = "sooqa-save-controls";
      controls.dataset.sooqaMediaUrl = mediaUrl;
      const save = makeButton(env.document, "Save", "sooqa-save");
      const saveDetailed = makeButton(env.document, "Save...", "sooqa-save-detailed");
      const status = env.document.createElement("span");
      status.className = "sooqa-save-status";
      const state = { actionId: null, payload: null, mode: null, sending: false, sent: false };

      const setStatus = (value) => {
        status.textContent = value;
      };

      const submit = async (mode) => {
        if (state.sending || state.sent) return;
        if (state.mode && state.mode !== mode) return;

        state.sending = true;
        save.disabled = true;
        saveDetailed.disabled = true;
        if (!state.payload) {
          let description = "";
          let tags = [];
          if (mode === "detailed") {
            const metadata = await collectMetadata(env);
            if (!metadata) {
              state.sending = false;
              save.disabled = false;
              saveDetailed.disabled = false;
              return;
            }
            description = metadata.description;
            tags = metadata.tags;
          }
          state.mode = mode;
          state.actionId = env.crypto.randomUUID();
          state.payload = buildPayload({
            actionId: state.actionId,
            mediaUrl,
            pageUrl: env.location.href,
            pageTitle: env.document.title,
            description,
            tags,
          });
        }

        const token = typeof env.GM_getValue === "function"
          ? env.GM_getValue(TOKEN_STORAGE_KEY, "")
          : "";
        if (!token) {
          state.sending = false;
          save.disabled = false;
          saveDetailed.disabled = false;
          setStatus("Configure companion token");
          return;
        }
        if (typeof env.GM_xmlhttpRequest !== "function") {
          state.sending = false;
          save.disabled = false;
          saveDetailed.disabled = false;
          setStatus("Tampermonkey request API unavailable");
          return;
        }
        setStatus("Saving…");
        env.GM_xmlhttpRequest({
          method: "POST",
          url: COMPANION_ENDPOINT,
          headers: {
            "Authorization": `Bearer ${token}`,
            "Content-Type": "application/json",
          },
          data: JSON.stringify(state.payload),
          timeout: 15_000,
          onload(response) {
            state.sending = false;
            if (response.status >= 200 && response.status < 300) {
              state.sent = true;
              setStatus("Accepted");
              return;
            }
            save.disabled = false;
            saveDetailed.disabled = false;
            setStatus("Save failed; retry");
          },
          onerror() {
            state.sending = false;
            save.disabled = false;
            saveDetailed.disabled = false;
            setStatus("Save failed; retry");
          },
          ontimeout() {
            state.sending = false;
            save.disabled = false;
            saveDetailed.disabled = false;
            setStatus("Save timed out; retry");
          },
        });
      };

      save.addEventListener("click", () => submit("simple"));
      saveDetailed.addEventListener("click", () => submit("detailed"));
      controls.append(save, " ", saveDetailed, " ", status);
      container.append(" ", controls);
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
      prompt: root.prompt.bind(root),
      GM_getValue: root.GM_getValue,
      GM_xmlhttpRequest: root.GM_xmlhttpRequest,
    };
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
    buildPayload,
    decorate,
    extractDirectAttachmentUrls,
    normalizeDirectMediaUrl,
    parseTags,
    boot,
  };
});
