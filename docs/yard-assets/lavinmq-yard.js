(() => {
  const STORAGE_KEY = "lavinmq-docs-theme";
  const MESSAGE_TYPE = "lavinmq-docs-theme";

  function preferredTheme() {
    try {
      const stored = window.localStorage.getItem(STORAGE_KEY);
      if (stored === "light" || stored === "dark") return stored;
    } catch (_error) {}

    if (window.matchMedia?.("(prefers-color-scheme: dark)").matches) {
      return "dark";
    }

    return "light";
  }

  function currentTheme() {
    const theme = document.documentElement.dataset.theme;
    return theme === "dark" ? "dark" : "light";
  }

  function setStoredTheme(theme) {
    try {
      window.localStorage.setItem(STORAGE_KEY, theme);
    } catch (_error) {}
  }

  function updateToggle(button) {
    if (!button) return;

    const theme = currentTheme();
    const nextTheme = theme === "dark" ? "light" : "dark";
    button.setAttribute("aria-label", `Switch to ${nextTheme} mode`);
    button.setAttribute("title", `Switch to ${nextTheme} mode`);
    button.setAttribute("aria-pressed", theme === "dark" ? "true" : "false");
  }

  function broadcastTheme(theme) {
    const message = { type: MESSAGE_TYPE, theme };
    const nav = document.getElementById("nav");

    if (nav?.contentWindow) {
      nav.contentWindow.postMessage(message, "*");
    }

    if (window.parent && window.parent !== window) {
      window.parent.postMessage(message, "*");
    }
  }

  function applyTheme(theme, options = {}) {
    const normalized = theme === "dark" ? "dark" : "light";
    const toggle = document.querySelector(".lavinmq-theme-toggle");

    document.documentElement.dataset.theme = normalized;
    document.documentElement.style.colorScheme = normalized;
    updateToggle(toggle);

    if (options.store !== false) setStoredTheme(normalized);
    if (options.broadcast) broadcastTheme(normalized);
  }

  function installToggle() {
    const header = document.getElementById("header");
    if (!header || document.querySelector(".lavinmq-theme-toggle")) return;

    const button = document.createElement("button");
    button.type = "button";
    button.className = "lavinmq-theme-toggle";
    button.innerHTML =
      '<span class="lavinmq-theme-toggle__icon" aria-hidden="true"></span>';

    button.addEventListener("click", () => {
      applyTheme(currentTheme() === "dark" ? "light" : "dark", {
        broadcast: true,
      });
    });

    header.appendChild(button);
    updateToggle(button);
  }

  function expandItem(item) {
    if (!item) return;

    item.classList.remove("collapsed");

    const toggle = item.querySelector(":scope > .item > a.toggle");
    if (toggle) toggle.setAttribute("aria-expanded", "true");
  }

  function expandClientNamespace() {
    const client = document.getElementById("object_AMQP::Client");

    if (!client) return;

    expandItem(document.getElementById("object_AMQP"));
    expandItem(client);
    client.querySelectorAll("li").forEach(expandItem);
  }

  function ready(callback) {
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", callback, { once: true });
    } else {
      callback();
    }
  }

  applyTheme(document.documentElement.dataset.theme || preferredTheme(), {
    store: false,
  });

  window.addEventListener("message", (event) => {
    if (event.data?.type === MESSAGE_TYPE) {
      applyTheme(event.data.theme, { store: false });
    }
  });

  window.addEventListener("storage", (event) => {
    if (event.key === STORAGE_KEY && event.newValue) {
      applyTheme(event.newValue, { store: false });
    }
  });

  ready(installToggle);
  ready(expandClientNamespace);
})();
