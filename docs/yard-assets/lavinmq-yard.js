(() => {
  const STORAGE_KEY = "lavinmq-docs-theme";
  const MESSAGE_TYPE = "lavinmq-docs-theme";
  // YARD shows the nav as a dropdown below this width (see style.css).
  const MOBILE_NAV_QUERY = "(max-width: 920px)";

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

  function collapseItem(item) {
    if (!item) return;

    item.classList.add("collapsed");

    const toggle = item.querySelector(":scope > .item > a.toggle");
    if (toggle) toggle.setAttribute("aria-expanded", "false");
  }

  function expandClientNamespace() {
    const client = document.getElementById("object_AMQP::Client");

    if (!client) return;

    expandItem(document.getElementById("object_AMQP"));
    expandItem(client);
    client.querySelectorAll("li").forEach(collapseItem);
  }

  function mobileNavOpen() {
    const nav = document.getElementById("nav");

    return (
      window.matchMedia(MOBILE_NAV_QUERY).matches &&
      nav?.style.display === "block"
    );
  }

  function closeMobileNav() {
    document.getElementById("nav")?.removeAttribute("style");
    document.querySelectorAll("#search a").forEach((link) => {
      link.classList.remove("active", "inactive");
    });
  }

  function openMobileNav(link) {
    document.getElementById("nav").style.display = "block";
    link.classList.add("active");
  }

  // Replaces YARD's toggle, which only closes while the frame keeps the src it
  // was opened with, and is never rebound after in-page navigation because
  // the copied header keeps YARD's "bound" marker.
  function toggleMobileNav(event) {
    const link = event.target.closest(".full_list_link");

    if (!link || !window.matchMedia(MOBILE_NAV_QUERY).matches) return;

    event.preventDefault();
    event.stopImmediatePropagation();

    if (mobileNavOpen()) {
      closeMobileNav();
    } else {
      openMobileNav(link);
    }
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
    } else if (event.data?.action === "expand") {
      setTimeout(expandClientNamespace, 0);
    } else if (event.data?.action === "navigate" && mobileNavOpen()) {
      closeMobileNav();
    }
  });

  document.addEventListener("click", toggleMobileNav, true);

  window.addEventListener("storage", (event) => {
    if (event.key === STORAGE_KEY && event.newValue) {
      applyTheme(event.newValue, { store: false });
    }
  });

  ready(installToggle);
  ready(expandClientNamespace);
})();
