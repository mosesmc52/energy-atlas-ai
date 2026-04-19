(function () {
  const GTM_CONTAINER_ID = "GTM-K92P8VMQ";
  const GTM_SCRIPT_ID = "energy-atlas-gtm-script";
  const GTM_NOSCRIPT_ID = "energy-atlas-gtm-noscript";
  const ANALYTICS_MESSAGE_TYPE = "energy_atlas_analytics";
  const LINK_ID = "energy-atlas-signin-link";
  const LINK_CLASS = "energy-atlas-signin-link";
  const SIGNIN_PATH = "/auth/signin/";
  const AUTH_STATUS_PATH = "/auth/status/";
  let authStatusPromise = null;

  function trackEvent(payload) {
    window.dataLayer = window.dataLayer || [];
    window.dataLayer.push(payload);
  }

  function ensureGoogleTagManager() {
    if (!document.head || document.getElementById(GTM_SCRIPT_ID)) {
      return;
    }

    trackEvent({ "gtm.start": new Date().getTime(), event: "gtm.js" });

    const script = document.createElement("script");
    script.id = GTM_SCRIPT_ID;
    script.async = true;
    script.src = `https://www.googletagmanager.com/gtm.js?id=${GTM_CONTAINER_ID}`;
    document.head.appendChild(script);
  }

  function ensureGoogleTagManagerNoScript() {
    if (!document.body || document.getElementById(GTM_NOSCRIPT_ID)) {
      return;
    }

    const noscript = document.createElement("noscript");
    noscript.id = GTM_NOSCRIPT_ID;

    const iframe = document.createElement("iframe");
    iframe.src = `https://www.googletagmanager.com/ns.html?id=${GTM_CONTAINER_ID}`;
    iframe.height = "0";
    iframe.width = "0";
    iframe.style.display = "none";
    iframe.style.visibility = "hidden";
    noscript.appendChild(iframe);

    document.body.insertAdjacentElement("afterbegin", noscript);
  }

  function removeSignInLinks() {
    const signInCandidates = document.querySelectorAll("header a[href]");
    signInCandidates.forEach((link) => {
      const href = String(link.getAttribute("href") || "").trim();
      const text = String(link.textContent || "").trim().toLowerCase();
      const isSignInText = text === "sign in";
      if (!href) {
        return;
      }
      let pathname = "";
      try {
        pathname = new URL(href, window.location.origin).pathname.replace(/\/+$/, "");
      } catch (_error) {
        pathname = href.replace(/\/+$/, "");
      }
      if (isSignInText || pathname === "/auth/signin") {
        link.remove();
      }
    });
  }

  function ensureSignInLink() {
    const header = document.querySelector("header");
    if (!header) {
      return;
    }

    header.style.position = "relative";

    removeSignInLinks();

    const existing = document.getElementById(LINK_ID);
    if (existing) {
      if (existing.parentElement !== header) {
        header.appendChild(existing);
      }
      return;
    }

    const link = document.createElement("a");
    link.id = LINK_ID;
    link.className = LINK_CLASS;
    link.href = SIGNIN_PATH;
    link.textContent = "Sign In";
    link.addEventListener("click", function () {
      trackEvent({
        event: "sign_in_clicked",
        app_surface: "chainlit",
        location: "chainlit_header",
      });
    });
    header.appendChild(link);
  }

  function fetchAuthStatus() {
    if (authStatusPromise) {
      return authStatusPromise;
    }

    authStatusPromise = fetch(AUTH_STATUS_PATH, {
      credentials: "include",
      headers: {
        Accept: "application/json",
      },
    })
      .then((response) => (response.ok ? response.json() : { authenticated: false }))
      .catch(() => ({ authenticated: false }));

    return authStatusPromise;
  }

  function syncSignInVisibility() {
    fetchAuthStatus().then((payload) => {
      if (payload && payload.authenticated === false) {
        ensureSignInLink();
      } else {
        removeSignInLinks();
      }
    });
  }

  window.addEventListener("message", function (event) {
    const payload = event.data;
    if (!payload || payload.type !== ANALYTICS_MESSAGE_TYPE || !payload.event) {
      return;
    }

    const { type, ...analyticsEvent } = payload;
    trackEvent(analyticsEvent);
  });

  const observer = new MutationObserver(() => syncSignInVisibility());
  observer.observe(document.documentElement, { childList: true, subtree: true });
  ensureGoogleTagManager();
  ensureGoogleTagManagerNoScript();
  window.addEventListener("load", syncSignInVisibility);
  window.addEventListener("load", ensureGoogleTagManagerNoScript);
  syncSignInVisibility();
})();
