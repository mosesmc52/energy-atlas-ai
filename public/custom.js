(function () {
  const GTM_CONTAINER_ID = "GTM-K92P8VMQ";
  const GTM_SCRIPT_ID = "energy-atlas-gtm-script";
  const GTM_NOSCRIPT_ID = "energy-atlas-gtm-noscript";
  const ANALYTICS_MESSAGE_TYPE = "energy_atlas_analytics";
  const SIGNIN_PATH = "/auth/signin/";
  const ALERTS_PATH = "/alerts/";
  const AUTH_STATUS_PATH = "/auth/status/";
  const DJANGO_AUTH_HINT_PARAM = "ea_from_django";
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
    const signInCandidates = document.querySelectorAll("a[href]");
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

  function _isSignInLink(link) {
    const href = String(link.getAttribute("href") || "").trim();
    const text = String(link.textContent || "").trim().toLowerCase();
    if (!href) {
      return false;
    }
    let pathname = "";
    try {
      pathname = new URL(href, window.location.origin).pathname.replace(/\/+$/, "");
    } catch (_error) {
      pathname = href.replace(/\/+$/, "");
    }
    return text === "sign in" || pathname === "/auth/signin";
  }

  function replaceSignInWithAlertsLink() {
    const links = Array.from(document.querySelectorAll("a[href]"));
    const signInLinks = links.filter((link) => _isSignInLink(link));
    if (!signInLinks.length) {
      return;
    }

    signInLinks.forEach((link, index) => {
      if (index === 0) {
        link.setAttribute("href", ALERTS_PATH);
        link.setAttribute("target", "_self");
        link.setAttribute("rel", "");

        const labelCandidates = Array.from(link.querySelectorAll("*"));
        let updated = false;
        labelCandidates.forEach((node) => {
          if (String(node.textContent || "").trim().toLowerCase() === "sign in") {
            node.textContent = "Alerts";
            updated = true;
          }
        });
        if (!updated) {
          link.textContent = "Alerts";
        }
      } else {
        link.remove();
      }
    });
  }

  function hasDjangoAuthHint() {
    const params = new URLSearchParams(window.location.search || "");
    const value = String(params.get(DJANGO_AUTH_HINT_PARAM) || "").trim().toLowerCase();
    return value === "1" || value === "true" || value === "yes" || value === "on";
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
    if (hasDjangoAuthHint()) {
      replaceSignInWithAlertsLink();
      return;
    }

    fetchAuthStatus().then((payload) => {
      if (payload && payload.authenticated === true) {
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
