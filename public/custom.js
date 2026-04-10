(function () {
  const GA_MEASUREMENT_ID = "G-4DQHVPWTF2";
  const GA_SCRIPT_ID = "energy-atlas-ga-script";
  const LINK_ID = "energy-atlas-signin-link";
  const LINK_CLASS = "energy-atlas-signin-link";
  const SIGNIN_PATH = "/auth/signin/";

  function ensureGoogleAnalytics() {
    if (document.getElementById(GA_SCRIPT_ID) || typeof document.head === "undefined") {
      return;
    }

    window.dataLayer = window.dataLayer || [];
    window.gtag =
      window.gtag ||
      function gtag() {
        window.dataLayer.push(arguments);
      };

    window.gtag("js", new Date());
    window.gtag("config", GA_MEASUREMENT_ID);

    const script = document.createElement("script");
    script.id = GA_SCRIPT_ID;
    script.async = true;
    script.src = `https://www.googletagmanager.com/gtag/js?id=${GA_MEASUREMENT_ID}`;
    document.head.appendChild(script);
  }

  function ensureSignInLink() {
    const header = document.querySelector("header");
    if (!header) {
      return;
    }

    header.style.position = "relative";

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
    header.appendChild(link);
  }

  const observer = new MutationObserver(() => ensureSignInLink());
  observer.observe(document.documentElement, { childList: true, subtree: true });
  ensureGoogleAnalytics();
  window.addEventListener("load", ensureSignInLink);
  ensureSignInLink();
})();
