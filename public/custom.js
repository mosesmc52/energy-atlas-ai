(function () {
  const LINK_ID = "energy-atlas-signin-link";
  const LINK_CLASS = "energy-atlas-signin-link";
  const SIGNIN_PATH = "/auth/signin/";

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
  window.addEventListener("load", ensureSignInLink);
  ensureSignInLink();
})();
