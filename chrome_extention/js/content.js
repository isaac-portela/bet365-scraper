// js/content.js

// Injeta hook.js no contexto da página
(function injectHook() {
  const s = document.createElement("script");
  s.src = chrome.runtime.getURL("js/hook.js");
  s.onload = () => s.remove();
  (document.head || document.documentElement).appendChild(s);
})();

// Escuta o CustomEvent do hook e repassa pro background
window.addEventListener("sendToAPI", (event) => {
  try {
    chrome.runtime.sendMessage({
      type: "SEND_HTTP",
      data: event.detail
    });
  } catch (e) {
    console.error("content.js sendMessage error:", e);
  }
});
