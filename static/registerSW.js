if ("serviceWorker" in navigator) {
    window.addEventListener("load", () => {
      navigator.serviceWorker.register("./static/sw.js", { scope: "./" });
    });
}
