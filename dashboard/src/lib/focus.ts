export function deferFocus(target: () => HTMLElement | null | undefined): void {
  const focus = () => {
    target()?.focus();
  };

  if (typeof requestAnimationFrame === "function") {
    requestAnimationFrame(focus);
    return;
  }

  setTimeout(focus, 0);
}
