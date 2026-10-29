import '@testing-library/jest-dom/vitest';
import { afterAll, afterEach } from 'vitest';

globalThis.ResizeObserver = class {
  observe() {}
  unobserve() {}
  disconnect() {}
};

// Radix Select scrolls the active option; jsdom does not implement layout scrolling.
HTMLElement.prototype.scrollIntoView = () => {};
HTMLElement.prototype.scrollTo = () => {};
HTMLElement.prototype.hasPointerCapture = () => false;
HTMLElement.prototype.setPointerCapture = () => {};
HTMLElement.prototype.releasePointerCapture = () => {};

// Capture the matching cancellation function even when a test switches clocks.
// Pending component animations must not outlive the jsdom Document.
const animationFrames = new Map<number, () => void>();
let nextAnimationFrame = 0;
globalThis.requestAnimationFrame = (callback) => {
  const id = ++nextAnimationFrame;
  const cancel = clearTimeout;
  const timer = setTimeout(() => {
    animationFrames.delete(id);
    callback(performance.now());
  }, 16);
  animationFrames.set(id, () => cancel(timer));
  return id;
};
globalThis.cancelAnimationFrame = (id) => {
  animationFrames.get(id)?.();
  animationFrames.delete(id);
};
function clearAnimationFrames() {
  for (const cancel of animationFrames.values()) cancel();
  animationFrames.clear();
}
afterEach(clearAnimationFrames);
afterAll(clearAnimationFrames);

// jsdom has no layout engine; Ant Design observes breakpoints with matchMedia.
Object.defineProperty(window, 'matchMedia', {
  writable: true,
  value: (query: string) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: () => {},
    removeListener: () => {},
    addEventListener: () => {},
    removeEventListener: () => {},
    dispatchEvent: () => false,
  }),
});
