import '@testing-library/jest-dom/vitest';

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

globalThis.requestAnimationFrame = (callback) =>
  setTimeout(() => callback(performance.now()), 16) as unknown as number;
globalThis.cancelAnimationFrame = (handle) => clearTimeout(handle);

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
