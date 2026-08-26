import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterEach, vi } from "vitest";

// Each test starts from a clean DOM and no leftover fetch stubs; without this,
// a component mounted by one test is still in the document during the next.
afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  // restoreAllMocks does not undo vi.stubGlobal, and the fetch stubs are
  // installed that way -- without this they survive into the next test, which is
  // exactly the leakage the hook above is supposed to prevent.
  vi.unstubAllGlobals();
});

// jsdom implements neither, and Carbon reaches for both.
if (!window.matchMedia) {
  window.matchMedia = ((query: string) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: () => {},
    removeListener: () => {},
    addEventListener: () => {},
    removeEventListener: () => {},
    dispatchEvent: () => false,
  })) as typeof window.matchMedia;
}

if (!window.ResizeObserver) {
  window.ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
}
