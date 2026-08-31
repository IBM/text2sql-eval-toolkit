import { describe, expect, it } from "vitest";

import { isRealDocumentLoad } from "./iframeLoad";

/**
 * A real iframe cannot be driven in jsdom, so the two cases are supplied as
 * fakes. They are the two the browser actually produces, measured against the
 * deployment: a same-origin `about:blank` at 3 ms, then a cross-origin document
 * whose location throws on read.
 */
const frameShowing = (href: string) =>
  ({ contentWindow: { location: { href } } }) as unknown as HTMLIFrameElement;

const frameThatThrows = () =>
  ({
    get contentWindow(): Window {
      throw new DOMException("Blocked a frame from accessing a cross-origin frame.");
    },
  }) as unknown as HTMLIFrameElement;

describe("isRealDocumentLoad", () => {
  it("ignores the initial about:blank load", () => {
    // The event that fired 3 ms in and cleared the placeholder too early.
    expect(isRealDocumentLoad(frameShowing("about:blank"))).toBe(false);
  });

  it("accepts a cross-origin document, whose location cannot be read", () => {
    expect(isRealDocumentLoad(frameThatThrows())).toBe(true);
  });

  it("accepts a readable document that is not about:blank", () => {
    expect(isRealDocumentLoad(frameShowing("https://example.invalid/"))).toBe(
      true,
    );
  });

  it("does not treat a missing contentWindow as loaded", () => {
    expect(isRealDocumentLoad({} as HTMLIFrameElement)).toBe(false);
  });
});
