import { describe, expect, it } from "vitest";

import { parseLocation, routes } from "./routes";

/**
 * The docs view's addresses.
 *
 * The point of the whole item is that a specific document can be linked to, so
 * `/docs/state-of-the-art` resolving to that document -- and surviving a
 * round trip -- is the requirement rather than a detail.
 */
describe("docs routes", () => {
  it("builds /docs for the reference and /docs/{name} for a document", () => {
    expect(routes.docs()).toBe("/docs");
    expect(routes.docs(null)).toBe("/docs");
    expect(routes.docs("state-of-the-art")).toBe("/docs/state-of-the-art");
  });

  it("resolves /docs to the view with no document selected", () => {
    const match = parseLocation("/docs");
    expect(match.view).toBe("docs");
    expect(match.configName).toBeNull();
    expect(match.notFound).toBe(false);
  });

  it("resolves /docs/{name} to that document", () => {
    const match = parseLocation("/docs/state-of-the-art");
    expect(match.view).toBe("docs");
    expect(match.configName).toBe("state-of-the-art");
  });

  it("round-trips a document name through the builder and the parser", () => {
    const name = "worked-examples";
    expect(parseLocation(routes.docs(name)).configName).toBe(name);
  });

  it("does not claim a path with extra segments under /docs", () => {
    // /docs/a/b is a mangled link; resolving it to /docs/a would open a
    // different document than the one the address names.
    const match = parseLocation("/docs/a/b");
    expect(match.view).not.toBe("docs");
    expect(match.notFound).toBe(true);
  });

  it("does not swallow other top-level routes", () => {
    expect(parseLocation("/benchmarks").view).toBe("benchmarks");
    expect(parseLocation("/my-keys").view).toBe("myKeys");
    expect(parseLocation("/llm-judge").view).toBe("llmJudge");
  });
});
