import { describe, expect, it } from "vitest";

import { resolveDetailPipeline } from "./detailPipeline";

const RECORDS = [
  { record_id: "a", predictions: { p1: {}, p2: {} } },
  { record_id: "b", predictions: { p2: {} } },
  { record_id: "c", predictions: {} },
];

describe("resolveDetailPipeline", () => {
  it("prefers the filtered pipeline when the record has it", () => {
    expect(resolveDetailPipeline("a", RECORDS, "p2")).toBe("p2");
  });

  it("falls back to the record's first pipeline when it lacks the filtered one", () => {
    expect(resolveDetailPipeline("b", RECORDS, "p1")).toBe("p2");
  });

  it("falls back when no pipeline is preferred", () => {
    expect(resolveDetailPipeline("a", RECORDS, null)).toBe("p1");
    expect(resolveDetailPipeline("a", RECORDS)).toBe("p1");
  });

  it("returns null for a record with no predictions", () => {
    expect(resolveDetailPipeline("c", RECORDS, "p1")).toBeNull();
  });

  it("returns null for a record that is not on this page", () => {
    // A shared link can point at a record outside the restored page; the caller
    // must handle null rather than opening an empty detail panel.
    expect(resolveDetailPipeline("missing", RECORDS, "p1")).toBeNull();
  });

  it("agrees between the click path and the restore path", () => {
    for (const id of ["a", "b", "c", "missing"]) {
      const clicked = resolveDetailPipeline(id, RECORDS, "p1");
      const restored = resolveDetailPipeline(id, RECORDS, "p1");
      expect(restored).toBe(clicked);
    }
  });
});
