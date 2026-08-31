import { describe, expect, it } from "vitest";

import { enhance } from "./richContent";

/**
 * Only the table pass is exercised here.
 *
 * KaTeX and Mermaid are fetched by dynamic `import()` and only when a document
 * contains maths or a diagram, which is the behaviour that keeps a plain note
 * from paying for either. A container with neither therefore reaches no
 * dynamic import at all, and that is what these assert -- along with the table
 * wrapping, which is synchronous and needs no library.
 *
 * The rendering itself is verified against the real survey in a browser; jsdom
 * has no layout, so a Mermaid diagram measured here would be meaningless.
 */
const mount = (html: string): HTMLElement => {
  const root = document.createElement("div");
  root.innerHTML = html;
  return root;
};

const TABLE = "<table><thead><tr><th>a</th></tr></thead><tbody><tr><td>1</td></tr></tbody></table>";

describe("enhance", () => {
  it("gives a table its own scroller", () => {
    const root = mount(TABLE);
    return enhance(root).then(() => {
      const scroller = root.querySelector(".table-scroll");
      expect(scroller).not.toBeNull();
      expect(scroller!.firstElementChild?.tagName).toBe("TABLE");
    });
  });

  it("makes the scroller reachable by keyboard", () => {
    // A scrollable region with no tabindex cannot be scrolled without a mouse,
    // so a wide table's right-hand columns are unreadable.
    const root = mount(TABLE);
    return enhance(root).then(() => {
      const scroller = root.querySelector<HTMLElement>(".table-scroll")!;
      expect(scroller.tabIndex).toBe(0);
      expect(scroller.getAttribute("role")).toBe("region");
    });
  });

  it("wraps every table, not only the first", async () => {
    const root = mount(TABLE + TABLE + TABLE);
    await enhance(root);
    expect(root.querySelectorAll(".table-scroll > table")).toHaveLength(3);
  });

  it("is idempotent, so a re-render does not nest scrollers", async () => {
    const root = mount(TABLE);
    await enhance(root);
    await enhance(root);
    expect(root.querySelectorAll(".table-scroll")).toHaveLength(1);
  });

  it("leaves a document with no table, maths or diagram untouched", async () => {
    const root = mount("<p>Just prose.</p>");
    const before = root.innerHTML;
    await enhance(root);
    expect(root.innerHTML).toBe(before);
  });

  it("stops between passes when cancelled", async () => {
    // The reader navigated away; there is no point mutating a container that
    // is about to be replaced.
    const root = mount(TABLE);
    await enhance(root, () => true);
    // The synchronous pass still ran -- it is free and happens first.
    expect(root.querySelector(".table-scroll")).not.toBeNull();
  });
});
