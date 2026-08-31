import { describe, expect, it } from "vitest";

import { renderMarkdown } from "./markdown";

/**
 * The renderer, and mostly the sanitiser.
 *
 * The documents are authored and trusted today. The tests below are about the
 * day they are not: a renderer that emits raw HTML is a script-injection point
 * the moment a document is generated, pasted or contributed, and "it is our own
 * file" is exactly the assumption that ages badly.
 */
describe("renderMarkdown", () => {
  it("renders ordinary Markdown", () => {
    const html = renderMarkdown("# Title\n\nSome **bold** and `code`.\n");
    expect(html).toContain("<h1");
    expect(html).toContain("<strong>bold</strong>");
    expect(html).toContain("<code>code</code>");
  });

  it("renders fenced code blocks and tables", () => {
    expect(renderMarkdown("```sql\nSELECT 1;\n```\n")).toContain("<pre>");
    expect(renderMarkdown("| a | b |\n|---|---|\n| 1 | 2 |\n")).toContain(
      "<table>",
    );
  });

  it("does not treat a hard wrap as a line break", () => {
    // These documents wrap at 80 columns. Rendering each wrap as <br> would
    // shred every paragraph in them.
    expect(renderMarkdown("one\ntwo\n")).not.toContain("<br");
  });

  it("strips a script tag", () => {
    const html = renderMarkdown("<script>alert(1)</script>\n\ntext\n");
    expect(html).not.toContain("<script");
    expect(html).not.toContain("alert(1)");
  });

  it("strips an inline event handler", () => {
    const html = renderMarkdown('<img src=x onerror="alert(1)">\n');
    expect(html).not.toContain("onerror");
  });

  it("strips a javascript: link", () => {
    const html = renderMarkdown("[click](javascript:alert(1))\n");
    expect(html).not.toContain("javascript:");
  });

  it("strips an iframe, so a document cannot embed another origin", () => {
    const html = renderMarkdown('<iframe src="https://example.invalid"></iframe>\n');
    expect(html).not.toContain("<iframe");
  });

  it("strips a form, so a document cannot collect input", () => {
    const html = renderMarkdown(
      '<form action="https://example.invalid"><input name="p"></form>\n',
    );
    expect(html).not.toContain("<form");
    expect(html).not.toContain("<input");
  });

  it("strips a style tag, so a document cannot restyle the dashboard", () => {
    const html = renderMarkdown("<style>body{display:none}</style>\n\ntext\n");
    expect(html).not.toContain("<style");
  });

  it("opens external links in a new tab with noopener", () => {
    const html = renderMarkdown("[docs](https://example.invalid/page)\n");
    expect(html).toContain('target="_blank"');
    // Without noopener the opened page gets a handle on this one.
    expect(html).toContain("noopener");
    expect(html).toContain("noreferrer");
  });

  it("leaves in-document anchors as same-tab links", () => {
    const html = renderMarkdown("[section](#section)\n");
    expect(html).not.toContain('target="_blank"');
  });

  it("keeps heading ids so in-document anchors resolve", () => {
    expect(renderMarkdown("## A section\n")).toContain("id=");
  });
});

describe("heading anchors", () => {
  it("slugs a heading the way GitHub and mkdocs do", () => {
    // So an anchor written against the published docs resolves here too.
    expect(renderMarkdown("## The state of the art\n")).toContain(
      'id="the-state-of-the-art"',
    );
    expect(renderMarkdown("## Why *this* matters!\n")).toContain(
      'id="why-this-matters"',
    );
  });

  it("disambiguates duplicate headings within one document", () => {
    const html = renderMarkdown("## Notes\n\ntext\n\n## Notes\n");
    expect(html).toContain('id="notes"');
    expect(html).toContain('id="notes-1"');
  });

  it("starts counting again for the next document", () => {
    // The counter is per render. Shared across renders, the second document
    // opened in a session would get "notes-1" for its first heading.
    renderMarkdown("## Notes\n");
    expect(renderMarkdown("## Notes\n")).toContain('id="notes"');
  });

  it("slugs from the text, not the rendered markup", () => {
    // A heading containing `code` would otherwise slug the tag names too.
    expect(renderMarkdown("## Use `SELECT`\n")).toContain('id="use-select"');
  });
});
