/**
 * Markdown to HTML, for the docs view.
 *
 * Both libraries are imported here rather than in the view so that the one
 * place doing the rendering is also the one place doing the sanitising -- a
 * second call site that reached for `marked` directly would be an XSS sink, and
 * this module exists so there is nothing to reach for.
 *
 * *Why sanitise our own files at all.* The documents are authored, versioned
 * and reviewed, so today the HTML is trusted. "It is our own file" is exactly
 * the assumption that ages badly: the moment a document is generated, pasted or
 * contributed, a renderer that emits raw HTML becomes a script-injection point,
 * and the fix would then have to be found rather than already being here. The
 * cost is one pass over a few kilobytes.
 *
 * *Why client-side.* Rendering to HTML on the server and shipping that to the
 * browser would mean trusting markup that arrived over the wire, which is a
 * worse position than trusting Markdown and rendering it here. It also keeps
 * the Python package's dependencies unchanged, which is the standing rule for
 * dashboard work. Both libraries land in this view's own lazy chunk -- the
 * entry bundle is budgeted at 460 KB in CI and has no room for them.
 */
import DOMPurify from "dompurify";
import { marked } from "marked";

/**
 * LaTeX, kept out of the reach of Markdown's escape rule.
 *
 * The survey writes inline math as `\(q\)` and display math as `\[ ... \]`.
 * CommonMark treats a backslash before punctuation as an escape, so by the time
 * the default renderer sees them the delimiters are gone and `\(q\)` has become
 * the literal text `(q)`. Tokenizing them here happens first, so the TeX
 * survives intact.
 *
 * Nothing is rendered at this stage. The TeX is emitted as the text content of
 * a marked element and typeset later, from the DOM, after sanitisation --
 * see `lib/richContent.ts`. Emitting KaTeX's HTML here would mean either
 * sanitising it away or trusting a generator's markup, and it would pull KaTeX
 * into this module for every document, including the ones with no maths in
 * them.
 */
const escapeHtml = (text: string): string =>
  text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");

marked.use({
  extensions: [
    {
      name: "mathBlock",
      level: "block",
      start: (src: string) => src.indexOf("\\["),
      tokenizer(src: string) {
        const match = /^\\\[([\s\S]+?)\\\]\s*/.exec(src);
        if (!match) return undefined;
        return {
          type: "mathBlock",
          raw: match[0],
          text: match[1].trim(),
        };
      },
      // marked types a custom token as its own `Generic`, which carries no
      // `text`; the tokenizer above is what puts one there.
      renderer: (token) =>
        `<div class="math-block">${escapeHtml(String(token.text ?? ""))}</div>\n`,
    },
    {
      name: "mathInline",
      level: "inline",
      start: (src: string) => src.indexOf("\\("),
      tokenizer(src: string) {
        const match = /^\\\(([\s\S]+?)\\\)/.exec(src);
        if (!match) return undefined;
        return {
          type: "mathInline",
          raw: match[0],
          text: match[1].trim(),
        };
      },
      renderer: (token) =>
        `<span class="math-inline">${escapeHtml(String(token.text ?? ""))}</span>`,
    },
  ],
});

marked.setOptions({
  // GitHub-flavoured line breaks are off: these documents are prose with hard
  // wraps at 80 columns, and treating each wrap as a <br> would shred every
  // paragraph.
  breaks: false,
  gfm: true,
});

/**
 * `#section` anchors within a document.
 *
 * marked stopped emitting heading ids in v5, so without this every in-document
 * link is dead and there is no way to send someone a section rather than a
 * page. The slug is the text lowercased with runs of non-word characters
 * collapsed to a dash -- the same shape GitHub and mkdocs produce, so an anchor
 * written for one of those renders here too.
 *
 * Duplicate headings get a numeric suffix, per document, which is why the seen
 * counter is reset on every render rather than living at module scope.
 */
let seenSlugs = new Map<string, number>();

const slugify = (text: string): string => {
  const base =
    text
      .toLowerCase()
      .replace(/[^\w\s-]/g, "")
      .trim()
      .replace(/[\s_-]+/g, "-") || "section";
  const count = seenSlugs.get(base) ?? 0;
  seenSlugs.set(base, count + 1);
  return count === 0 ? base : `${base}-${count}`;
};

marked.use({
  renderer: {
    heading(token) {
      const text = this.parser.parseInline(token.tokens);
      // The slug comes from the raw text, not the rendered HTML: a heading
      // containing `code` would otherwise slug the tag names too.
      const id = slugify(token.text);
      return `<h${token.depth} id="${id}">${text}</h${token.depth}>\n`;
    },
  },
});

/**
 * Render Markdown to sanitised HTML.
 *
 * `target="_blank"` is added to external links by the hook below rather than by
 * post-processing the string: rewriting attributes with a regular expression
 * after sanitisation would reintroduce exactly what sanitisation removed.
 */
export function renderMarkdown(source: string): string {
  seenSlugs = new Map();
  const html = marked.parse(source, { async: false }) as string;
  return DOMPurify.sanitize(html, {
    // Anything not on this list is dropped. `id` stays so in-document anchors
    // work; `href`, `src`, `alt` and `title` are what the content needs.
    //
    // `class` is here because two things downstream are found by it: a fenced
    // block's `language-*`, which is how a Mermaid diagram is told from any
    // other code block, and the `math-*` markers above. It carries no ability
    // to execute; the worst a document can do with it is apply a style that
    // already exists on the page.
    ALLOWED_ATTR: [
      "href",
      "src",
      "alt",
      "title",
      "id",
      "class",
      "align",
      "colspan",
    ],
    // No <iframe>, <object>, <form> or <style>: nothing in an authored note
    // needs them, and each is a way for a future document to do more than
    // display text.
    FORBID_TAGS: ["iframe", "object", "embed", "form", "input", "style"],
    FORBID_ATTR: ["srcset", "formaction"],
  });
}

/**
 * Open links to other origins in a new tab, safely.
 *
 * Registered once, at module load, so it applies to every `renderMarkdown`
 * call. `rel="noopener noreferrer"` is not optional here: without `noopener`
 * the opened page gets a handle on this one through `window.opener`.
 */
DOMPurify.addHook("afterSanitizeAttributes", (node) => {
  if (!(node instanceof HTMLAnchorElement)) return;
  const href = node.getAttribute("href") || "";
  if (/^https?:\/\//i.test(href)) {
    node.setAttribute("target", "_blank");
    node.setAttribute("rel", "noopener noreferrer");
  }
});
