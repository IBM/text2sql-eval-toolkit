/**
 * Typesetting maths and drawing diagrams in already-rendered Markdown.
 *
 * This works on the DOM rather than on an HTML string, and it runs *after*
 * sanitisation. That ordering is deliberate: both KaTeX and Mermaid are
 * generators of markup, and passing their output back through the sanitiser as
 * text would either strip it (KaTeX's spans carry attributes the docs
 * allow-list does not) or mean trusting a string that was assembled from
 * document content.
 *
 * **Both libraries are imported dynamically, and only when a document actually
 * needs them.** Mermaid is by far the largest dependency in this project, and
 * KaTeX brings a font set. Most documents contain neither a diagram nor an
 * equation, and those documents should not pay for either. The dynamic
 * `import()` calls are also what keep both out of the docs view's own chunk --
 * Vite gives each its own, fetched on demand.
 *
 * Failures are contained. A diagram that Mermaid cannot parse is left as the
 * code block it already was, with a note; an equation KaTeX rejects is left as
 * its TeX source. Neither should take down a page of otherwise readable prose.
 */
import DOMPurify from "dompurify";

/** Marks a block that has already been handled, so a re-render is idempotent. */
const DONE = "data-rich-done";

/**
 * Typeset every `.math-inline` and `.math-block` under `root`.
 *
 * The elements and their delimiters come from the tokenizer in `markdown.ts`;
 * their text content is the raw TeX.
 */
async function typesetMaths(root: HTMLElement): Promise<void> {
  const nodes = Array.from(
    root.querySelectorAll<HTMLElement>(
      `.math-inline:not([${DONE}]), .math-block:not([${DONE}])`,
    ),
  );
  if (nodes.length === 0) return;

  const [{ default: katex }] = await Promise.all([
    import("katex"),
    // KaTeX's own stylesheet. Imported here rather than in the view so it is
    // fetched by the same documents that fetch the library.
    import("katex/dist/katex.min.css"),
  ]);

  for (const node of nodes) {
    const tex = node.textContent ?? "";
    node.setAttribute(DONE, "");
    try {
      katex.render(tex, node, {
        displayMode: node.classList.contains("math-block"),
        // `throwOnError` so a bad expression is caught here and left as source,
        // rather than KaTeX rendering its own red error text in place.
        throwOnError: true,
        // Never let document content reach KaTeX's \href, \url or \includegraphics.
        trust: false,
        strict: false,
      });
    } catch {
      // Leave the TeX visible. Unreadable is better than absent, and it says
      // plainly which expression needs attention.
      node.textContent = tex;
      node.classList.add("math-error");
    }
  }
}

/**
 * Replace every ```mermaid fenced block under `root` with a rendered diagram.
 *
 * Two layers of defence on the output. Mermaid runs at `securityLevel: 'strict'`,
 * which escapes label text and disables click bindings; the SVG it returns is
 * then sanitised as SVG. The diagrams in this repository are authored and
 * reviewed, and the same argument applies as for the Markdown around them: a
 * generator that emits markup from document text is a sink the moment the
 * document stops being trusted.
 */
async function drawDiagrams(root: HTMLElement): Promise<void> {
  const blocks = Array.from(
    root.querySelectorAll<HTMLElement>(
      `pre > code.language-mermaid:not([${DONE}])`,
    ),
  );
  if (blocks.length === 0) return;

  const { default: mermaid } = await import("mermaid");
  mermaid.initialize({
    startOnLoad: false,
    securityLevel: "strict",
    // Labels as SVG <text>, not as HTML inside a <foreignObject>.
    //
    // Mermaid's default is the latter, and sanitising its output as SVG strips
    // the HTML inside -- which drew every diagram as a set of correctly placed,
    // entirely empty boxes. The alternative fix is to admit HTML to the
    // sanitiser; this one is tighter, because it means there is no HTML in the
    // SVG to have an opinion about. Checked against all ten of the survey's
    // diagrams, including its timeline, which renders identically either way.
    htmlLabels: false,
    flowchart: { htmlLabels: false },
    // Carbon's own type, so a diagram does not read as a different product.
    fontFamily:
      '"IBM Plex Sans", system-ui, -apple-system, "Segoe UI", sans-serif',
    theme: "neutral",
  });

  for (const [index, block] of blocks.entries()) {
    block.setAttribute(DONE, "");
    const source = block.textContent ?? "";
    const pre = block.parentElement;
    if (!pre) continue;

    const figure = document.createElement("figure");
    figure.className = "t2s-diagram";
    try {
      // The id must be unique per render or Mermaid reuses a stale definition.
      const { svg } = await mermaid.render(
        `t2s-mermaid-${index}-${source.length}`,
        source,
      );
      const scroller = document.createElement("div");
      scroller.className = "t2s-diagram__scroll";
      figure.appendChild(scroller);
      scroller.innerHTML = DOMPurify.sanitize(svg, {
        // SVG only. Mermaid is configured above to keep its labels in SVG
        // <text>, so there is no HTML in here that needs admitting.
        USE_PROFILES: { svg: true, svgFilters: true },
        // Mermaid emits <style> inside the SVG for node and edge colours;
        // dropping it leaves an unstyled, illegible diagram. It is scoped to
        // the SVG's own generated ids.
        ADD_TAGS: ["style"],
      });
      sizeDiagram(figure);
      addZoomControl(figure);
      pre.replaceWith(figure);
    } catch (error) {
      // A diagram that will not parse should not cost the reader the source.
      const note = document.createElement("p");
      note.className = "t2s-diagram-error";
      note.textContent = `This diagram could not be drawn: ${
        error instanceof Error ? error.message : String(error)
      }`;
      pre.parentElement?.insertBefore(note, pre);
    }
  }
}

/**
 * How far a diagram may be scaled down to fit its column.
 *
 * Fitting to the column is what a reader wants: the whole figure visible, no
 * sideways scrolling. Fitting *unconditionally* is not, because Mermaid draws
 * some flowcharts very wide and very short -- one in the survey is 1922 by 84
 * pixels, an aspect ratio of 23:1 -- and squeezing that into a narrow column
 * once rendered it 32 pixels tall, with labels far too small to read.
 *
 * So: scale down to fit, but never past this. Below it the diagram keeps this
 * size and its figure scrolls instead. The value is measured rather than
 * guessed -- every diagram in the survey needs at most 0.53 to fit the article
 * column, so at a normal window all of them fit and nothing scrolls; the floor
 * only comes into play on a narrow one.
 */
const MIN_DIAGRAM_SCALE = 0.5;

/**
 * Fit a diagram to its column, down to a legibility floor.
 *
 * Mermaid emits `width="100%"` with an inline `max-width` equal to the natural
 * width. That attribute is what carries the size, so it is read off before
 * being replaced.
 *
 * The three properties do the work between them: `width` is the natural size,
 * so a diagram narrower than the column is never blown up; `max-width` lets a
 * wider one shrink to the column; and `min-width` stops the shrinking at the
 * floor above. `height: auto` in the stylesheet keeps the aspect ratio.
 */
function sizeDiagram(figure: HTMLElement): void {
  const svg = figure.querySelector("svg");
  if (!svg) return;
  const natural = Number.parseFloat(svg.style.maxWidth);
  svg.removeAttribute("width");
  if (!Number.isFinite(natural) || natural <= 0) {
    svg.style.maxWidth = "100%";
    return;
  }
  svg.style.width = `${Math.round(natural)}px`;
  svg.style.maxWidth = "100%";
  svg.style.minWidth = `${Math.round(natural * MIN_DIAGRAM_SCALE)}px`;
}

/** Marks the button that opens a diagram at full size. */
export const ZOOM_ATTRIBUTE = "data-diagram-zoom";

/**
 * Add a "View full size" control to a diagram.
 *
 * Fitting to the column means the widest diagrams are drawn at around half
 * size, and on a narrow window they hit the floor above and are clipped. Both
 * are reasonable defaults and neither is a way to read a dense figure, so
 * there is a way out of them.
 *
 * It sits below the diagram, outside the element that scrolls. Inside it, and
 * absolutely positioned, it scrolled away with the content on exactly the
 * diagrams that are too wide to fit -- which are the ones it exists for.
 *
 * A real `<button>` rather than a click handler on the figure: the figure
 * scrolls, and making the whole of it a click target means dragging to scroll
 * opens a dialog. It is also the difference between something reachable by
 * keyboard and something that is not.
 *
 * Added here rather than in the view because this is where the figure is
 * built; the view listens for it, since the dialog is React's.
 */
function addZoomControl(figure: HTMLElement): void {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "t2s-diagram__zoom";
  button.setAttribute(ZOOM_ATTRIBUTE, "");
  button.textContent = "View full size";
  figure.appendChild(button);
}

/**
 * Give every table its own horizontal scroller.
 *
 * A survey's comparison tables run to six columns, which is wider than the
 * prose measure the article is set to. Without a wrapper the choice is between
 * a table that overflows the page and one squeezed into a column meant for
 * sentences; with one, the table gets the width of the view and scrolls inside
 * itself if it still does not fit.
 *
 * Done here rather than in the renderer because it is presentation, and doing
 * it in the DOM keeps the sanitised HTML as the single description of the
 * document's content.
 */
function wrapTables(root: HTMLElement): void {
  for (const table of Array.from(
    root.querySelectorAll<HTMLTableElement>(`table:not([${DONE}])`),
  )) {
    table.setAttribute(DONE, "");
    if (table.parentElement?.classList.contains("table-scroll")) continue;
    const scroller = document.createElement("div");
    scroller.className = "table-scroll";
    // `tabindex` so a scrollable region is reachable by keyboard; without it a
    // wide table's right-hand columns cannot be read without a mouse.
    scroller.tabIndex = 0;
    scroller.setAttribute("role", "region");
    scroller.setAttribute("aria-label", "Table");
    table.replaceWith(scroller);
    scroller.appendChild(table);
  }
}

/**
 * Upgrade a rendered Markdown container in place.
 *
 * Args:
 *     root: The element holding the sanitised HTML.
 *     isCancelled: Consulted between the two passes so a document the reader
 *         has already navigated away from stops work instead of mutating a
 *         container that is about to be replaced.
 */
export async function enhance(
  root: HTMLElement,
  isCancelled: () => boolean = () => false,
): Promise<void> {
  // Synchronous and free -- no library to fetch -- so it happens before
  // anything is awaited and the tables are right on the first paint.
  wrapTables(root);

  // Sequential, not concurrent: both mutate the same tree, and the maths pass
  // is the cheaper of the two, so doing it first puts something readable on
  // screen while Mermaid is still being fetched.
  await typesetMaths(root);
  if (isCancelled()) return;
  await drawDiagrams(root);
}
