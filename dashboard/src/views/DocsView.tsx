import React, { useEffect, useRef, useState } from "react";
import { InlineNotification, SkeletonText } from "@carbon/react";
import { ArrowLeft } from "@carbon/icons-react";
import {
  REFERENCE_URL,
  fetchDoc,
  fetchDocs,
  type DocInfo,
} from "../services/docs";
import { renderMarkdown } from "../lib/markdown";
import { ZOOM_ATTRIBUTE, enhance } from "../lib/richContent";
import { routes } from "../lib/routes";
import { LinkTile, TileGrid } from "./LinkTile";
import "./DocsView.css";


interface Props {
  /** Document stem from `/docs/{name}`, or null for the index. */
  name: string | null;
  onNavigate: (href: string) => void;
}

export const DocsView: React.FC<Props> = ({ name, onNavigate }) =>
  name ? (
    <DocumentPage name={name} onNavigate={onNavigate} />
  ) : (
    <DocsIndex onNavigate={onNavigate} />
  );

// --- the index -------------------------------------------------------------

const DocsIndex: React.FC<{ onNavigate: (href: string) => void }> = ({
  onNavigate,
}) => {
  const [docs, setDocs] = useState<DocInfo[] | null>(null);
  const [available, setAvailable] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    fetchDocs(controller.signal)
      .then((list) => {
        setDocs(list.items);
        setAvailable(list.available);
      })
      .catch((e: unknown) => {
        if (controller.signal.aborted) return;
        setDocs([]);
        setError(e instanceof Error ? e.message : "Failed to list documents");
      });
    return () => controller.abort();
  }, []);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "1rem" }}>
      <div>
        <h3 style={{ margin: 0 }}>Documentation</h3>
        <p
          style={{
            margin: "0.35rem 0 0",
            maxWidth: "52rem",
            lineHeight: 1.45,
            color: "var(--cds-text-secondary)",
          }}
        >
          The published API reference, plus long-form notes written for this
          project. Every note has its own address, so a link opens the exact one
          being discussed.
        </p>
      </div>

      {error && (
        <InlineNotification
          kind="error"
          title="Could not list documents"
          subtitle={error}
          lowContrast
          onCloseButtonClick={() => setError(null)}
        />
      )}

      <TileGrid>
        <LinkTile
          eyebrow="Reference"
          title="API reference"
          summary="Every exported symbol, generated from the docstrings. Opens on Read the Docs."
          href={REFERENCE_URL}
          external
        />
        {docs === null
          ? [0, 1, 2].map((i) => <TileSkeleton key={i} />)
          : docs.map((doc) => (
              <LinkTile
                key={doc.name}
                eyebrow="Note"
                title={doc.title}
                summary={doc.summary}
                href={routes.docs(doc.name)}
                onNavigate={onNavigate}
              />
            ))}
      </TileGrid>

      {docs !== null && docs.length === 0 && <EmptyState available={available} />}
    </div>
  );
};

const TileSkeleton: React.FC = () => (
  <div className="t2s-tile t2s-tile--inert" aria-hidden>
    <SkeletonText paragraph lineCount={3} />
  </div>
);

/**
 * What a pip install sees.
 *
 * `docs/` is packaged in neither the wheel nor the sdist -- deliberately, so
 * the notes stay in the repository rather than shipping to PyPI. The
 * consequence is that most installs have no notes to show, and the correct
 * thing is to say why rather than render an index with one tile on it and no
 * explanation. `available` distinguishes "not installed" from "installed and
 * empty"; they need different sentences.
 */
const EmptyState: React.FC<{ available: boolean }> = ({ available }) => (
  <p
    style={{
      margin: 0,
      maxWidth: "42rem",
      lineHeight: 1.5,
      color: "var(--cds-text-secondary)",
    }}
  >
    {available ? (
      <>No notes are installed yet.</>
    ) : (
      <>
        No notes are installed. This view reads them from the repository, and
        they are not part of the published package —{" "}
        <a
          href="https://github.com/IBM/text2sql-eval-toolkit/tree/main/docs/notes"
          target="_blank"
          rel="noopener noreferrer"
        >
          read them on GitHub
        </a>
        . The API reference above needs no install.
      </>
    )}
  </p>
);

// --- one document ----------------------------------------------------------

const DocumentPage: React.FC<{
  name: string;
  onNavigate: (href: string) => void;
}> = ({ name, onNavigate }) => {
  const [html, setHtml] = useState<string | null>(null);
  const [title, setTitle] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [zoomed, setZoomed] = useState<SVGElement | null>(null);
  const article = useRef<HTMLElement | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    setHtml(null);
    setError(null);
    fetchDoc(name, controller.signal)
      .then((doc) => {
        setTitle(doc.title);
        setHtml(renderMarkdown(doc.markdown));
      })
      .catch((e: unknown) => {
        if (controller.signal.aborted) return;
        setError(e instanceof Error ? e.message : "Failed to load document");
      });
    return () => controller.abort();
  }, [name]);

  // Maths and diagrams are drawn from the DOM after the sanitised HTML is in
  // place -- see lib/richContent.ts for why that is the right order, and why
  // neither library is fetched for a document that has no use for it.
  useEffect(() => {
    if (!article.current || html === null) return;
    let cancelled = false;
    void enhance(article.current, () => cancelled).catch(() => {
      // The prose is already on screen. A failure here costs the diagrams and
      // the equations, and should not blank the page that carries them.
    });
    return () => {
      cancelled = true;
    };
  }, [html]);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "1rem" }}>
      <a
        className="t2s-doc-back"
        href={routes.docs()}
        onClick={(event) => {
          if (
            event.defaultPrevented ||
            event.button !== 0 ||
            event.metaKey ||
            event.ctrlKey ||
            event.shiftKey ||
            event.altKey
          ) {
            return;
          }
          event.preventDefault();
          onNavigate(routes.docs());
        }}
      >
        <ArrowLeft size={16} /> All documentation
      </a>

      {error ? (
        <InlineNotification
          kind="error"
          title="Could not open that document"
          subtitle={error}
          lowContrast
          hideCloseButton
        />
      ) : html === null ? (
        <SkeletonText paragraph lineCount={14} />
      ) : (
        <article
          ref={article}
          className="t2s-markdown"
          aria-label={title ?? "Document"}
          // A note links into the rest of the dashboard -- the tour is mostly
          // such links. Without this they are full page loads: correct, but a
          // white flash and a re-fetch of the bundle for a route the app
          // already has. Delegated from the article rather than bound per
          // anchor, because the anchors arrive as sanitised HTML and there is
          // nothing to attach a handler to.
          onClick={(event) => {
            const target = event.target as HTMLElement;

            const zoom = target.closest(`[${ZOOM_ATTRIBUTE}]`);
            if (zoom) {
              // The live node, not its markup: it was sanitised on the way in
              // and cloning it keeps it that way, with no HTML round trip to
              // reason about.
              const svg = zoom.parentElement?.querySelector("svg") ?? null;
              if (svg) setZoomed(svg);
              return;
            }

            const anchor = target.closest("a");
            if (!anchor || !article.current?.contains(anchor)) return;
            // Leave anything the browser handles better: modified clicks, the
            // middle button, and any link opening in a new tab or another
            // origin.
            if (
              event.defaultPrevented ||
              event.button !== 0 ||
              event.metaKey ||
              event.ctrlKey ||
              event.shiftKey ||
              event.altKey ||
              anchor.target === "_blank"
            ) {
              return;
            }
            const href = anchor.getAttribute("href") || "";
            // Root-relative only. `#anchor` must keep its default behaviour or
            // in-document navigation stops working, and an absolute URL is
            // another site.
            if (!href.startsWith("/")) return;
            event.preventDefault();
            onNavigate(href);
          }}
          // Sanitised in `lib/markdown.ts`, which is the only place that renders
          // Markdown -- see that module for why the sanitising is not skipped
          // for files we wrote ourselves.
          dangerouslySetInnerHTML={{ __html: html }}
        />
      )}

      {zoomed && <DiagramDialog svg={zoomed} onClose={() => setZoomed(null)} />}
    </div>
  );
};

/**
 * One diagram, at its natural size, over the page.
 *
 * The point is room: in the article a wide diagram is drawn at around half
 * size to fit the column, and here it is drawn at the size Mermaid laid it out
 * at and scrolls if that is still larger than the window.
 */
const DiagramDialog: React.FC<{ svg: SVGElement; onClose: () => void }> = ({
  svg,
  onClose,
}) => {
  const closeButton = useRef<HTMLButtonElement | null>(null);

  // Escape closes, and the page behind does not scroll while this is open --
  // scrolling the thing you cannot see is disorienting.
  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        onClose();
        return;
      }
      // Tab is held inside the dialog. Close is its only focusable control, so
      // there is nowhere else to send focus -- and without this, Tab walks into
      // the page behind an `aria-modal` dialog, which is exactly what
      // `aria-modal` promises it will not do.
      if (event.key === "Tab") {
        event.preventDefault();
        closeButton.current?.focus();
      }
    };
    document.addEventListener("keydown", onKeyDown);
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    // Focus moves into the dialog, and back out again on close, so a keyboard
    // user is not left where the page used to be.
    const previouslyFocused = document.activeElement as HTMLElement | null;
    closeButton.current?.focus();
    return () => {
      document.removeEventListener("keydown", onKeyDown);
      document.body.style.overflow = previousOverflow;
      previouslyFocused?.focus();
    };
  }, [onClose]);

  const canvas = useRef<HTMLDivElement | null>(null);

  // An effect rather than a ref callback: StrictMode attaches refs twice in
  // development, and this way the clone is replaced when the diagram changes
  // instead of being appended beside the last one.
  useEffect(() => {
    const node = canvas.current;
    if (!node) return;
    const clone = svg.cloneNode(true) as SVGElement;
    // Undo the fitting the article applies; here there is room.
    clone.removeAttribute("style");
    clone.style.width = svg.style.width || "auto";
    clone.style.maxWidth = "none";
    clone.style.height = "auto";
    node.replaceChildren(clone);
    return () => node.replaceChildren();
  }, [svg]);

  return (
    <div
      className="t2s-diagram-dialog"
      role="dialog"
      aria-modal="true"
      aria-label="Diagram, full size"
      // Anything but the diagram closes. Comparing target to currentTarget
      // would have been tidier and did not work: the bar and the canvas fill
      // the dialog, so the dialog element itself is never the click target and
      // there is no backdrop left to hit. Excluding the diagram means dragging
      // to scroll a wide one cannot dismiss it by accident.
      onClick={(event) => {
        const target = event.target as Element;
        if (target.closest("svg") || target.closest("button")) return;
        onClose();
      }}
    >
      <div className="t2s-diagram-dialog__bar">
        <button
          type="button"
          ref={closeButton}
          className="t2s-diagram-dialog__close"
          onClick={onClose}
        >
          Close
        </button>
      </div>
      <div className="t2s-diagram-dialog__canvas" ref={canvas} />
    </div>
  );
};
