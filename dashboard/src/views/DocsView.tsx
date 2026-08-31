import React, { useEffect, useRef, useState } from "react";
import { InlineNotification, SkeletonText } from "@carbon/react";
import { ArrowLeft, Launch } from "@carbon/icons-react";
import { fetchDoc, fetchDocs, type DocInfo } from "../services/docs";
import { renderMarkdown } from "../lib/markdown";
import { enhance } from "../lib/richContent";
import { routes } from "../lib/routes";
import "./DocsView.css";

/**
 * The published API reference.
 *
 * A link out rather than an embed. It was framed inside the dashboard until
 * the docs view became an index of tiles, at which point a tile that opens a
 * frame of somebody else's site -- which cannot be styled from here, and which
 * needed a `frame-src` exception in our own CSP to display at all -- was doing
 * more work than a link for the same result.
 */
const REFERENCE_URL = "https://text2sql-eval-toolkit.readthedocs.io/en/latest/";

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

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fill, minmax(270px, 1fr))",
          gap: "0.75rem",
        }}
      >
        <Tile
          eyebrow="Reference"
          title="API reference"
          summary="Every exported symbol, generated from the docstrings. Opens on Read the Docs."
          href={REFERENCE_URL}
          external
        />
        {docs === null
          ? [0, 1, 2].map((i) => <TileSkeleton key={i} />)
          : docs.map((doc) => (
              <Tile
                key={doc.name}
                eyebrow="Note"
                title={doc.title}
                summary={doc.summary}
                href={routes.docs(doc.name)}
                onNavigate={onNavigate}
              />
            ))}
      </div>

      {docs !== null && docs.length === 0 && <EmptyState available={available} />}
    </div>
  );
};

/**
 * One tile.
 *
 * An anchor, not a div with a click handler: these are links, and a link is
 * what makes "open in a new tab", middle-click and copy-address work. The
 * plain left click is intercepted for single-page navigation and every other
 * gesture is handed back to the browser -- the same bargain `NavLink` strikes.
 */
const Tile: React.FC<{
  eyebrow: string;
  title: string;
  summary: string;
  href: string;
  external?: boolean;
  onNavigate?: (href: string) => void;
}> = ({ eyebrow, title, summary, href, external = false, onNavigate }) => (
  <a
    className="t2s-doc-tile"
    href={href}
    {...(external
      ? { target: "_blank", rel: "noopener noreferrer" }
      : {
          onClick: (event: React.MouseEvent<HTMLAnchorElement>) => {
            if (
              !onNavigate ||
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
            onNavigate(href);
          },
        })}
  >
    <span className="t2s-doc-tile__eyebrow">
      {eyebrow}
      {external && <Launch size={14} aria-label="opens in a new tab" />}
    </span>
    <span className="t2s-doc-tile__title">{title}</span>
    {summary && <span className="t2s-doc-tile__summary">{summary}</span>}
  </a>
);

const TileSkeleton: React.FC = () => (
  <div className="t2s-doc-tile t2s-doc-tile--skeleton" aria-hidden>
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
            const anchor = (event.target as HTMLElement).closest("a");
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
    </div>
  );
};
