import React, { useEffect, useRef, useState } from "react";
import { InlineNotification, SkeletonText } from "@carbon/react";
import { Launch } from "@carbon/icons-react";
import { fetchDoc, fetchDocs, type DocInfo } from "../services/docs";
import { renderMarkdown } from "../lib/markdown";
import { enhance } from "../lib/richContent";
import { isRealDocumentLoad } from "../lib/iframeLoad";
import { routes } from "../lib/routes";
import { NavLink } from "./NavLink";
import "./DocsView.css";

/**
 * Where the published API reference lives.
 *
 * Framing it is permitted from both sides, but only just: Read the Docs sends
 * neither `X-Frame-Options` nor `frame-ancestors`, so it consents -- and the
 * dashboard's own CSP has to name this exact origin under `frame-src`, because
 * `frame-src` falls back to `default-src 'self'` and would otherwise block it.
 * See `ui/middleware.py`. Never widen that to a wildcard.
 *
 * The frame cannot be restyled from here; the same-origin policy forbids
 * reaching into it. The docs site is themed to match instead, in `mkdocs.yml`.
 */
const REFERENCE_URL = "https://text2sql-eval-toolkit.readthedocs.io/en/latest/";

interface Props {
  /** Document stem from `/docs/{name}`, or null for `/docs`. */
  name: string | null;
  onNavigate: (href: string) => void;
}

export const DocsView: React.FC<Props> = ({ name, onNavigate }) => {
  const [docs, setDocs] = useState<DocInfo[]>([]);
  const [available, setAvailable] = useState(true);
  const [listError, setListError] = useState<string | null>(null);
  const [html, setHtml] = useState<string | null>(null);
  const [title, setTitle] = useState<string | null>(null);
  const [docError, setDocError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    const controller = new AbortController();
    fetchDocs(controller.signal)
      .then((list) => {
        setDocs(list.items);
        setAvailable(list.available);
      })
      .catch((e: unknown) => {
        if (controller.signal.aborted) return;
        setListError(e instanceof Error ? e.message : "Failed to list documents");
      });
    return () => controller.abort();
  }, []);

  useEffect(() => {
    if (!name) {
      setHtml(null);
      setTitle(null);
      setDocError(null);
      return;
    }
    const controller = new AbortController();
    setLoading(true);
    setDocError(null);
    fetchDoc(name, controller.signal)
      .then((doc) => {
        setTitle(doc.title);
        setHtml(renderMarkdown(doc.markdown));
      })
      .catch((e: unknown) => {
        if (controller.signal.aborted) return;
        setHtml(null);
        setTitle(null);
        setDocError(e instanceof Error ? e.message : "Failed to load document");
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
  }, [name]);

  const showingReference = !name;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
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
          project. Every document has its own address, so a link opens the exact
          one being discussed.
        </p>
      </div>

      {listError && (
        <InlineNotification
          kind="error"
          title="Could not list documents"
          subtitle={listError}
          lowContrast
          onCloseButtonClick={() => setListError(null)}
        />
      )}

      <div style={{ display: "flex", gap: "1.5rem", alignItems: "flex-start" }}>
        <DocumentList
          docs={docs}
          available={available}
          activeName={name}
          onNavigate={onNavigate}
        />
        <div style={{ flex: 1, minWidth: 0 }}>
          {showingReference ? (
            <Reference />
          ) : (
            <Document
              title={title}
              html={html}
              loading={loading}
              error={docError}
            />
          )}
        </div>
      </div>
    </div>
  );
};

const LIST_WIDTH_PX = 240;

const DocumentList: React.FC<{
  docs: DocInfo[];
  available: boolean;
  activeName: string | null;
  onNavigate: (href: string) => void;
}> = ({ docs, available, activeName, onNavigate }) => (
  <nav
    aria-label="Documents"
    style={{
      width: LIST_WIDTH_PX,
      flexShrink: 0,
      display: "flex",
      flexDirection: "column",
      gap: "0.15rem",
    }}
  >
    <NavLink
      href={routes.docs()}
      onNavigate={onNavigate}
      active={activeName === null}
    >
      API reference
    </NavLink>
    <div
      style={{
        height: "1px",
        background: "var(--cds-border-subtle)",
        margin: "0.5rem 0",
      }}
    />
    {docs.map((doc) => (
      <NavLink
        key={doc.name}
        href={routes.docs(doc.name)}
        onNavigate={onNavigate}
        active={activeName === doc.name}
      >
        {doc.title}
      </NavLink>
    ))}
    {docs.length === 0 && <EmptyState available={available} />}
  </nav>
);

/**
 * What a pip install sees.
 *
 * `docs/` is packaged in neither the wheel nor the sdist -- deliberately, so
 * the notes stay in the repository rather than shipping to PyPI. The
 * consequence is that most installs have no documents to show, and the correct
 * thing is to say why rather than render nothing. `available` distinguishes
 * "not installed" from "installed and empty"; they need different sentences.
 */
const EmptyState: React.FC<{ available: boolean }> = ({ available }) => (
  <p
    style={{
      margin: "0.25rem 0.5rem",
      fontSize: "0.8125rem",
      lineHeight: 1.45,
      color: "var(--cds-text-secondary)",
    }}
  >
    {available ? (
      <>No notes are installed yet.</>
    ) : (
      <>
        No documents are installed. This view reads them from the repository,
        and they are not part of the published package —{" "}
        <a
          href="https://github.com/IBM/text2sql-eval-toolkit/tree/main/docs/notes"
          target="_blank"
          rel="noopener noreferrer"
        >
          read them on GitHub
        </a>
        . The API reference beside this list needs no install.
      </>
    )}
  </p>
);

const Document: React.FC<{
  title: string | null;
  html: string | null;
  loading: boolean;
  error: string | null;
}> = ({ title, html, loading, error }) => {
  const article = useRef<HTMLElement | null>(null);

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

  if (error) {
    return (
      <InlineNotification
        kind="error"
        title="Could not open that document"
        subtitle={error}
        lowContrast
        hideCloseButton
      />
    );
  }
  if (loading || html === null) {
    return <SkeletonText paragraph lineCount={12} />;
  }
  return (
    <article
      ref={article}
      className="t2s-markdown"
      aria-label={title ?? "Document"}
      // Sanitised in `lib/markdown.ts`, which is the only place that renders
      // Markdown -- see that module for why the sanitising is not skipped for
      // files we wrote ourselves.
      dangerouslySetInnerHTML={{ __html: html }}
    />
  );
};

/** How long a cold load may take before the wait is worth explaining. */
const SLOW_LOAD_MS = 6000;

const Reference: React.FC = () => {
  const [loaded, setLoaded] = useState(false);
  const [slow, setSlow] = useState(false);

  // The published site sits behind a CDN, and a cold fetch of it took several
  // seconds on the deployment -- during which the frame is a blank white box
  // with nothing to say it is working. That reads as broken, which in a demo
  // is worse than reading as slow.
  useEffect(() => {
    if (loaded) return;
    const timer = setTimeout(() => setSlow(true), SLOW_LOAD_MS);
    return () => clearTimeout(timer);
  }, [loaded]);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
      <div style={{ display: "flex", justifyContent: "flex-end" }}>
        <a
          href={REFERENCE_URL}
          target="_blank"
          rel="noopener noreferrer"
          style={{
            display: "inline-flex",
            alignItems: "center",
            gap: "0.35rem",
            fontSize: "0.8125rem",
          }}
        >
          Open on Read the Docs <Launch size={16} />
        </a>
      </div>
      <div
        style={{
          position: "relative",
          height: "calc(100vh - 16rem)",
          minHeight: "32rem",
          border: "1px solid var(--cds-border-subtle)",
          background: "var(--cds-layer)",
        }}
      >
        {!loaded && (
          <div
            // In front of the frame, and opaque. An iframe paints its own
            // background -- white, for `about:blank` and for the docs site --
            // so a placeholder behind it is a placeholder nobody sees.
            style={{
              position: "absolute",
              inset: 0,
              zIndex: 1,
              background: "var(--cds-layer)",
              display: "flex",
              flexDirection: "column",
              alignItems: "center",
              justifyContent: "center",
              gap: "0.5rem",
              padding: "2rem",
              textAlign: "center",
              color: "var(--cds-text-secondary)",
              fontSize: "0.875rem",
            }}
          >
            <span>Loading the published reference…</span>
            {slow && (
              <span style={{ fontSize: "0.8125rem" }}>
                It is taking longer than usual.{" "}
                <a href={REFERENCE_URL} target="_blank" rel="noopener noreferrer">
                  Open it on Read the Docs
                </a>{" "}
                instead.
              </span>
            )}
          </div>
        )}
        <iframe
          src={REFERENCE_URL}
          title="Text-to-SQL Evaluation Toolkit API reference"
          // The frame is another origin, so nothing here can reach into it and
          // nothing in it can reach out. The sandbox is narrowed to what the
          // docs site needs to work: its own scripts for search and navigation,
          // and links that open in a new tab.
          sandbox="allow-scripts allow-popups allow-popups-to-escape-sandbox allow-forms"
          referrerPolicy="no-referrer"
          // Not `setLoaded(true)` directly: a fresh iframe fires `load` for
          // its initial `about:blank` before the real document arrives, and
          // acting on that one clears the placeholder while the frame is still
          // empty. See lib/iframeLoad.ts.
          onLoad={(event) => {
            if (isRealDocumentLoad(event.currentTarget)) setLoaded(true);
          }}
          style={{
            position: "relative",
            width: "100%",
            height: "100%",
            border: "none",
          }}
        />
      </div>
    </div>
  );
};
