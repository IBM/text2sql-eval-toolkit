import React, { useEffect, useMemo, useRef, useState } from "react";
import { ComboBox, InlineNotification, Tag } from "@carbon/react";
import { apiFetch, apiUrl } from "../lib/api";

export type SqlParseMode = "sqlglot" | "sqlglot_optimized";

export interface VisualTreeNode {
  label: string;
  meta?: string;
  edge?: string;
  detail?: string;
  children: VisualTreeNode[];
}

export interface SqlParseAnalysis {
  features?: Record<string, number>;
  categories?: string[];
}

export interface SqlParseResult {
  ok: boolean;
  error?: string | null;
  dialect?: string | null;
  parse_mode?: SqlParseMode | null;
  visual_tree?: VisualTreeNode | null;
  formatted_sql?: string | null;
  analysis?: SqlParseAnalysis | null;
  analysis_error?: string | null;
}

const PARSE_MODE_ITEMS: { id: SqlParseMode; label: string; description: string }[] = [
  {
    id: "sqlglot",
    label: "SQLGlot",
    description: "Raw sqlglot parse (structure as written).",
  },
  {
    id: "sqlglot_optimized",
    label: "SQLGlot optimized",
    description: "AST after sqlglot.optimizer.optimize (same as sqlglot_optimized_equivalence).",
  },
];

const TREE_PANEL_STYLE: React.CSSProperties = {
  border: "1px solid var(--cds-border-subtle-01)",
  borderRadius: 4,
  padding: "1rem",
  backgroundColor: "var(--cds-layer-01)",
  display: "flex",
  flexDirection: "column",
  gap: "0.75rem",
};

const TREE_CANVAS_STYLE: React.CSSProperties = {
  maxHeight: "26rem",
  overflow: "auto",
  padding: "0.75rem 1rem",
  borderRadius: 4,
  border: "1px solid var(--cds-border-subtle-01)",
  backgroundColor: "var(--cds-layer-02)",
};

/** Connector-line tree (nested list + CSS). */
const TREE_STYLES = `
.sql-visual-tree,
.sql-visual-tree ul {
  list-style: none;
  margin: 0;
  padding: 0;
}
.sql-visual-tree ul {
  margin-left: 1.1rem;
  padding-left: 0.85rem;
  border-left: 1px solid var(--cds-border-subtle-02, rgba(141, 141, 141, 0.4));
}
.sql-visual-tree > li {
  margin: 0;
}
.sql-visual-tree li {
  position: relative;
  margin: 0;
  padding: 0.2rem 0 0.2rem 0;
}
.sql-visual-tree li::before {
  content: "";
  position: absolute;
  left: -0.85rem;
  top: 0.95rem;
  width: 0.85rem;
  height: 0;
  border-top: 1px solid var(--cds-border-subtle-02, rgba(141, 141, 141, 0.4));
}
.sql-visual-tree > li::before {
  display: none;
}
.sql-visual-tree-row {
  display: inline-flex;
  flex-wrap: wrap;
  align-items: baseline;
  gap: 0.35rem 0.5rem;
  padding: 0.15rem 0.4rem;
  border-radius: 3px;
  cursor: default;
  max-width: 100%;
}
.sql-visual-tree-row.is-clickable {
  cursor: pointer;
}
.sql-visual-tree-row.is-clickable:hover {
  background: var(--cds-layer-hover-01, rgba(141, 141, 141, 0.12));
}
.sql-visual-tree-toggle {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 1rem;
  height: 1rem;
  flex-shrink: 0;
  font-size: 0.7rem;
  line-height: 1;
  color: var(--cds-icon-secondary);
  border: 1px solid var(--cds-border-subtle-01);
  border-radius: 2px;
  background: var(--cds-layer-01);
  user-select: none;
}
.sql-visual-tree-leaf-dot {
  display: inline-block;
  width: 0.35rem;
  height: 0.35rem;
  border-radius: 50%;
  background: var(--cds-support-info);
  flex-shrink: 0;
}
.sql-visual-tree-edge {
  font-size: 0.7rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.03em;
  color: var(--cds-text-helper);
  padding: 0.05rem 0.35rem;
  border-radius: 2px;
  background: var(--cds-layer-accent-01, rgba(15, 98, 254, 0.08));
}
.sql-visual-tree-type {
  font-family: var(--cds-code-01-font-family, monospace);
  font-size: 0.8125rem;
  font-weight: 600;
  color: var(--cds-support-info);
}
.sql-visual-tree-meta {
  font-size: 0.75rem;
  color: var(--cds-text-secondary);
}
.sql-visual-tree-detail {
  font-family: var(--cds-code-01-font-family, monospace);
  font-size: 0.75rem;
  color: var(--cds-text-primary);
  word-break: break-word;
}
`;

function VisualTreeBranch({
  node,
  depth,
  expanded,
}: {
  node: VisualTreeNode;
  depth: number;
  expanded: boolean;
}) {
  const hasChildren = (node.children?.length ?? 0) > 0;
  const [open, setOpen] = useState(expanded);
  const prevExpanded = useRef(expanded);

  useEffect(() => {
    if (prevExpanded.current !== expanded) {
      setOpen(expanded);
      prevExpanded.current = expanded;
    }
  }, [expanded]);

  return (
    <li>
      <div
        className={`sql-visual-tree-row${hasChildren ? " is-clickable" : ""}`}
        role={hasChildren ? "button" : undefined}
        tabIndex={hasChildren ? 0 : undefined}
        onClick={hasChildren ? () => setOpen((v) => !v) : undefined}
        onKeyDown={
          hasChildren
            ? (e) => {
                if (e.key === "Enter" || e.key === " ") {
                  e.preventDefault();
                  setOpen((v) => !v);
                }
              }
            : undefined
        }
        aria-expanded={hasChildren ? open : undefined}
      >
        {hasChildren ? (
          <span className="sql-visual-tree-toggle" aria-hidden>
            {open ? "−" : "+"}
          </span>
        ) : (
          <span className="sql-visual-tree-leaf-dot" aria-hidden />
        )}
        {node.edge ? <span className="sql-visual-tree-edge">{node.edge}</span> : null}
        <span className="sql-visual-tree-type">{node.label}</span>
        {node.meta ? <span className="sql-visual-tree-meta">({node.meta})</span> : null}
        {node.detail ? <code className="sql-visual-tree-detail">{node.detail}</code> : null}
      </div>
      {hasChildren && open ? (
        <ul>
          {node.children.map((child, i) => (
            <VisualTreeBranch
              key={`${depth}-${node.edge ?? node.label}-${i}`}
              node={child}
              depth={depth + 1}
              expanded={expanded}
            />
          ))}
        </ul>
      ) : null}
    </li>
  );
}

function VisualSqlTree({
  root,
  expanded,
}: {
  root: VisualTreeNode;
  expanded: boolean;
}) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
      <style>{TREE_STYLES}</style>
      <div style={TREE_CANVAS_STYLE}>
        <ul className="sql-visual-tree">
          <VisualTreeBranch node={root} depth={0} expanded={expanded} />
        </ul>
      </div>
    </div>
  );
}

function AnalysisTags({ analysis }: { analysis: SqlParseAnalysis | null | undefined }) {
  const categories = analysis?.categories ?? [];
  const features = analysis?.features;
  if (!categories.length && !features) return null;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
      {categories.length > 0 ? (
        <div style={{ display: "flex", flexWrap: "wrap", gap: "0.35rem", alignItems: "center" }}>
          <span style={{ fontSize: "0.8125rem", fontWeight: 600, marginRight: "0.25rem" }}>Categories</span>
          {categories.map((c) => (
            <Tag key={c} type="blue" size="sm">
              {c}
            </Tag>
          ))}
        </div>
      ) : null}
      {features && Object.keys(features).length > 0 ? (
        <p style={{ margin: 0, fontSize: "0.8125rem", color: "var(--cds-text-secondary)" }}>
          <strong style={{ color: "var(--cds-text-primary)" }}>Structure:</strong>{" "}
          {Object.entries(features)
            .map(([k, v]) => `${k.replace(/^query_/, "")}=${v}`)
            .join(", ")}
        </p>
      ) : null}
    </div>
  );
}

async function fetchSqlParse(
  sql: string,
  dialect: string,
  parseMode: SqlParseMode
): Promise<SqlParseResult> {
  const res = await apiFetch(apiUrl("/api/sql/parse"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sql, dialect, parse_mode: parseMode }),
  });
  return (await res.json()) as SqlParseResult;
}

function useDebouncedSqlParse(
  sql: string,
  dialect: string,
  parseMode: SqlParseMode,
  debounceMs = 450
) {
  const [result, setResult] = useState<SqlParseResult | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    const trimmed = sql.trim();
    if (!trimmed) {
      setResult(null);
      setLoading(false);
      return;
    }

    let cancelled = false;
    setLoading(true);
    const timer = window.setTimeout(() => {
      void fetchSqlParse(trimmed, dialect, parseMode)
        .then((data) => {
          if (!cancelled) setResult(data);
        })
        .catch((err: unknown) => {
          if (!cancelled) {
            setResult({
              ok: false,
              error: err instanceof Error ? err.message : String(err),
            });
          }
        })
        .finally(() => {
          if (!cancelled) setLoading(false);
        });
    }, debounceMs);

    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
  }, [sql, dialect, parseMode, debounceMs]);

  return { result, loading };
}

const COLUMN_SHELL_STYLE: React.CSSProperties = {
  display: "flex",
  flexDirection: "column",
  gap: "0.5rem",
  minWidth: 0,
  padding: "0.75rem",
  borderRadius: 4,
  border: "1px solid var(--cds-border-subtle-01)",
  backgroundColor: "var(--cds-layer-02)",
};

function SqlAstSinglePanel({
  title,
  sql,
  dialect,
  parseMode,
  treeExpanded,
  compact,
}: {
  title: string;
  sql: string;
  dialect: string;
  parseMode: SqlParseMode;
  treeExpanded: boolean;
  compact?: boolean;
}) {
  const { result, loading } = useDebouncedSqlParse(sql, dialect, parseMode);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem", minWidth: 0 }}>
      {title ? <p style={{ margin: 0, fontSize: "0.875rem", fontWeight: 600 }}>{title}</p> : null}
      {!sql.trim() ? (
        <p style={{ margin: 0, fontSize: "0.8125rem", color: "var(--cds-text-secondary)" }}>
          Enter SQL above to see the parse tree.
        </p>
      ) : loading && !result ? (
        <p style={{ margin: 0, fontSize: "0.8125rem", color: "var(--cds-text-secondary)" }}>
          Parsing…
        </p>
      ) : result && !result.ok ? (
        <InlineNotification kind="error" title="Parse error" subtitle={result.error ?? "Unknown"} lowContrast />
      ) : result?.visual_tree ? (
        <>
          {loading ? (
            <p style={{ margin: 0, fontSize: "0.75rem", color: "var(--cds-text-secondary)" }}>Updating…</p>
          ) : null}
          {!compact ? <AnalysisTags analysis={result.analysis ?? undefined} /> : null}
          <VisualSqlTree root={result.visual_tree} expanded={treeExpanded} />
          {result.formatted_sql ? (
            <details>
              <summary style={{ fontSize: "0.8125rem", cursor: "pointer" }}>Formatted SQL</summary>
              <pre
                style={{
                  margin: "0.5rem 0 0",
                  padding: "0.75rem",
                  borderRadius: 4,
                  border: "1px solid var(--cds-border-subtle-01)",
                  backgroundColor: "var(--cds-layer-01)",
                  fontSize: "0.8125rem",
                  overflow: "auto",
                  maxHeight: compact ? "8rem" : "12rem",
                  whiteSpace: "pre-wrap",
                  wordBreak: "break-word",
                  fontFamily: "var(--cds-code-01-font-family, monospace)",
                }}
              >
                {result.formatted_sql}
              </pre>
            </details>
          ) : null}
        </>
      ) : null}
    </div>
  );
}

export interface SqlAstTreePanelProps {
  dialect: string;
  groundTruthSqls: string[];
  predictedSql: string;
}

export const SqlAstTreePanel: React.FC<SqlAstTreePanelProps> = ({
  dialect,
  groundTruthSqls,
  predictedSql,
}) => {
  const [parseMode, setParseMode] = useState<SqlParseMode>("sqlglot");
  const [treeExpanded, setTreeExpanded] = useState(true);

  const selectedModeItem = useMemo(
    () => PARSE_MODE_ITEMS.find((m) => m.id === parseMode) ?? PARSE_MODE_ITEMS[0],
    [parseMode]
  );

  const gtPanels = useMemo(() => {
    const entries = groundTruthSqls.map((sql, idx) => ({
      id: `gt-${idx}`,
      title: groundTruthSqls.length > 1 ? `Ground truth ${idx + 1}` : "Ground truth",
      sql,
    }));
    if (entries.length === 0) {
      return [{ id: "gt-0", title: "Ground truth", sql: "" }];
    }
    if (entries.length === 1) return entries;
    return entries.filter((e) => e.sql.trim());
  }, [groundTruthSqls]);

  const [selectedGtId, setSelectedGtId] = useState(gtPanels[0]?.id ?? "gt-0");

  useEffect(() => {
    if (!gtPanels.some((p) => p.id === selectedGtId)) {
      setSelectedGtId(gtPanels[0]?.id ?? "gt-0");
    }
  }, [gtPanels, selectedGtId]);

  const selectedGt = gtPanels.find((p) => p.id === selectedGtId) ?? gtPanels[0];
  const showGtPicker = gtPanels.length > 1;

  return (
    <div style={TREE_PANEL_STYLE}>
      <div>
        <h4 style={{ margin: 0, fontSize: "1rem", fontWeight: 600 }}>SQL parse tree</h4>
        <p
          style={{
            margin: "0.35rem 0 0",
            fontSize: "0.8125rem",
            lineHeight: 1.45,
            color: "var(--cds-text-secondary)",
            maxWidth: "52rem",
          }}
        >
          Ground truth and predicted ASTs side by side. Choose raw SQLGlot parse or the optimized AST used by{" "}
          <code>sqlglot_optimized_equivalence</code>.
        </p>
      </div>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
          gap: "0.75rem",
          alignItems: "end",
        }}
      >
        <ComboBox
          id="sql-parse-mode"
          titleText="Parse mode"
          items={PARSE_MODE_ITEMS}
          itemToString={(item) => (item ? item.label : "")}
          selectedItem={selectedModeItem}
          onChange={(e) => {
            const item = e.selectedItem as (typeof PARSE_MODE_ITEMS)[number] | null;
            if (item) setParseMode(item.id);
          }}
        />
        <p style={{ margin: 0, fontSize: "0.8125rem", color: "var(--cds-text-secondary)", lineHeight: 1.45 }}>
          {selectedModeItem.description}
        </p>
      </div>

      <p style={{ margin: 0, fontSize: "0.8125rem", color: "var(--cds-text-secondary)" }}>
        Dialect: <strong style={{ color: "var(--cds-text-primary)" }}>{dialect}</strong>
        {" · "}
        Mode: <strong style={{ color: "var(--cds-text-primary)" }}>{selectedModeItem.label}</strong>
      </p>

      <div style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap", justifyContent: "flex-end" }}>
        <button
          type="button"
          onClick={() => setTreeExpanded(true)}
          style={{
            padding: "0.35rem 0.75rem",
            fontSize: "0.8125rem",
            borderRadius: 4,
            border: "1px solid var(--cds-border-subtle-01)",
            background: treeExpanded ? "var(--cds-layer-selected-01)" : "var(--cds-layer-02)",
            cursor: "pointer",
          }}
        >
          Expand all
        </button>
        <button
          type="button"
          onClick={() => setTreeExpanded(false)}
          style={{
            padding: "0.35rem 0.75rem",
            fontSize: "0.8125rem",
            borderRadius: 4,
            border: "1px solid var(--cds-border-subtle-01)",
            background: !treeExpanded ? "var(--cds-layer-selected-01)" : "var(--cds-layer-02)",
            cursor: "pointer",
          }}
        >
          Collapse all
        </button>
      </div>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(min(100%, 22rem), 1fr))",
          gap: "1rem",
          alignItems: "start",
        }}
      >
        <div style={COLUMN_SHELL_STYLE}>
          <p style={{ margin: 0, fontSize: "0.9375rem", fontWeight: 600 }}>Ground truth</p>
          {showGtPicker ? (
            <ComboBox
              id="sql-gt-variant"
              titleText="Ground truth variant"
              items={gtPanels}
              itemToString={(item) => (item ? item.title : "")}
              selectedItem={selectedGt ?? null}
              onChange={(e) => {
                const item = e.selectedItem as (typeof gtPanels)[number] | null;
                if (item) setSelectedGtId(item.id);
              }}
            />
          ) : null}
          {selectedGt ? (
            <SqlAstSinglePanel
              title={showGtPicker ? "" : selectedGt.title}
              sql={selectedGt.sql}
              dialect={dialect}
              parseMode={parseMode}
              treeExpanded={treeExpanded}
            />
          ) : null}
        </div>

        <div style={COLUMN_SHELL_STYLE}>
          <p style={{ margin: 0, fontSize: "0.9375rem", fontWeight: 600 }}>Predicted</p>
          <SqlAstSinglePanel
            title=""
            sql={predictedSql}
            dialect={dialect}
            parseMode={parseMode}
            treeExpanded={treeExpanded}
          />
        </div>
      </div>
    </div>
  );
};
