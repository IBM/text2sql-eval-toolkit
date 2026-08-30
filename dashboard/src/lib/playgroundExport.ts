/**
 * Turning one playground record into something you can paste elsewhere.
 *
 * The playground is where a disagreement about a score gets settled, and those
 * arguments happen in issues, reviews and papers rather than in the tool. So the
 * export carries the things an argument needs -- the question, both statements,
 * both result sets, and every metric with its value -- rather than a screenshot
 * of them.
 *
 * There is no PDF writer here on purpose. A real one is a ~300 KB dependency,
 * against an entry-bundle budget of 460 KB, to reproduce something every browser
 * already does well. "PDF" opens the HTML export in a print dialog, where Save
 * as PDF is one click and the page breaks are the browser's problem.
 */

export interface ExportableRecord {
  benchmarkId: string;
  recordId: string;
  question: string;
  dbId: string;
  groundTruthSqls: string[];
  predictedSql: string;
  pipeline: string | null;
  url: string;
  groundTruthTables: TableData[];
  predictedTable: TableData | null;
  predictionError: string | null;
  metrics: MetricRow[];
}

export interface TableData {
  columns: string[];
  rows: unknown[][];
}

export interface MetricRow {
  name: string;
  value: string;
  group: string;
  description: string;
}

/** Rows beyond this are summarised rather than listed. */
const MAX_EXPORTED_ROWS = 50;

function cell(value: unknown): string {
  if (value === null || value === undefined) return "";
  return String(value);
}

function escapeHtml(text: string): string {
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

/** Pipe characters would otherwise split a Markdown cell in two. */
function escapeMarkdownCell(text: string): string {
  return text.replace(/\|/g, "\\|").replace(/\n/g, " ");
}

function markdownTable(table: TableData): string {
  if (table.columns.length === 0) return "_empty result_";
  const shown = table.rows.slice(0, MAX_EXPORTED_ROWS);
  const head = `| ${table.columns.map(escapeMarkdownCell).join(" | ")} |`;
  const rule = `| ${table.columns.map(() => "---").join(" | ")} |`;
  const body = shown
    .map(
      (row) => `| ${row.map((v) => escapeMarkdownCell(cell(v))).join(" | ")} |`,
    )
    .join("\n");
  const omitted =
    table.rows.length > shown.length
      ? `\n\n_${table.rows.length - shown.length} further row(s) not shown._`
      : "";
  return `${head}\n${rule}\n${body}${omitted}`;
}

function htmlTable(table: TableData): string {
  if (table.columns.length === 0) return "<p><em>empty result</em></p>";
  const shown = table.rows.slice(0, MAX_EXPORTED_ROWS);
  const head = table.columns.map((c) => `<th>${escapeHtml(c)}</th>`).join("");
  const body = shown
    .map(
      (row) =>
        `<tr>${row.map((v) => `<td>${escapeHtml(cell(v))}</td>`).join("")}</tr>`,
    )
    .join("\n");
  const omitted =
    table.rows.length > shown.length
      ? `<p class="note">${table.rows.length - shown.length} further row(s) not shown.</p>`
      : "";
  return `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>${omitted}`;
}

/** A filename that says what it holds and sorts sensibly. */
export function exportFilename(
  record: ExportableRecord,
  extension: string,
): string {
  const safe = (s: string) =>
    s.replace(/[^A-Za-z0-9._-]+/g, "-").replace(/^-|-$/g, "");
  return `${safe(record.benchmarkId)}-${safe(record.recordId)}.${extension}`;
}

export function toMarkdown(record: ExportableRecord): string {
  const parts: string[] = [];
  parts.push(`# ${record.benchmarkId} — record ${record.recordId}`);
  parts.push("");
  if (record.question) parts.push(`**Question.** ${record.question}`);
  if (record.dbId) parts.push(`**Database.** \`${record.dbId}\``);
  if (record.pipeline) parts.push(`**Pipeline.** \`${record.pipeline}\``);
  parts.push(`**Link.** ${record.url}`);
  parts.push("");

  record.groundTruthSqls
    .filter((sql) => sql.trim())
    .forEach((sql, i, all) => {
      parts.push(
        all.length > 1
          ? `## Ground truth SQL (${i + 1})`
          : "## Ground truth SQL",
      );
      parts.push("", "```sql", sql.trim(), "```", "");
    });

  if (record.predictedSql.trim()) {
    parts.push(
      "## Predicted SQL",
      "",
      "```sql",
      record.predictedSql.trim(),
      "```",
      "",
    );
  }

  record.groundTruthTables.forEach((table, i, all) => {
    parts.push(
      all.length > 1
        ? `## Ground truth result (${i + 1})`
        : "## Ground truth result",
    );
    parts.push("", markdownTable(table), "");
  });

  parts.push("## Predicted result", "");
  if (record.predictionError) {
    parts.push(
      `> The predicted SQL did not execute: ${record.predictionError}`,
      "",
    );
  } else if (record.predictedTable) {
    parts.push(markdownTable(record.predictedTable), "");
  } else {
    parts.push("_no result_", "");
  }

  if (record.metrics.length > 0) {
    parts.push("## Metrics", "");
    parts.push("| Metric | Value | What it means |");
    parts.push("| --- | --- | --- |");
    record.metrics.forEach((m) => {
      parts.push(
        `| \`${m.name}\` | ${escapeMarkdownCell(m.value)} | ${escapeMarkdownCell(m.description)} |`,
      );
    });
    parts.push("");
  }
  return parts.join("\n");
}

export function toHtml(record: ExportableRecord): string {
  const sqlBlocks = record.groundTruthSqls
    .filter((sql) => sql.trim())
    .map(
      (sql, i, all) =>
        `<h2>Ground truth SQL${all.length > 1 ? ` (${i + 1})` : ""}</h2><pre><code>${escapeHtml(sql.trim())}</code></pre>`,
    )
    .join("\n");

  const gtTables = record.groundTruthTables
    .map(
      (t, i, all) =>
        `<h2>Ground truth result${all.length > 1 ? ` (${i + 1})` : ""}</h2>${htmlTable(t)}`,
    )
    .join("\n");

  const predictedBlock = record.predictionError
    ? `<p class="error">The predicted SQL did not execute: ${escapeHtml(record.predictionError)}</p>`
    : record.predictedTable
      ? htmlTable(record.predictedTable)
      : "<p><em>no result</em></p>";

  const metricRows = record.metrics
    .map(
      (m) =>
        `<tr><td><code>${escapeHtml(m.name)}</code></td><td>${escapeHtml(m.value)}</td><td>${escapeHtml(m.description)}</td></tr>`,
    )
    .join("\n");

  // Self-contained: no external stylesheet, so it renders the same from a file,
  // an email attachment, or a print dialog.
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>${escapeHtml(record.benchmarkId)} — record ${escapeHtml(record.recordId)}</title>
<style>
  body { font: 15px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
         max-width: 60rem; margin: 2rem auto; padding: 0 1rem; color: #161616; }
  h1 { font-size: 1.5rem; margin-bottom: 0.25rem; }
  h2 { font-size: 1.05rem; margin-top: 1.75rem; }
  .meta { color: #525252; margin: 0 0 1.5rem; }
  .meta a { color: #0f62fe; }
  pre { background: #f4f4f4; padding: 0.75rem; overflow-x: auto; border-radius: 4px; }
  code { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.875em; }
  table { border-collapse: collapse; width: 100%; margin: 0.5rem 0; font-size: 0.875rem; }
  th, td { border: 1px solid #e0e0e0; padding: 0.35rem 0.5rem; text-align: left; vertical-align: top; }
  th { background: #f4f4f4; }
  .note, .error { color: #525252; font-size: 0.875rem; }
  .error { color: #da1e28; }
  @media print { body { margin: 0; max-width: none; } h2 { break-after: avoid; } table { break-inside: auto; } }
</style>
</head>
<body>
<h1>${escapeHtml(record.benchmarkId)} — record ${escapeHtml(record.recordId)}</h1>
<p class="meta">
  ${record.question ? `<strong>Question.</strong> ${escapeHtml(record.question)}<br>` : ""}
  ${record.dbId ? `<strong>Database.</strong> <code>${escapeHtml(record.dbId)}</code><br>` : ""}
  ${record.pipeline ? `<strong>Pipeline.</strong> <code>${escapeHtml(record.pipeline)}</code><br>` : ""}
  <strong>Link.</strong> <a href="${escapeHtml(record.url)}">${escapeHtml(record.url)}</a>
</p>
${sqlBlocks}
${record.predictedSql.trim() ? `<h2>Predicted SQL</h2><pre><code>${escapeHtml(record.predictedSql.trim())}</code></pre>` : ""}
${gtTables}
<h2>Predicted result</h2>
${predictedBlock}
${metricRows ? `<h2>Metrics</h2><table><thead><tr><th>Metric</th><th>Value</th><th>What it means</th></tr></thead><tbody>${metricRows}</tbody></table>` : ""}
</body>
</html>`;
}
