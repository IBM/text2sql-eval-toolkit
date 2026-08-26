/**
 * Minimal SQL syntax highlighting for the detail panels.
 *
 * One copy, deliberately. This existed twice -- once in error analysis, once in
 * pipeline detail -- with identical keyword lists and different formatting. The
 * result table went the same way and the two copies drifted apart: one gained
 * pagination and the other rendered every row of an 86,502-row result.
 *
 * Escaping happens first and the markup is added afterwards, so predicted SQL --
 * which is model output, and therefore arbitrary text -- cannot inject markup
 * into the page.
 */

function escapeHtml(text: string): string {
  return text.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
}

export function highlightSql(sql: string): string {
  const escaped = escapeHtml(sql);
  const keywords = [
    "SELECT","FROM","WHERE","GROUP BY","ORDER BY","HAVING","LIMIT","JOIN","LEFT JOIN",
    "RIGHT JOIN","INNER JOIN","OUTER JOIN","ON","AS","AND","OR","NOT","IN","EXISTS",
    "COUNT","SUM","AVG","MIN","MAX","DISTINCT","CASE","WHEN","THEN","ELSE","END",
  ];
  const sorted = keywords.sort((a, b) => b.length - a.length);
  let html = escaped;
  sorted.forEach((kw) => {
    const token = kw.replace(/\s+/g, "\\s+");
    const re = new RegExp(`\\b${token}\\b`, "gi");
    html = html.replace(
      re,
      (m) => `<span style="color:#0f62fe;font-weight:600;">${m.toUpperCase()}</span>`
    );
  });
  return html;
}
