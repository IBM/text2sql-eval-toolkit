import { describe, expect, it } from "vitest";

import {
  type ExportableRecord,
  exportFilename,
  toHtml,
  toMarkdown,
} from "./playgroundExport";

const record: ExportableRecord = {
  benchmarkId: "spider_dev",
  recordId: "1490",
  question: 'How many singers are there in "Gentleman"?',
  dbId: "concert_singer",
  groundTruthSqls: ["SELECT COUNT(*) FROM singer"],
  predictedSql: "SELECT count(*) FROM singer",
  pipeline: "wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi",
  url: "https://example.test/run/spider_dev/record/1490",
  groundTruthTables: [{ columns: ["count(*)"], rows: [[6]] }],
  predictedTable: { columns: ["count(*)"], rows: [[6]] },
  predictionError: null,
  metrics: [
    {
      name: "execution_accuracy",
      value: "1",
      group: "Execution match",
      description: "1 if the result sets match.",
    },
  ],
};

describe("markdown export", () => {
  const md = toMarkdown(record);

  it("carries what an argument about a score needs", () => {
    expect(md).toContain("spider_dev");
    expect(md).toContain("1490");
    expect(md).toContain(record.question);
    expect(md).toContain("SELECT COUNT(*) FROM singer");
    expect(md).toContain("SELECT count(*) FROM singer");
    expect(md).toContain("execution_accuracy");
    expect(md).toContain(record.url);
  });

  it("fences SQL so it survives being pasted", () => {
    expect(md).toContain("```sql");
  });

  it("escapes pipes, which would otherwise split a cell in two", () => {
    const piped: ExportableRecord = {
      ...record,
      metrics: [
        { name: "m", value: "a|b", group: "g", description: "left|right" },
      ],
    };
    const out = toMarkdown(piped);
    expect(out).toContain("a\\|b");
    expect(out).toContain("left\\|right");
  });

  it("summarises a long result rather than pasting thousands of rows", () => {
    const many: ExportableRecord = {
      ...record,
      predictedTable: {
        columns: ["n"],
        rows: Array.from({ length: 120 }, (_, i) => [i]),
      },
    };
    const out = toMarkdown(many);
    expect(out).toContain("70 further row(s) not shown");
  });

  it("says so when the prediction did not execute", () => {
    const failed: ExportableRecord = {
      ...record,
      predictedTable: null,
      predictionError: "no such table: singer",
    };
    expect(toMarkdown(failed)).toContain("no such table: singer");
  });

  it("says why when a record has no metrics", () => {
    // Omitting the section made an export taken before evaluation look the same
    // as one where every metric was missing, with no way to tell which.
    const out = toMarkdown({ ...record, metrics: [] });
    expect(out).toContain("## Metrics");
    expect(out).toContain("had not been evaluated");
  });
});

describe("html export", () => {
  const html = toHtml(record);

  it("is a self-contained document", () => {
    expect(html.startsWith("<!doctype html>")).toBe(true);
    expect(html).toContain("<style>");
    // No external stylesheet or script: it must render the same from a file,
    // an attachment, or a print dialog.
    expect(html).not.toMatch(/<link[^>]+href=["']http/);
    expect(html).not.toMatch(/<script[^>]+src=/);
  });

  it("escapes content rather than letting it become markup", () => {
    const hostile: ExportableRecord = {
      ...record,
      question: '<img src=x onerror="alert(1)">',
      predictedSql: "SELECT '<script>alert(1)</script>'",
    };
    const out = toHtml(hostile);
    expect(out).not.toContain("<img src=x");
    expect(out).not.toContain("<script>alert(1)</script>");
    expect(out).toContain("&lt;img src=x");
  });

  it("carries the metrics table", () => {
    expect(html).toContain("<h2>Metrics</h2>");
    expect(html).toContain("execution_accuracy");
    expect(html).toContain("1 if the result sets match.");
  });

  it("keeps every metric, not just the first", () => {
    const many = {
      ...record,
      metrics: Array.from({ length: 19 }, (_, i) => ({
        name: `metric_${i}`,
        value: String(i),
        group: "g",
        description: `d${i}`,
      })),
    };
    const out = toHtml(many);
    many.metrics.forEach((m) => expect(out).toContain(m.name));
  });

  it("says why when there are none", () => {
    const out = toHtml({ ...record, metrics: [] });
    expect(out).toContain("<h2>Metrics</h2>");
    expect(out).toContain("had not been evaluated");
  });

  it("keeps print rules, so a browser can still print it sensibly", () => {
    expect(html).toContain("@media print");
  });
});

describe("filenames", () => {
  it("names the benchmark and record", () => {
    expect(exportFilename(record, "md")).toBe("spider_dev-1490.md");
  });

  it("strips characters that break a filename", () => {
    const awkward = { ...record, recordId: "6627/65 2a" };
    expect(exportFilename(awkward, "html")).toBe("spider_dev-6627-65-2a.html");
  });
});

describe("judge results in the export", () => {
  const judged: ExportableRecord = {
    ...record,
    storedJudgeExplanation:
      "The prediction counts all singers rather than those in Gentleman.",
    judge: {
      verdict: "No",
      score: 0,
      explanation:
        "The predicted SQL ignores the album filter.\nSo it is wrong.",
      model: "anthropic:claude-sonnet-4-5",
      configName: "llm_judge_claude",
      cached: true,
    },
  };

  it("carries the on-demand verdict into Markdown", () => {
    const md = toMarkdown(judged);
    expect(md).toContain("## LLM judge (on demand)");
    expect(md).toContain("**Verdict.** No (score 0)");
    expect(md).toContain("llm_judge_claude");
    expect(md).toContain("anthropic:claude-sonnet-4-5");
    expect(md).toContain("The predicted SQL ignores the album filter.");
  });

  it("carries the on-demand verdict into HTML", () => {
    const html = toHtml(judged);
    expect(html).toContain("LLM judge (on demand)");
    expect(html).toContain("No (score 0)");
    expect(html).toContain("llm_judge_claude");
    expect(html).toContain("The predicted SQL ignores the album filter.");
  });

  it("carries the stored explanation, which no metric row holds", () => {
    // The view renders this as prose instead of as a metric, so it used to be
    // absent from exports even when the record had been judged.
    expect(toMarkdown(judged)).toContain(
      "The prediction counts all singers rather than those in Gentleman.",
    );
    expect(toHtml(judged)).toContain(
      "The prediction counts all singers rather than those in Gentleman.",
    );
  });

  it("says nothing about a judge when none has run", () => {
    expect(toMarkdown(record)).not.toContain("LLM judge");
    expect(toHtml(record)).not.toContain("LLM judge");
  });

  it("escapes judge prose rather than letting it into the HTML", () => {
    const hostile: ExportableRecord = {
      ...judged,
      judge: { ...judged.judge!, explanation: "<script>alert(1)</script>" },
    };
    const html = toHtml(hostile);
    expect(html).not.toContain("<script>alert(1)</script>");
    expect(html).toContain("&lt;script&gt;");
  });

  it("keeps the line breaks in a multi-paragraph explanation", () => {
    // Judge explanations are written as prose with newlines; collapsing them
    // turns a structured argument into a wall of text.
    expect(toHtml(judged)).toContain("white-space: pre-wrap");
  });
});
