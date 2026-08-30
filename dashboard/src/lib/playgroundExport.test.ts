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

  it("handles a record with no metrics yet", () => {
    expect(() => toMarkdown({ ...record, metrics: [] })).not.toThrow();
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
    expect(html).toContain("execution_accuracy");
    expect(html).toContain("<table>");
  });

  it("has print rules, since PDF goes through the print dialog", () => {
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
