import React, { useState } from "react";
import { Button, ComboBox, InlineNotification, Tag, Tile } from "@carbon/react";

import { apiFetch, apiUrl } from "../lib/api";

/**
 * Run LLM-as-judge on the record currently open, and show what it said.
 *
 * Calls the same endpoint the record view uses, deliberately: that endpoint
 * caches a verdict against the record, the pipeline, the config *and* a digest
 * of the config's contents, and meters spend against the budget. A second path
 * would have to reproduce both, and would drift.
 *
 * The cached flag is surfaced rather than hidden. Judging costs money, and
 * "this answer came from the cache" is the difference between a free click and
 * a billed one.
 */

interface Props {
  benchmarkId: string | null;
  recordId: string | null;
  pipeline: string | null;
  configs: { name: string; path: string }[];
}

interface JudgeResult {
  verdict: string;
  score: number | null;
  explanation: string | null;
  model: string;
  config_name: string;
  cached: boolean;
  usage?: { month: string; spent_usd: number; budget_usd?: number } | null;
}

const VERDICT_TAGS: Record<string, "green" | "red" | "purple" | "gray"> = {
  Yes: "green",
  No: "red",
  Maybe: "purple",
  "N/A": "gray",
};

export const JudgePlayground: React.FC<Props> = ({
  benchmarkId,
  recordId,
  pipeline,
  configs,
}) => {
  const [configName, setConfigName] = useState<string>(
    "llm_judge_default_config",
  );
  const [result, setResult] = useState<JudgeResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [running, setRunning] = useState(false);

  const ready = Boolean(benchmarkId && recordId && pipeline);

  const run = async () => {
    if (!ready) return;
    setRunning(true);
    setError(null);
    try {
      const res = await apiFetch(
        apiUrl(
          `/api/benchmarks/${encodeURIComponent(benchmarkId as string)}/judge`,
        ),
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            record_id: recordId,
            pipeline,
            config_name: configName,
          }),
        },
      );
      setResult(await res.json());
    } catch (e) {
      setResult(null);
      setError(e instanceof Error ? e.message : "The judge could not be run");
    } finally {
      setRunning(false);
    }
  };

  const configNames = configs.length
    ? configs.map((c) => c.name)
    : ["llm_judge_default_config"];

  return (
    <section
      style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}
    >
      <div>
        <h4 style={{ margin: 0, fontSize: "1rem", fontWeight: 600 }}>
          Judge Playground
        </h4>
        <p
          style={{
            margin: "0.35rem 0 0",
            maxWidth: "52rem",
            lineHeight: 1.45,
            fontSize: "0.8125rem",
            color: "var(--cds-text-secondary)",
          }}
        >
          Ask an LLM whether the predicted SQL answers the question, using the
          record above. A verdict is cached against this record, pipeline and
          config — including the config's contents — so running it again costs
          nothing until one of those changes.
        </p>
      </div>

      {error && (
        <InlineNotification
          kind="error"
          title="Judge"
          subtitle={error}
          lowContrast
          onCloseButtonClick={() => setError(null)}
        />
      )}

      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          gap: "0.75rem",
          alignItems: "flex-end",
        }}
      >
        <div style={{ flex: "1 1 20rem", minWidth: "min(100%, 16rem)" }}>
          <ComboBox
            id="judge-config"
            titleText="Judge config"
            items={configNames}
            itemToString={(item) => (item as string) ?? ""}
            selectedItem={configName}
            onChange={({ selectedItem }) =>
              setConfigName(
                (selectedItem as string) ?? "llm_judge_default_config",
              )
            }
          />
        </div>
        <Button
          kind="primary"
          disabled={!ready || running}
          onClick={() => void run()}
        >
          {running ? "Judging…" : "Run judge"}
        </Button>
      </div>

      {!ready && (
        <p
          style={{
            margin: 0,
            fontSize: "0.8125rem",
            color: "var(--cds-text-secondary)",
          }}
        >
          Load a record and pick a pipeline above first.
        </p>
      )}

      {result && (
        <Tile
          style={{ display: "flex", flexDirection: "column", gap: "0.6rem" }}
        >
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: "0.5rem",
              flexWrap: "wrap",
            }}
          >
            <Tag type={VERDICT_TAGS[result.verdict] ?? "gray"} size="sm">
              {result.verdict}
            </Tag>
            {result.score !== null && (
              <span style={{ fontSize: "0.875rem" }}>score {result.score}</span>
            )}
            <Tag type={result.cached ? "cool-gray" : "blue"} size="sm">
              {result.cached ? "from cache — no inference" : "fresh inference"}
            </Tag>
            <span
              style={{
                fontSize: "0.75rem",
                color: "var(--cds-text-secondary)",
                fontFamily: "monospace",
              }}
            >
              {result.model}
            </span>
          </div>

          {result.explanation && (
            <div>
              <p
                style={{
                  margin: "0 0 0.25rem",
                  fontSize: "0.8125rem",
                  fontWeight: 600,
                }}
              >
                Why
              </p>
              <p
                style={{
                  margin: 0,
                  fontSize: "0.8125rem",
                  lineHeight: 1.45,
                  whiteSpace: "pre-wrap",
                }}
              >
                {result.explanation}
              </p>
            </div>
          )}

          {result.usage && (
            <p
              style={{
                margin: 0,
                fontSize: "0.75rem",
                color: "var(--cds-text-secondary)",
              }}
            >
              Spent this month: ${result.usage.spent_usd.toFixed(4)}
              {result.usage.budget_usd ? ` of $${result.usage.budget_usd}` : ""}
              . Rates are an estimate until calibrated against a real invoice.
            </p>
          )}
        </Tile>
      )}
    </section>
  );
};
