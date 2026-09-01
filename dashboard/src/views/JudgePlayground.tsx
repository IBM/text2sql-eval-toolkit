import React, { useCallback, useEffect, useRef, useState } from "react";
import { Button, ComboBox, InlineNotification, Tag, Tile } from "@carbon/react";
import { Renew } from "@carbon/icons-react";

import { apiFetch, apiUrl } from "../lib/api";
import { toYaml } from "../lib/judgeConfig";

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
  /** Judge config named in the address, if any. Its verdict is restored on load. */
  initialConfigName?: string | null;
  /**
   * Reports the verdict on screen, or null when there is none.
   *
   * The parent owns the address and the export, and both need to know what the
   * judge said -- so this component holds the result but does not keep it to
   * itself.
   */
  onResultChange?: (result: JudgeResult | null) => void;
}

export interface JudgeResult {
  verdict: string;
  score: number | null;
  explanation: string | null;
  model: string;
  config_name: string;
  cached: boolean;
  usage?: { month: string; spent_usd: number; budget_usd?: number } | null;
}

const DEFAULT_CONFIG_NAME = "llm_judge_default_config";

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
  initialConfigName,
  onResultChange,
}) => {
  const [configName, setConfigName] = useState<string>(
    initialConfigName || DEFAULT_CONFIG_NAME,
  );
  const [result, setResult] = useState<JudgeResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [running, setRunning] = useState(false);

  const ready = Boolean(benchmarkId && recordId && pipeline);

  const publish = useCallback(
    (next: JudgeResult | null) => {
      setResult(next);
      onResultChange?.(next);
    },
    [onResultChange],
  );

  const judge = useCallback(
    async (
      config: string,
      cachedOnly: boolean,
      refresh = false,
    ): Promise<JudgeResult | null> => {
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
            config_name: config,
            cached_only: cachedOnly,
            refresh,
          }),
        },
      );
      // 204: asked for a stored verdict, there is none.
      return res.status === 204 ? null : ((await res.json()) as JudgeResult);
    },
    [benchmarkId, recordId, pipeline],
  );

  // `refresh` ignores any stored verdict and judges again, replacing it. An
  // edited config already re-judges by itself -- its contents are in the cache
  // key -- so this is for the case the key cannot see: identical inputs, and a
  // stored verdict you want rid of.
  const run = async (refresh = false) => {
    if (!ready) return;
    setRunning(true);
    setError(null);
    try {
      publish(await judge(configName, false, refresh));
    } catch (e) {
      publish(null);
      setError(e instanceof Error ? e.message : "The judge could not be run");
    } finally {
      setRunning(false);
    }
  };

  // A shared link names the config it was judged with; restore that verdict.
  //
  // Cached-only, always. Opening a link someone sent must not start an
  // inference: the sender is sharing an answer, not authorising the reader to
  // spend on their behalf. If the verdict has since aged out of the cache the
  // reader gets the button, with the right config already selected.
  const restoreKey = `${benchmarkId}|${recordId}|${pipeline}|${initialConfigName ?? ""}`;
  const restoredRef = useRef<string | null>(null);
  useEffect(() => {
    if (!initialConfigName || !ready) return;
    if (restoredRef.current === restoreKey) return;
    restoredRef.current = restoreKey;
    setConfigName(initialConfigName);
    let cancelled = false;
    void (async () => {
      try {
        const found = await judge(initialConfigName, true);
        if (!cancelled && found) publish(found);
      } catch {
        // A restore is best-effort: the button is still there.
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [initialConfigName, ready, restoreKey, judge, publish]);

  // The verdict belongs to the record and pipeline it was given for; when
  // either changes it is no longer about what is on screen.
  const subjectKey = `${benchmarkId}|${recordId}|${pipeline}`;
  const subjectRef = useRef(subjectKey);
  useEffect(() => {
    if (subjectRef.current === subjectKey) return;
    subjectRef.current = subjectKey;
    publish(null);
    setError(null);
  }, [subjectKey, publish]);

  const configNames = configs.length
    ? configs.map((c) => c.name)
    : [DEFAULT_CONFIG_NAME];

  // What the chosen judge actually is: its model and its prompt. Picking a
  // config by name said nothing about what it would ask, and the only way to
  // find out was to leave for the config editor and come back.
  //
  // YAML in a `pre` rather than the CodeMirror editor the config view uses:
  // that editor's chunk is 416 KB against this view's 44 KB, which is a great
  // deal of syntax highlighting for a read-only prompt template.
  const [configYaml, setConfigYaml] = useState<string | null>(null);
  const [configModel, setConfigModel] = useState<string | null>(null);
  const [configError, setConfigError] = useState<string | null>(null);
  const [configOpen, setConfigOpen] = useState(false);

  useEffect(() => {
    if (!configName) return;
    let cancelled = false;
    setConfigError(null);
    void (async () => {
      try {
        const res = await apiFetch(
          apiUrl(`/api/llm-judge/configs/${encodeURIComponent(configName)}`),
        );
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const body = (await res.json()) as Record<string, unknown>;
        if (cancelled) return;
        const model = (body?.model ?? {}) as Record<string, unknown>;
        setConfigModel(
          typeof model.id === "string" ? model.id : null,
        );
        setConfigYaml(toYaml(body));
      } catch (e) {
        if (cancelled) return;
        setConfigYaml(null);
        setConfigModel(null);
        setConfigError(
          e instanceof Error ? e.message : "Could not read the config",
        );
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [configName]);

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
          nothing until one of those changes. <strong>Judge again</strong>{" "}
          ignores the cache and asks the model afresh, replacing the stored
          verdict.
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
              setConfigName((selectedItem as string) ?? DEFAULT_CONFIG_NAME)
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
        {result && (
          <Button
            kind="tertiary"
            renderIcon={Renew}
            disabled={!ready || running}
            onClick={() => void run(true)}
          >
            Judge again
          </Button>
        )}
      </div>

      {(configYaml || configError) && (
        <div
          style={{
            border: "1px solid var(--cds-border-subtle-01)",
            background: "var(--cds-layer-02)",
          }}
        >
          <div
            style={{
              display: "flex",
              alignItems: "center",
              flexWrap: "wrap",
              gap: "0.5rem",
              padding: "0.5rem 0.75rem",
            }}
          >
            <span style={{ fontSize: "0.8125rem", fontWeight: 600 }}>
              Judge config
            </span>
            <code style={{ fontSize: "0.8125rem" }}>{configName}</code>
            {configModel && (
              <Tag type="cool-gray" size="sm" title={configModel}>
                {configModel}
              </Tag>
            )}
            <div style={{ marginInlineStart: "auto" }}>
              <Button
                kind="ghost"
                size="sm"
                onClick={() => setConfigOpen((open) => !open)}
                disabled={!configYaml}
              >
                {configOpen ? "Hide prompt" : "Show prompt"}
              </Button>
            </div>
          </div>
          {configError && (
            <p
              style={{
                margin: 0,
                padding: "0 0.75rem 0.5rem",
                fontSize: "0.8125rem",
                color: "var(--cds-text-error)",
              }}
            >
              Could not read this config: {configError}
            </p>
          )}
          {configOpen && configYaml && (
            // Collapsed by default. The prompt template is the bulk of every
            // config and runs to forty-odd lines, which would push the verdict
            // and the run controls off the screen for a reader who only wanted
            // to know which model was judging.
            <pre
              style={{
                margin: 0,
                padding: "0.75rem",
                borderBlockStart: "1px solid var(--cds-border-subtle-01)",
                background: "var(--cds-layer-01)",
                fontSize: "0.75rem",
                lineHeight: 1.5,
                maxHeight: "22rem",
                overflow: "auto",
                whiteSpace: "pre",
              }}
            >
              {configYaml}
            </pre>
          )}
        </div>
      )}

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
