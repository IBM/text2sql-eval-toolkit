import React, { useCallback, useEffect, useMemo, useState } from "react";
import {
  Button,
  ComboBox,
  InlineNotification,
  Tag,
  TextInput,
} from "@carbon/react";
import { Add, Code, Copy, Edit, TrashCan } from "@carbon/icons-react";
import type { Diagnostic } from "@codemirror/lint";
import { apiFetch, apiUrl } from "../lib/api";
import {
  formatConfig,
  missingRequiredFields,
  parseConfig,
  toYaml,
} from "../lib/judgeConfig";
import { CodeEditor } from "./CodeEditor";

interface ConfigInfo {
  name: string;
  path: string;
  user_defined?: boolean;
}

interface ConfigListResponse {
  items: ConfigInfo[];
}

const DEFAULT_LLM_JUDGE_CONFIG_NAME = "llm_judge_default_config";

// Matches the server's own rule, so a bad name is refused here with an
// explanation instead of coming back as an opaque 400.
const NAME_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/;

// What a new config starts as. A template beats an empty box: the two required
// keys are already in place, and the model id shows the provider:name form
// rather than leaving it to be guessed.
//
// Written as YAML rather than an object so the block scalar is visible in the
// source here too -- which is the whole argument for editing YAML.
const NEW_CONFIG_TEMPLATE = `model:
  id: anthropic:claude-sonnet-4-5
  max_tokens: 1000
  temperature: 0

prompt_template: |
  You are evaluating whether a predicted SQL query correctly answers a question.

  Question: {question}
  Ground truth SQL: {ground_truth_sql}
  Predicted SQL: {predicted_sql}
  Ground truth result: {ground_truth_df}
  Predicted result: {predicted_df}

  Answer Yes if the prediction correctly answers the question, No otherwise.
  Start your reply with Yes or No, then explain.
`;

export const LLMJudgeConfigView: React.FC = () => {
  const [configs, setConfigs] = useState<ConfigInfo[]>([]);
  const [selected, setSelected] = useState<ConfigInfo | null>(null);
  const [raw, setRaw] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  // Non-null while naming a new config; the name is only settled on save.
  const [newName, setNewName] = useState<string | null>(null);
  // Non-null while renaming the selected config.
  const [renameTo, setRenameTo] = useState<string | null>(null);

  const refreshList = useCallback(async (): Promise<ConfigInfo[]> => {
    const res = await apiFetch(apiUrl("/api/llm-judge/configs"));
    const json: ConfigListResponse = await res.json();
    setConfigs(json.items);
    return json.items;
  }, []);

  const loadConfig = useCallback(async (cfg: ConfigInfo) => {
    try {
      setLoading(true);
      setError(null);
      setMessage(null);
      const res = await apiFetch(apiUrl(`/api/llm-judge/configs/${cfg.name}`));
      const json = await res.json();
      // Back to YAML, which is what is on disk. The endpoint returns the
      // parsed structure; the shape is the same either way.
      setRaw(toYaml(json));
    } catch (e: any) {
      setError(e.message || "Failed to load config");
      setRaw("");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    const load = async () => {
      try {
        setError(null);
        const items = await refreshList();
        const defaultCfg = items.find(
          (c) => c.name === DEFAULT_LLM_JUDGE_CONFIG_NAME,
        );
        if (defaultCfg) {
          setSelected(defaultCfg);
          await loadConfig(defaultCfg);
        }
      } catch (e: any) {
        setError(e.message || "Failed to load config list");
      }
    };
    void load();
  }, [loadConfig, refreshList]);

  const startNewConfig = () => {
    setError(null);
    setMessage(null);
    setSelected(null);
    setNewName("");
    setRaw(NEW_CONFIG_TEMPLATE);
  };

  /**
   * Start a new config from the one on screen.
   *
   * The editor's contents are kept as they are; only the name is asked for.
   * Starting from a template meant that adapting an existing judge -- changing
   * the model, keeping forty lines of prompt -- began by copying the prompt out
   * of one config and into another by hand.
   */
  const duplicateConfig = () => {
    if (!selected) return;
    setError(null);
    setMessage(null);
    setRenameTo(null);
    setSelected(null);
    setNewName(`${selected.name}_copy`);
  };

  const startRename = () => {
    if (!selected) return;
    setError(null);
    setMessage(null);
    setRenameTo(selected.name);
  };

  const renameConfig = async () => {
    const from = selected?.name;
    const to = (renameTo || "").trim();
    if (!from || !to) return;
    if (!NAME_PATTERN.test(to)) {
      setError(
        "A config name must start with a letter or digit and contain only " +
          "letters, digits, dots, dashes and underscores.",
      );
      return;
    }
    try {
      setLoading(true);
      setError(null);
      const res = await apiFetch(
        apiUrl(`/api/llm-judge/configs/${from}/rename`),
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ new_name: to }),
        },
      );
      const body = await res.json();
      const items = await refreshList();
      setSelected(items.find((c) => c.name === to) ?? null);
      setRenameTo(null);
      setMessage(
        body?.reverted_to_packaged
          ? `Renamed to "${to}". The packaged "${from}" is back in use under its own name.`
          : `Renamed "${from}" to "${to}".`,
      );
    } catch (e: any) {
      setError(e.message || "Failed to rename config");
    } finally {
      setLoading(false);
    }
  };

  const cancelNewConfig = () => {
    setNewName(null);
    setRaw("");
    setError(null);
  };

  const saveConfig = async () => {
    const creating = newName !== null;
    const name = creating ? (newName || "").trim() : selected?.name;
    if (!name) return;

    if (creating && !NAME_PATTERN.test(name)) {
      setError(
        "A config name must start with a letter or digit and contain only " +
          "letters, digits, dots, dashes and underscores.",
      );
      return;
    }
    if (creating && configs.some((c) => c.name === name)) {
      setError(`A config named "${name}" already exists. Pick another name.`);
      return;
    }

    const parsedResult = parseConfig(raw);
    if (!parsedResult.ok) {
      // Distinguish a malformed document from a rejected save; they need
      // different fixes. The position is already marked in the editor, so this
      // repeats it rather than being the only place it is said.
      const { message, line, column } = parsedResult.error;
      setError(
        line
          ? `The config is not valid YAML: ${message} (line ${line}, column ${column}).`
          : `The config is not valid YAML: ${message}`,
      );
      return;
    }
    const parsed = parsedResult.value;

    // The server refuses a save without these, with a 400. Saying so here
    // costs a round trip less, and names both at once rather than whichever
    // the server checks first.
    const missing = missingRequiredFields(parsed);
    if (missing.length > 0) {
      setError(
        `This config is valid YAML but not a usable judge: ${missing.join(
          " and ",
        )} ${missing.length > 1 ? "are" : "is"} missing.`,
      );
      return;
    }

    try {
      setLoading(true);
      setError(null);
      setMessage(null);
      await apiFetch(apiUrl(`/api/llm-judge/configs/${name}`), {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(parsed),
      });
      const items = await refreshList();
      const saved = items.find((c) => c.name === name) || null;
      setSelected(saved);
      setNewName(null);
      setMessage(
        creating
          ? `Created "${name}". It is now selectable in the judge playground.`
          : `Saved "${name}".`,
      );
    } catch (e: any) {
      setError(e.message || "Failed to save config");
    } finally {
      setLoading(false);
    }
  };

  const deleteConfig = async () => {
    if (!selected?.user_defined) return;
    const name = selected.name;
    try {
      setLoading(true);
      setError(null);
      setMessage(null);
      const res = await apiFetch(apiUrl(`/api/llm-judge/configs/${name}`), {
        method: "DELETE",
      });
      const body = await res.json();
      const items = await refreshList();
      // A deleted edit may uncover the packaged config it was shadowing.
      const remaining = items.find((c) => c.name === name) || null;
      setSelected(remaining);
      if (remaining) {
        await loadConfig(remaining);
      } else {
        setRaw("");
      }
      setMessage(
        body?.reverted_to_packaged
          ? `Deleted the edit to "${name}"; the packaged config is back in use.`
          : `Deleted "${name}".`,
      );
    } catch (e: any) {
      setError(e.message || "Failed to delete config");
    } finally {
      setLoading(false);
    }
  };

  const editing = newName !== null || selected !== null;

  // Parsed on every keystroke rather than only on save. The point of the item
  // is that a missing indent is *shown*, not discovered by pressing Save.
  const parse = useMemo(() => parseConfig(raw), [raw]);

  const diagnostics = useMemo<Diagnostic[]>(() => {
    if (parse.ok || parse.error.from === undefined) return [];
    return [
      {
        from: parse.error.from,
        to: parse.error.to ?? parse.error.from + 1,
        severity: "error",
        message: parse.error.message,
      },
    ];
  }, [parse]);

  const reformat = () => {
    const result = formatConfig(raw);
    if (!result.ok || result.text === undefined) return;
    setRaw(result.text);
    setMessage(null);
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: "1rem",
        }}
      >
        <h3 style={{ margin: 0 }}>LLM-as-judge configuration</h3>
        <Button
          kind="ghost"
          size="sm"
          renderIcon={Add}
          onClick={startNewConfig}
          disabled={loading}
        >
          New config
        </Button>
      </div>
      {error && (
        <InlineNotification
          kind="error"
          title="Error"
          subtitle={error}
          lowContrast
          onCloseButtonClick={() => setError(null)}
        />
      )}
      {message && (
        <InlineNotification
          kind="success"
          title="Saved"
          subtitle={message}
          lowContrast
          onCloseButtonClick={() => setMessage(null)}
        />
      )}
      {renameTo !== null && (
        <TextInput
          id="llm-config-rename"
          labelText={`Rename "${selected?.name ?? ""}"`}
          helperText="Letters, digits, dots, dashes and underscores."
          value={renameTo}
          onChange={(e) => setRenameTo(e.target.value)}
        />
      )}
      {newName !== null ? (
        <TextInput
          id="llm-config-new-name"
          labelText="New config name"
          helperText="Letters, digits, dots, dashes and underscores."
          value={newName}
          onChange={(e) => setNewName(e.target.value)}
          placeholder="llm_judge_claude_config"
        />
      ) : (
        <div style={{ display: "flex", alignItems: "flex-end", gap: "0.5rem" }}>
          <div style={{ flex: 1 }}>
            <ComboBox
              id="llm-config-select"
              titleText="Select config"
              items={configs}
              itemToString={(item) => (item ? item.name : "")}
              selectedItem={selected}
              onChange={(e) => {
                const cfg = e.selectedItem as ConfigInfo | null;
                setSelected(cfg);
                setMessage(null);
                if (cfg) void loadConfig(cfg);
                else setRaw("");
              }}
              placeholder="Choose a YAML config"
            />
          </div>
          {selected?.user_defined && (
            <Tag type="blue" size="md">
              edited
            </Tag>
          )}
        </div>
      )}
      <div
        style={{
          display: "flex",
          alignItems: "flex-end",
          justifyContent: "space-between",
          gap: "1rem",
        }}
      >
        <label
          htmlFor="llm-config-editor"
          className="cds--label"
          style={{ marginBottom: 0 }}
        >
          Config YAML (edit and save)
        </label>
        <Button
          kind="ghost"
          size="sm"
          renderIcon={Code}
          onClick={reformat}
          disabled={!editing || parse.ok === false}
        >
          Format
        </Button>
      </div>
      <div id="llm-config-editor">
        <CodeEditor
          value={raw}
          onChange={setRaw}
          diagnostics={diagnostics}
          readOnly={!editing}
          ariaLabel="Judge configuration, YAML"
        />
      </div>
      {parse.ok === false && parse.error.line !== undefined && (
        <p
          style={{
            margin: 0,
            fontSize: "0.75rem",
            color: "var(--cds-support-error)",
          }}
        >
          Line {parse.error.line}, column {parse.error.column}:{" "}
          {parse.error.message}
        </p>
      )}
      <div style={{ display: "flex", gap: "0.5rem" }}>
        {renameTo === null && (
          <Button
            kind="primary"
            onClick={() => void saveConfig()}
            disabled={!editing || loading || parse.ok === false}
          >
            {newName !== null ? "Create config" : "Save config"}
          </Button>
        )}
        {newName !== null && (
          <Button kind="secondary" onClick={cancelNewConfig} disabled={loading}>
            Cancel
          </Button>
        )}

        {/* Renaming replaces the row while it is in progress: confirming or
            cancelling it is the only thing that makes sense with a half-typed
            name in the field above. */}
        {renameTo !== null && (
          <>
            <Button
              kind="primary"
              onClick={() => void renameConfig()}
              disabled={loading || !renameTo.trim()}
            >
              Rename
            </Button>
            <Button
              kind="secondary"
              onClick={() => setRenameTo(null)}
              disabled={loading}
            >
              Cancel rename
            </Button>
          </>
        )}

        {newName === null && renameTo === null && selected && (
          <Button
            kind="secondary"
            renderIcon={Copy}
            onClick={duplicateConfig}
            disabled={loading}
          >
            Duplicate
          </Button>
        )}
        {/* Only a config in the data root can move. A packaged one is shared
            with every install, so the server refuses; offering the button
            anyway would be offering a guaranteed error. */}
        {newName === null && renameTo === null && selected?.user_defined && (
          <Button
            kind="secondary"
            renderIcon={Edit}
            onClick={startRename}
            disabled={loading}
          >
            Rename
          </Button>
        )}
        {newName === null && renameTo === null && selected?.user_defined && (
          <Button
            kind="danger--tertiary"
            renderIcon={TrashCan}
            onClick={() => void deleteConfig()}
            disabled={loading}
          >
            Delete
          </Button>
        )}
      </div>
    </div>
  );
};
