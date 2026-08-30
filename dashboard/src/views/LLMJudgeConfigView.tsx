import React, { useCallback, useEffect, useState } from "react";
import {
  Button,
  ComboBox,
  InlineNotification,
  Tag,
  TextArea,
  TextInput,
} from "@carbon/react";
import { Add, TrashCan } from "@carbon/icons-react";
import { apiFetch, apiUrl } from "../lib/api";

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
const NEW_CONFIG_TEMPLATE = {
  model: {
    id: "anthropic:claude-sonnet-4-5",
    max_tokens: 1000,
    temperature: 0,
  },
  prompt_template:
    "You are evaluating whether a predicted SQL query correctly answers a question.\n\n" +
    "Question: {question}\n" +
    "Ground truth SQL: {ground_truth_sql}\n" +
    "Predicted SQL: {predicted_sql}\n" +
    "Ground truth result: {ground_truth_df}\n" +
    "Predicted result: {predicted_df}\n\n" +
    "Answer Yes if the prediction correctly answers the question, No otherwise.\n" +
    "Start your reply with Yes or No, then explain.",
};

export const LLMJudgeConfigView: React.FC = () => {
  const [configs, setConfigs] = useState<ConfigInfo[]>([]);
  const [selected, setSelected] = useState<ConfigInfo | null>(null);
  const [raw, setRaw] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  // Non-null while naming a new config; the name is only settled on save.
  const [newName, setNewName] = useState<string | null>(null);

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
      setRaw(JSON.stringify(json, null, 2));
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
        const defaultCfg = items.find((c) => c.name === DEFAULT_LLM_JUDGE_CONFIG_NAME);
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
    setRaw(JSON.stringify(NEW_CONFIG_TEMPLATE, null, 2));
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
          "letters, digits, dots, dashes and underscores."
      );
      return;
    }
    if (creating && configs.some((c) => c.name === name)) {
      setError(`A config named "${name}" already exists. Pick another name.`);
      return;
    }

    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch (e: any) {
      // Distinguish invalid JSON from a rejected save; they need different fixes.
      setError(`The config is not valid JSON: ${e.message}`);
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
          : `Saved "${name}".`
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
          : `Deleted "${name}".`
      );
    } catch (e: any) {
      setError(e.message || "Failed to delete config");
    } finally {
      setLoading(false);
    }
  };

  const editing = newName !== null || selected !== null;

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
      <TextArea
        id="llm-config-editor"
        labelText="Config JSON (edit and save)"
        rows={20}
        value={raw}
        onChange={(e) => setRaw(e.target.value)}
        disabled={!editing}
      />
      <div style={{ display: "flex", gap: "0.5rem" }}>
        <Button
          kind="primary"
          onClick={() => void saveConfig()}
          disabled={!editing || loading}
        >
          {newName !== null ? "Create config" : "Save config"}
        </Button>
        {newName !== null && (
          <Button kind="secondary" onClick={cancelNewConfig} disabled={loading}>
            Cancel
          </Button>
        )}
        {newName === null && selected?.user_defined && (
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
