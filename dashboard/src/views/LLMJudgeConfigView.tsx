import React, { useEffect, useState } from "react";
import {
  Button,
  ComboBox,
  InlineNotification,
  TextArea,
} from "@carbon/react";
import { apiUrl } from "../lib/api";

interface ConfigInfo {
  name: string;
  path: string;
}

interface ConfigListResponse {
  items: ConfigInfo[];
}

export const LLMJudgeConfigView: React.FC = () => {
  const [configs, setConfigs] = useState<ConfigInfo[]>([]);
  const [selected, setSelected] = useState<ConfigInfo | null>(null);
  const [raw, setRaw] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    const load = async () => {
      try {
        setError(null);
        const res = await fetch(apiUrl("/api/llm-judge/configs"));
        if (!res.ok) {
          throw new Error(`HTTP ${res.status}`);
        }
        const json: ConfigListResponse = await res.json();
        setConfigs(json.items);
      } catch (e: any) {
        setError(e.message || "Failed to load config list");
      }
    };
    void load();
  }, []);

  const loadConfig = async (cfg: ConfigInfo) => {
    try {
      setLoading(true);
      setError(null);
      setMessage(null);
      const res = await fetch(apiUrl(`/api/llm-judge/configs/${cfg.name}`));
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }
      const json = await res.json();
      setRaw(JSON.stringify(json, null, 2));
    } catch (e: any) {
      setError(e.message || "Failed to load config");
      setRaw("");
    } finally {
      setLoading(false);
    }
  };

  const saveConfig = async () => {
    if (!selected) return;
    try {
      setLoading(true);
      setError(null);
      setMessage(null);
      const parsed = JSON.parse(raw);
      const res = await fetch(apiUrl(`/api/llm-judge/configs/${selected.name}`), {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(parsed),
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(txt || `HTTP ${res.status}`);
      }
      setMessage("Config saved successfully");
    } catch (e: any) {
      setError(e.message || "Failed to save config");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
      <h3 style={{ margin: 0 }}>LLM-as-judge configuration</h3>
      {error && (
        <InlineNotification
          kind="error"
          title="Error"
          subtitle={error}
          lowContrast
        />
      )}
      {message && (
        <InlineNotification
          kind="success"
          title="Saved"
          subtitle={message}
          lowContrast
        />
      )}
      <ComboBox
        id="llm-config-select"
        titleText="Select config"
        items={configs}
        itemToString={(item) => (item ? item.name : "")}
        selectedItem={selected}
        onChange={(e) => {
          const cfg = e.selectedItem as ConfigInfo | null;
          setSelected(cfg);
          if (cfg) void loadConfig(cfg);
        }}
        placeholder="Choose a YAML config"
      />
      <TextArea
        id="llm-config-editor"
        labelText="Config JSON (edit and save)"
        rows={20}
        value={raw}
        onChange={(e) => setRaw(e.target.value)}
        disabled={!selected}
      />
      <div>
        <Button kind="primary" onClick={() => void saveConfig()} disabled={!selected || loading}>
          Save config
        </Button>
      </div>
    </div>
  );
};

