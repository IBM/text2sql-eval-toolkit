import React from "react";
import { ComboBox } from "@carbon/react";
import type { BenchmarkLlmJudgeConfig } from "../hooks/useLlmJudgeConfigs";

interface Props {
  configs: BenchmarkLlmJudgeConfig[];
  selectedId: number | null;
  onChange: (id: number | null) => void;
  disabled?: boolean;
  id?: string;
}

function configLabel(config: BenchmarkLlmJudgeConfig): string {
  if (config.model_id) {
    return `${config.name} (${config.model_id})`;
  }
  return config.name;
}

export const LlmJudgeSelector: React.FC<Props> = ({
  configs,
  selectedId,
  onChange,
  disabled = false,
  id = "llm-judge-select",
}) => {
  if (configs.length === 0) {
    return null;
  }

  const selected =
    configs.find((item) => item.id === selectedId) ?? configs[0] ?? null;

  return (
    <ComboBox
      id={id}
      titleText="LLM judge"
      items={configs}
      itemToString={(item) => (item ? configLabel(item) : "")}
      selectedItem={selected}
      onChange={(event) => {
        const next = event.selectedItem as BenchmarkLlmJudgeConfig | null;
        onChange(next?.id ?? null);
      }}
      placeholder="Select LLM judge"
      disabled={disabled}
    />
  );
};
