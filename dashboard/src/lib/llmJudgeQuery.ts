export function appendLlmJudgeConfigId(
  params: URLSearchParams,
  llmJudgeConfigId: number | null | undefined
): URLSearchParams {
  if (llmJudgeConfigId != null) {
    params.set("llm_judge_config_id", String(llmJudgeConfigId));
  }
  return params;
}

export function withLlmJudgeConfigId(
  path: string,
  llmJudgeConfigId: number | null | undefined
): string {
  if (llmJudgeConfigId == null) {
    return path;
  }
  const separator = path.includes("?") ? "&" : "?";
  return `${path}${separator}llm_judge_config_id=${llmJudgeConfigId}`;
}
