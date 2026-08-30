#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Request and response models for the dashboard API.

Separated from the route handlers so the wire contract can be read in one place:
`server.py` had grown past 3,400 lines with 41 of these interleaved among the
endpoints that use them.

These are data definitions only -- no behaviour -- so anything that needs to
inspect what the API accepts or returns should start here.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class BenchmarkSummary(BaseModel):
    benchmark_id: str
    name: str
    description: str
    db_type: str
    num_records: int
    num_pipelines: int
    logo: Optional[str] = None
    eval_results_bytes: Optional[int] = None


class BenchmarksResponse(BaseModel):
    items: List[BenchmarkSummary]


class BenchmarkConfigInput(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    description: str
    data: str
    schema_path: str = Field(alias="schema")
    predictions: str
    db_engine: Dict[str, Any]
    logo: Optional[str] = None
    logo_url: Optional[str] = None


class BenchmarkConfigResponse(BaseModel):
    benchmark_id: str
    config: Dict[str, Any]


class BenchmarkLogoUploadRequest(BaseModel):
    benchmark_id: str
    filename: Optional[str] = None
    mime_type: Optional[str] = None
    content_base64: str


class PipelineMetrics(BaseModel):
    name: str
    metrics: Dict[str, Any]


class BenchmarkDetailResponse(BaseModel):
    benchmark_id: str
    default_sort_metric: str
    pipelines: List[PipelineMetrics]


class CreateBenchmarkRequest(BenchmarkConfigInput):
    benchmark_id: str


class UpdateBenchmarkRequest(BenchmarkConfigInput):
    pass


class PipelineAliasesResponse(BaseModel):
    """``{alias: pipeline_id}`` for one benchmark, plus its inverse."""

    benchmark_id: str
    aliases: Dict[str, str]
    by_pipeline: Dict[str, str]


class BenchmarkCategorySummaryResponse(BaseModel):
    benchmark_id: str
    default_sort_metric: str
    overall: List[PipelineMetrics]
    categories: Dict[str, List[PipelineMetrics]]
    has_full_results: bool = True


class ErrorRecordSummary(BaseModel):
    record_id: str
    question: str
    predictions: Dict[str, Dict[str, Any]]


class PaginatedErrorResponse(BaseModel):
    items: List[ErrorRecordSummary]
    total: int
    page: int
    page_size: int


class CompareRow(BaseModel):
    pipeline: str
    metric: str
    left: Optional[float]
    right: Optional[float]
    diff: Optional[float]


class CompareResponse(BaseModel):
    benchmark_id: str
    left_id: str
    right_id: str
    rows: List[CompareRow]


class BinaryMetricConfusionCounts(BaseModel):
    a0b0: int
    a0b1: int
    a1b0: int
    a1b1: int


class BinaryMetricConfusionRates(BaseModel):
    a0b0: float
    a0b1: float
    a1b0: float
    a1b1: float


class BinaryMetricConfusionByPipelineRow(BaseModel):
    pipeline: str
    counts: BinaryMetricConfusionCounts
    n_valid: int
    rates: BinaryMetricConfusionRates
    agreement_rate: float
    disagreement_rate: float


class BinaryMetricConfusionByPipelineResponse(BaseModel):
    benchmark_id: str
    metric_a: str
    metric_b: str
    per_pipeline: List[BinaryMetricConfusionByPipelineRow]


class CrossPipelineBinaryMetricConfusionCounts(BaseModel):
    left0right0: int
    left0right1: int
    left1right0: int
    left1right1: int


class CrossPipelineBinaryMetricConfusionRates(BaseModel):
    left0right0: float
    left0right1: float
    left1right0: float
    left1right1: float


class CrossPipelineBinaryMetricConfusionResponse(BaseModel):
    benchmark_id: str
    left_id: str
    right_id: str
    metric_left: str
    metric_right: str
    n_valid: int
    counts: CrossPipelineBinaryMetricConfusionCounts
    rates: CrossPipelineBinaryMetricConfusionRates
    agreement_rate: float
    disagreement_rate: float


class LLMJudgeConfigInfo(BaseModel):
    name: str
    path: str


class LLMJudgeConfigListResponse(BaseModel):
    items: List[LLMJudgeConfigInfo]


class EvaluateRequest(BaseModel):
    use_llm: bool = False
    llm_judge_config_path: Optional[str] = None
    force_rerun_llm_judge: bool = False
    force_rerun: bool = False


class ExecuteSqlRequest(BaseModel):
    sql: str
    record_id: Optional[str] = None
    db_id: Optional[str] = None
    timeout_s: Optional[int] = None


class ExecuteSqlResponse(BaseModel):
    benchmark_id: str
    db_type: str
    sql: str
    db_id: Optional[str] = None
    execution_time_ms: float
    row_count: int
    column_count: int
    result: Dict[str, Any]


class AddGroundTruthSqlRequest(BaseModel):
    record_id: str
    sql: str


class AddGroundTruthSqlResponse(BaseModel):
    benchmark_id: str
    record_id: str
    added: bool
    message: str
    ground_truth_count: int


class RecordIdItem(BaseModel):
    record_id: str
    question: str


class RecordIdsResponse(BaseModel):
    benchmark_id: str
    items: List[RecordIdItem]


class PipelinePlaygroundInfo(BaseModel):
    name: str
    predicted_sql: Optional[str] = None
    has_prompt: bool = False
    has_agent_trace: bool = False
    evaluation: Optional[Dict[str, Any]] = None
    prediction_error: Optional[str] = None
    prediction_row_count: Optional[int] = None
    prediction_column_count: Optional[int] = None
    predicted_df: Optional[str] = None


class PlaygroundInitResponse(BaseModel):
    benchmark_id: str
    record_id: str
    question: str
    db_id: Optional[str] = None
    ground_truth_sqls: List[str]
    pipelines: List[PipelinePlaygroundInfo] = Field(default_factory=list)
    ground_truth_row_counts: List[int] = Field(default_factory=list)
    ground_truth_dfs: List[str] = Field(default_factory=list)


class PlaygroundEvaluateRequest(BaseModel):
    record_id: str
    ground_truth_sqls: List[str]
    predicted_sql: str
    timeout_s: Optional[int] = 90
    use_llm: bool = False
    llm_judge_config_path: Optional[str] = None
    force_rerun_llm_judge: bool = False
    merge_pipeline: Optional[str] = None


class PlaygroundEvaluateResponse(BaseModel):
    benchmark_id: str
    record_id: str
    evaluation: Dict[str, Any]
    ground_truth_row_counts: List[int] = Field(default_factory=list)
    ground_truth_dfs: List[str] = Field(
        default_factory=list,
        description="Pandas orient=split JSON per ground-truth SQL (same order as ground_truth_sqls)",
    )
    predicted_df: Optional[str] = Field(
        default=None,
        description="Pandas orient=split JSON for predicted SQL result (empty schema if execution failed)",
    )
    prediction_error: Optional[str] = None
    prediction_row_count: Optional[int] = None
    prediction_column_count: Optional[int] = None


class EvaluationMetricDefinitionsResponse(BaseModel):
    groups: List[str]
    metrics: List[Dict[str, Any]]


class JobStatus(BaseModel):
    job_id: str
    benchmark_id: str
    status: str
    error: Optional[str] = None


class FetchJobStatus(BaseModel):
    job_id: str
    state: str  # queued | running | completed | failed
    bytes_downloaded: int = 0
    total_bytes: int = 0
    error: Optional[str] = None


class JudgeRequest(BaseModel):
    record_id: str
    pipeline: str
    config_name: Optional[str] = None


class JudgeUsage(BaseModel):
    month: str
    spent_usd: float
    budget_usd: float
    remaining_usd: float
    calls: int
    warning: bool


class JudgeResponse(BaseModel):
    benchmark_id: str
    record_id: str
    pipeline: str
    verdict: str
    score: Optional[float]
    explanation: Optional[str]
    model: str
    config_name: str
    cached: bool
    # Distinguishes this from the llm_score baked into the published snapshot.
    source: str = "on-demand"
    usage: Optional[JudgeUsage] = None


class SessionInfo(BaseModel):
    """What the frontend needs to decide which actions to offer."""

    tier: str
    mode: str
    email: Optional[str] = None
    signed_in: bool = False
    can_run_judge: bool = False
    can_mutate: bool = False
    # The caller's role, and whether they may reach the user console. Admin
    # is not a tier -- it is a separate gate -- so it needs its own field.
    role: str = "read_only"
    can_manage_users: bool = False
    # Remaining budget, so a user sees the ceiling approaching rather than
    # meeting it as an opaque error.
    judge_usage: Optional["JudgeUsage"] = None


class DeploymentInfo(BaseModel):
    """What this server is serving, and how current it is."""

    mode: str
    toolkit_version: str
    # Which Hugging Face snapshot the results came from. A link shared today is
    # only interpretable months later if the reader can see which data it shows.
    data_revision: Optional[str] = None
    data_provisioned_at: Optional[str] = None
    # Pre-computed results, not a live evaluation. Worth stating outright.
    results_are_precomputed: bool = True
    sign_in_available: bool = False
    judge_available: bool = False


class ResultsFetchRequest(BaseModel):
    benchmarks: Optional[List[str]] = None
    pipelines: Optional[List[str]] = None
    models: Optional[List[str]] = None
    revision: Optional[str] = None
    force: bool = False
