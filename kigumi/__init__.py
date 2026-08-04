"""kigumi: load-bearing joinery for LLM content pipelines.

Working in a project that depends on kigumi? Start here:
  kigumi brief             # what this library already owns; do not reimplement it
  kigumi docs              # list every page shipped inside the wheel

Before modifying nodes in a kigumi project:
  kigumi trace <run_id>    # current state: nodes, map items, every LLM call
  kigumi plan              # what would recompute (and cost money) after your change
  kigumi explain <node>    # why will this node miss the cache?

Capability index (need -> symbol, grouped by domain):

  Prompt: inject, load_template, PromptSpec, PromptAxis, section, clip
  Call+cache: LLMCaller(cache_dir, seed), call_validated, repair_loop, Budget
  DAG: Dag, @dag.node/map/scan/foreach, Subgraph, dag.plan/explain/diff
  Binary: files=/files_fn=, ctx.read_text, ctx.emit_file, BlobStore
  Agent: @dag.agent/agent_scan, AgentSpec, PiRpcAdapter, EvidencePolicy
  Eval: bench, llm_judge, pairwise_judge, evolve_prompt
  Test: ScriptedTransport, FakeTransport, @pytest.mark.live, kigumi guard
  Ops: kigumi init/doctor/trace/approve/gc, dag.run/resume

Every page below is readable offline from the installed wheel via `kigumi docs <name>`:
brief, capabilities (full "I need X" index), adoption (narrative guide), api
(signatures and failure handling), cli, contracts (promises), design, changelog.
"""

from ._declarations import CachePolicy, ResourceRequest
from ._runstate import StateIntegrityError
from ._version import __version__
from .agents import (
    AgentAdapter,
    AgentBuildContext,
    AgentCapabilities,
    AgentCompletion,
    AgentError,
    AgentFileSelector,
    AgentLimits,
    AgentPublish,
    AgentRequest,
    AgentResultView,
    AgentRunContext,
    AgentRunResult,
    AgentSpec,
    AgentTask,
)
from .artifacts import (
    atomic_write_json,
    atomic_write_text,
    canonical_json,
    sha,
    sha256_file,
    write_artifact,
)
from .bench import (
    AgentSubject,
    CallerSubject,
    DagSubject,
    ExperimentSubject,
    FunctionSubject,
    TrialContext,
    TrialObservation,
    Variant,
    bench,
)
from .blobs import BlobStore
from .calling import (
    Budget,
    BudgetExceeded,
    BudgetPermit,
    Caller,
    DryRunError,
    LLMCaller,
    observe,
)
from .config import KigumiConfig, find_project_root, load_config, load_env
from .dag import (
    CheckpointPending,
    Dag,
    ExplainResult,
    NodeContext,
    PlanResult,
    RecoveryReceipt,
    RunResult,
    UndeclaredInputError,
)
from .errors import CacheIntegrityError, OutputOwnershipError
from .evals import Judgment, evaluate, gated_metric, llm_judge, pairwise_judge
from .evidence import EvidenceMode, EvidencePolicy
from .failures import (
    AgentExecutionFailure,
    AgentRuntimeFailureCode,
    AgentRuntimeFailureSubCode,
    ProviderFailure,
    ProviderFailureKind,
    ProviderFailureStage,
)
from .optimize import Candidate, EvolveResult, evolve_prompt
from .pi import PiRpcAdapter
from .prompt import (
    Attachment,
    CarryRef,
    Clipped,
    FileRef,
    InputRef,
    ItemRef,
    KigumiPromptWarning,
    Message,
    ParamRef,
    PreflightPolicy,
    PreflightReport,
    PreflightViolation,
    PromptAxis,
    PromptDefinitionError,
    PromptLayer,
    PromptMaterial,
    PromptRef,
    PromptResolution,
    PromptResolutionError,
    PromptSpec,
    RequestTooLarge,
    ResolvedPrompt,
    ResponseSpec,
    TemplateSlotError,
    clip,
    inject,
    load_template,
    preflight,
    render_items,
    render_template,
    schema_format_section,
    section,
    slot_names,
)
from .repair import RepairExhausted, call_validated, repair_loop
from .retry import AmbiguousAttemptError, RetryExhausted, RetryPolicy, RetrySchedule
from .slots import AdaptiveCapacity, FileSlots
from .store import CacheLookup, approve_checkpoint, diff_runs, gc_artifacts, gc_cache
from .subgraph import Subgraph
from .testing import ScriptedTransport
from .transport import (
    EmptyResponseError,
    LiteLLMTransport,
    Response,
    StdlibTransport,
    Transport,
    TruncatedResponseError,
)

__all__ = [
    "__version__",
    "AdaptiveCapacity",
    "Attachment",
    "AgentAdapter",
    "AgentBuildContext",
    "AgentCapabilities",
    "AgentCompletion",
    "AgentError",
    "AgentExecutionFailure",
    "AgentRuntimeFailureCode",
    "AgentRuntimeFailureSubCode",
    "AgentFileSelector",
    "AgentLimits",
    "AgentPublish",
    "AgentRequest",
    "AgentResultView",
    "AgentRunContext",
    "AgentRunResult",
    "AgentSpec",
    "AgentSubject",
    "AgentTask",
    "BlobStore",
    "Budget",
    "BudgetExceeded",
    "BudgetPermit",
    "CacheIntegrityError",
    "CacheLookup",
    "Candidate",
    "CachePolicy",
    "Caller",
    "CallerSubject",
    "CheckpointPending",
    "CarryRef",
    "Clipped",
    "Dag",
    "DagSubject",
    "DryRunError",
    "EvidenceMode",
    "EvidencePolicy",
    "EmptyResponseError",
    "ExplainResult",
    "ExperimentSubject",
    "EvolveResult",
    "FileSlots",
    "FileRef",
    "FunctionSubject",
    "Judgment",
    "InputRef",
    "ItemRef",
    "KigumiConfig",
    "KigumiPromptWarning",
    "LLMCaller",
    "LiteLLMTransport",
    "Message",
    "NodeContext",
    "OutputOwnershipError",
    "ParamRef",
    "PreflightPolicy",
    "PreflightReport",
    "PreflightViolation",
    "observe",
    "PlanResult",
    "PiRpcAdapter",
    "ProviderFailure",
    "ProviderFailureKind",
    "ProviderFailureStage",
    "PromptAxis",
    "PromptDefinitionError",
    "PromptLayer",
    "PromptMaterial",
    "PromptRef",
    "PromptResolution",
    "PromptResolutionError",
    "PromptSpec",
    "RequestTooLarge",
    "RecoveryReceipt",
    "RepairExhausted",
    "RetryExhausted",
    "RetryPolicy",
    "RetrySchedule",
    "Response",
    "ResponseSpec",
    "ResourceRequest",
    "ResolvedPrompt",
    "RunResult",
    "StateIntegrityError",
    "ScriptedTransport",
    "StdlibTransport",
    "Subgraph",
    "TemplateSlotError",
    "Transport",
    "TrialContext",
    "TrialObservation",
    "TruncatedResponseError",
    "UndeclaredInputError",
    "AmbiguousAttemptError",
    "Variant",
    "approve_checkpoint",
    "atomic_write_json",
    "atomic_write_text",
    "bench",
    "call_validated",
    "canonical_json",
    "clip",
    "diff_runs",
    "evaluate",
    "evolve_prompt",
    "find_project_root",
    "gated_metric",
    "gc_cache",
    "gc_artifacts",
    "inject",
    "llm_judge",
    "load_config",
    "load_env",
    "load_template",
    "pairwise_judge",
    "preflight",
    "render_items",
    "render_template",
    "repair_loop",
    "schema_format_section",
    "section",
    "sha",
    "sha256_file",
    "slot_names",
    "write_artifact",
]
