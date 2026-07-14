"""kigumi: load-bearing joinery for LLM content pipelines."""

__version__ = "0.3.1"

from ._declarations import CachePolicy
from .artifacts import (
    atomic_write_json,
    atomic_write_text,
    canonical_json,
    sha,
    sha256_file,
    write_artifact,
)
from .bench import Variant, bench
from .blobs import BlobStore
from .calling import Budget, BudgetExceeded, Caller, DryRunError, LLMCaller, observe
from .config import KigumiConfig, find_project_root, load_config, load_env
from .dag import (
    CheckpointPending,
    Dag,
    ExplainResult,
    NodeContext,
    PlanResult,
    RunResult,
    UndeclaredInputError,
)
from .errors import OutputOwnershipError
from .evals import Judgment, evaluate, gated_metric, llm_judge, pairwise_judge
from .optimize import Candidate, EvolveResult, evolve_prompt
from .prompt import (
    Clipped,
    KigumiPromptWarning,
    TemplateSlotError,
    clip,
    inject,
    load_template,
    render_items,
    render_template,
    schema_format_section,
    section,
    slot_names,
)
from .repair import RepairExhausted, call_validated, repair_loop
from .slots import AdaptiveCapacity, FileSlots
from .store import approve_checkpoint, diff_runs, gc_artifacts, gc_cache
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
    "AdaptiveCapacity",
    "BlobStore",
    "Budget",
    "BudgetExceeded",
    "Candidate",
    "CachePolicy",
    "Caller",
    "CheckpointPending",
    "Clipped",
    "Dag",
    "DryRunError",
    "EmptyResponseError",
    "ExplainResult",
    "EvolveResult",
    "FileSlots",
    "Judgment",
    "KigumiConfig",
    "KigumiPromptWarning",
    "LLMCaller",
    "LiteLLMTransport",
    "NodeContext",
    "OutputOwnershipError",
    "observe",
    "PlanResult",
    "RepairExhausted",
    "Response",
    "RunResult",
    "ScriptedTransport",
    "StdlibTransport",
    "Subgraph",
    "TemplateSlotError",
    "Transport",
    "TruncatedResponseError",
    "UndeclaredInputError",
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
