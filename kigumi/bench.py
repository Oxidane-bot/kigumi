"""Deterministic evidence grids for functions, callers and DAG-backed subjects."""

from __future__ import annotations

import copy
import inspect
import json
import statistics
import tempfile
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Literal, Protocol

from . import artifacts
from .agents import (
    AgentAdapter,
    AgentBuildContext,
    AgentSpec,
    AgentTask,
    agent_external_identity,
)
from .calling import Caller, LLMCaller
from .config import KigumiConfig
from .dag import Dag
from .evals import Judgment, Metric

SeedMode = Literal["applied", "unsupported"]


@dataclass(frozen=True)
class TrialContext:
    example_id: str
    seed: int
    trial_id: str
    project_root: Path
    evidence_root: Path


@dataclass(frozen=True)
class TrialObservation:
    output: Any
    usage: Mapping[str, Any] | None = None
    evidence: Mapping[str, Any] = field(default_factory=dict)
    seed_applied: bool = False
    duration_seconds: float | None = None


class ExperimentSubject(Protocol):
    seed_mode: SeedMode
    seed_keyed: bool

    def identity(self) -> Mapping[str, Any]: ...

    def run(self, example: dict[str, Any], context: TrialContext) -> TrialObservation: ...


@dataclass(frozen=True)
class FunctionSubject:
    function: Callable[[dict[str, Any], TrialContext], TrialObservation | Any]
    identity_data: Mapping[str, Any]
    seed_mode: SeedMode = "unsupported"
    seed_keyed: bool = False

    def __init__(
        self,
        function: Callable[[dict[str, Any], TrialContext], TrialObservation | Any],
        *,
        identity: Mapping[str, Any],
        seed_mode: SeedMode = "unsupported",
        seed_keyed: bool = False,
    ) -> None:
        object.__setattr__(self, "function", function)
        object.__setattr__(self, "identity_data", copy.deepcopy(dict(identity)))
        object.__setattr__(self, "seed_mode", seed_mode)
        object.__setattr__(self, "seed_keyed", seed_keyed)
        artifacts.canonical_json(self.identity_data)

    def identity(self) -> Mapping[str, Any]:
        return copy.deepcopy(dict(self.identity_data))

    def run(self, example: dict[str, Any], context: TrialContext) -> TrialObservation:
        result = self.function(copy.deepcopy(example), context)
        if isinstance(result, TrialObservation):
            return result
        return TrialObservation(result, seed_applied=self.seed_mode == "applied")


@dataclass(frozen=True)
class CallerSubject:
    task: Callable[[dict[str, Any], Caller], Any]
    caller_factory: Callable[[int, TrialContext], Caller]
    identity_data: Mapping[str, Any]
    seed_mode: SeedMode = "applied"
    seed_keyed: bool = True

    def identity(self) -> Mapping[str, Any]:
        return copy.deepcopy(dict(self.identity_data))

    def run(self, example: dict[str, Any], context: TrialContext) -> TrialObservation:
        caller = self.caller_factory(context.seed, context)
        output = self.task(copy.deepcopy(example), caller)
        calls = getattr(caller, "calls", None)
        usage = _usage(calls) if isinstance(calls, list) else None
        return TrialObservation(
            output,
            usage=usage,
            evidence={"calls": copy.deepcopy(calls)} if isinstance(calls, list) else {},
            seed_applied=True,
        )


@dataclass(frozen=True)
class DagSubject:
    factory: Callable[[TrialContext], Any]
    target: str
    identity_data: Mapping[str, Any]
    output: Callable[[dict[str, Any]], Any] = lambda artifact: artifact
    seed_mode: SeedMode = "unsupported"
    seed_keyed: bool = False

    def identity(self) -> Mapping[str, Any]:
        return copy.deepcopy(dict(self.identity_data))

    def run(self, example: dict[str, Any], context: TrialContext) -> TrialObservation:
        dag = self.factory(context)
        if dag.config.project_root.resolve() != context.project_root.resolve():
            raise ValueError("DagSubject project_root must equal the trial project_root")
        if dag.config.artifacts_path.resolve() != context.evidence_root.resolve():
            raise ValueError("DagSubject artifacts_path must equal the trial evidence_root")
        node = dag._nodes[self.target]
        result = dag.run(targets=(self.target,))
        artifact = result.artifacts[self.target]
        return TrialObservation(
            self.output(artifact),
            usage=None,
            evidence={"run_id": result.run_id, "target": self.target, "cache": node.cache},
            seed_applied=self.seed_mode == "applied",
        )


def _agent_default_output(artifact: dict[str, Any]) -> Any:
    return copy.deepcopy(artifact["completion"])


class _UnusedTransport:
    def resolve(self, model: str) -> str:
        return model

    def complete(self, messages: list[dict[str, Any]], model: str, **params: Any) -> Any:
        del messages, model, params
        raise RuntimeError("AgentSubject does not use Dag.caller")


class _AgentSubjectFailure(RuntimeError):
    def __init__(self, error: Exception, evidence: Mapping[str, Any]) -> None:
        super().__init__(str(error))
        self.original_type = type(error).__name__
        self.trial_evidence = copy.deepcopy(dict(evidence))


@dataclass(frozen=True)
class AgentSubject:
    """Run one isolated, cache-off Agent DAG per experiment trial."""

    adapter: AgentAdapter
    spec: AgentSpec
    task: Callable[[dict[str, Any], AgentBuildContext], AgentTask]
    files: Callable[[dict[str, Any]], Mapping[str, str | bytes]] | None = None
    output: Callable[[dict[str, Any]], Any] = _agent_default_output
    external_fingerprint: Any | None = None
    seed_mode: SeedMode = "unsupported"
    seed_keyed: bool = False

    def __post_init__(self) -> None:
        if not callable(self.task) or (self.files is not None and not callable(self.files)):
            raise TypeError("AgentSubject task/files must be callable")
        if not callable(self.output):
            raise TypeError("AgentSubject output must be callable")
        artifacts.canonical_json(self.identity())

    def identity(self) -> Mapping[str, Any]:
        execution_identity = agent_external_identity(self.adapter, self.spec)
        if self.external_fingerprint is not None:
            artifacts.canonical_json(self.external_fingerprint)
        value = {
            "kind": "agent",
            "adapter": execution_identity["adapter"],
            "spec": self.spec.identity(),
            "task_source": _callable_digest(self.task),
            "files_source": _callable_digest(self.files) if self.files is not None else None,
            "output_source": _callable_digest(self.output),
            "external_fingerprint": artifacts.sha(self.external_fingerprint)
            if self.external_fingerprint is not None
            else None,
        }
        artifacts.canonical_json(value)
        return copy.deepcopy(value)

    def run(self, example: dict[str, Any], context: TrialContext) -> TrialObservation:
        declared: list[str] = []
        if self.files is not None:
            supplied = self.files(copy.deepcopy(example))
            if not isinstance(supplied, Mapping):
                raise TypeError("AgentSubject files(example) must return a mapping")
            for raw_path, data in supplied.items():
                relative = _trial_file_path(raw_path)
                if relative in declared:
                    raise ValueError(f"AgentSubject files contains duplicate path: {relative}")
                if not isinstance(data, str | bytes):
                    raise TypeError("AgentSubject file values must be text or bytes")
                target = context.project_root / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(data.encode("utf-8") if isinstance(data, str) else data)
                declared.append(relative)
        config = KigumiConfig(
            project_root=context.project_root,
            artifacts_dir=str(context.evidence_root),
            llm_cache_dir=str(context.evidence_root / "_llm"),
            source_dirs=[],
        )
        dag = Dag(config, LLMCaller(_UnusedTransport(), config.llm_cache_path))

        @dag.node("example", params={"example": copy.deepcopy(example)}, cache="off")
        def canonical_example(inputs: dict[str, Any], ctx: Any) -> dict[str, Any]:
            del inputs
            return copy.deepcopy(ctx.params["example"])

        @dag.agent(
            "agent",
            adapter=self.adapter,
            spec=self.spec,
            deps=("example",),
            files=tuple(declared),
            cache="off",
        )
        def execute(inputs: dict[str, dict[str, Any]], ctx: AgentBuildContext) -> AgentTask:
            task = self.task(copy.deepcopy(inputs["example"]), ctx)
            if not isinstance(task, AgentTask):
                raise TypeError("AgentSubject task must return AgentTask")
            return task

        try:
            result = dag.run(targets=("agent",))
        except Exception as error:
            evidence = _latest_agent_failure(context.evidence_root)
            raise _AgentSubjectFailure(error, evidence) from error
        artifact = result.artifacts["agent"]
        return TrialObservation(
            self.output(copy.deepcopy(artifact)),
            usage=copy.deepcopy(artifact.get("usage")),
            evidence={
                "run_id": result.run_id,
                "target": "agent",
                "cache": "off",
                "agent": self.identity(),
                "trajectory": copy.deepcopy(artifact.get("trajectory")),
                "raw_evidence": copy.deepcopy(artifact.get("evidence", [])),
            },
            seed_applied=False,
            duration_seconds=artifact.get("duration_seconds"),
        )


@dataclass(frozen=True)
class Variant:
    name: str
    hypothesis: str
    subject: ExperimentSubject
    incumbent: bool = False


def bench(
    variants: Iterable[Variant],
    examples: Iterable[dict[str, Any]],
    metric: Metric,
    *,
    seeds: Iterable[int] = range(5),
    pass_threshold: float | None = None,
    experiment_dir: Path | None = None,
    report_path: Path | None = None,
) -> dict[str, Any]:
    """Run an isolated evidence grid without choosing, mutating or promoting a winner."""
    variant_items = list(variants)
    example_items = list(examples)
    seed_items = list(seeds)
    _validate_inputs(variant_items, example_items, seed_items)
    identities = []
    for variant in variant_items:
        identity = copy.deepcopy(dict(variant.subject.identity()))
        artifacts.canonical_json(identity)
        identities.append(identity)
        if (
            len(seed_items) > 1
            and isinstance(variant.subject, DagSubject)
            and _dag_target_cache_policy(variant.subject, experiment_dir) == "auto"
        ):
            raise ValueError("multi-seed DagSubject requires target cache='refresh' or cache='off'")

    root = (
        Path(experiment_dir)
        if experiment_dir is not None
        else Path(tempfile.mkdtemp(prefix="kigumi-experiment-"))
    ).resolve()
    root.mkdir(parents=True, exist_ok=True)
    example_ids = [artifacts.sha(example) for example in example_items]
    if len(set(example_ids)) != len(example_ids):
        raise ValueError("bench examples must not contain duplicate example contents")

    trials: list[dict[str, Any]] = []
    scores_by_variant: list[list[float]] = [[] for _ in variant_items]
    by_example: list[dict[str, list[float]]] = [
        {example_id: [] for example_id in example_ids} for _ in variant_items
    ]
    paired_variants = zip(variant_items, identities, strict=True)
    for variant_index, (variant, identity) in enumerate(paired_variants):
        for seed in seed_items:
            for example, example_id in zip(example_items, example_ids, strict=True):
                trial_id = artifacts.sha(
                    {
                        "variant": variant.name,
                        "subject": identity,
                        "example": example_id,
                        "seed": seed,
                    }
                )
                trial_root = root / "trials" / trial_id
                project_root = trial_root / "project"
                evidence_root = trial_root / "evidence"
                project_root.mkdir(parents=True, exist_ok=True)
                evidence_root.mkdir(parents=True, exist_ok=True)
                context = TrialContext(example_id, seed, trial_id, project_root, evidence_root)
                started = time.monotonic()
                error: dict[str, str] | None = None
                try:
                    observation = variant.subject.run(copy.deepcopy(example), context)
                    if not isinstance(observation, TrialObservation):
                        raise TypeError("ExperimentSubject.run must return TrialObservation")
                    expected_seed = variant.subject.seed_mode == "applied"
                    if observation.seed_applied != expected_seed:
                        raise ValueError(
                            "TrialObservation seed_applied contradicts subject seed_mode"
                        )
                except Exception as failure:
                    failure_evidence = getattr(failure, "trial_evidence", {})
                    failure_type = getattr(failure, "original_type", type(failure).__name__)
                    observation = TrialObservation(
                        None,
                        evidence=failure_evidence if isinstance(failure_evidence, Mapping) else {},
                        seed_applied=False,
                    )
                    judgment = Judgment(0.0, f"{failure_type}: {failure}", ("task_error",))
                    error = {
                        "stage": "subject",
                        "type": failure_type,
                        "message": str(failure),
                    }
                else:
                    try:
                        judgment = metric(copy.deepcopy(example), observation.output)
                    except Exception as failure:
                        judgment = Judgment(
                            0.0, f"{type(failure).__name__}: {failure}", ("metric_error",)
                        )
                        error = {
                            "stage": "metric",
                            "type": type(failure).__name__,
                            "message": str(failure),
                        }
                duration = (
                    observation.duration_seconds
                    if observation.duration_seconds is not None
                    else time.monotonic() - started
                )
                scores_by_variant[variant_index].append(judgment.score)
                by_example[variant_index][example_id].append(judgment.score)
                trials.append(
                    {
                        "trial_id": trial_id,
                        "variant": variant.name,
                        "example_id": example_id,
                        "seed": seed,
                        "seed_mode": variant.subject.seed_mode,
                        "seed_keyed": variant.subject.seed_keyed,
                        "seed_applied": observation.seed_applied,
                        "project_root": str(project_root),
                        "evidence_root": str(evidence_root),
                        "duration_seconds": duration,
                        "output": copy.deepcopy(observation.output),
                        "usage": dict(observation.usage) if observation.usage is not None else None,
                        "evidence": dict(observation.evidence),
                        "judgment": _judgment_record(judgment),
                        "error": error,
                    }
                )

    reports = []
    for index, variant in enumerate(variant_items):
        scores = scores_by_variant[index]
        item = {
            "name": variant.name,
            "hypothesis": variant.hypothesis,
            "incumbent": variant.incumbent,
            "subject_identity": identities[index],
            "mean": statistics.mean(scores),
            "stdev": statistics.pstdev(scores),
            "by_example": by_example[index],
        }
        if pass_threshold is not None:
            item["pass_rate"] = sum(score >= pass_threshold for score in scores) / len(scores)
        reports.append(item)
    report = {
        "schema_version": 2,
        "experiment_dir": str(root),
        "examples": example_ids,
        "seeds": seed_items,
        "pass_threshold": pass_threshold,
        "variants": reports,
        "trials": trials,
    }
    if report_path is not None:
        artifacts.atomic_write_json(report_path, report)
    return report


def _validate_inputs(
    variants: list[Variant], examples: list[dict[str, Any]], seeds: list[int]
) -> None:
    if not variants:
        raise ValueError("bench variants must not be empty")
    names = [variant.name for variant in variants]
    if len(set(names)) != len(names):
        raise ValueError("bench variant names must be unique; duplicate name found")
    for variant in variants:
        if not isinstance(variant.hypothesis, str) or not variant.hypothesis.strip():
            raise ValueError("假设是变体的准入证,没有假设的变体是结构层面的乱调参")
        if variant.subject.seed_mode not in {"applied", "unsupported"}:
            raise ValueError("subject seed_mode must be 'applied' or 'unsupported'")
    if sum(variant.incumbent for variant in variants) != 1:
        raise ValueError('必须恰有一个 incumbent；没有现状对照回答不了"比现状好吗"')
    if not examples:
        raise ValueError("bench examples must not be empty")
    if not seeds:
        raise ValueError("bench seeds must not be empty")
    if len(set(seeds)) != len(seeds):
        raise ValueError("bench seeds must not contain duplicates")


def _judgment_record(judgment: Judgment) -> dict[str, Any]:
    return {
        "score": judgment.score,
        "feedback": judgment.feedback,
        "tags": list(judgment.tags),
        "subscores": copy.deepcopy(judgment.subscores),
    }


def _usage(calls: list[Any]) -> dict[str, Any]:
    total_tokens = 0
    observable = False
    for metadata in calls:
        if not isinstance(metadata, dict) or not isinstance(metadata.get("usage"), dict):
            continue
        value = metadata["usage"].get("total_tokens")
        if value is not None:
            total_tokens += int(value)
            observable = True
    return {"calls": len(calls), "total_tokens": total_tokens if observable else None, "cost": None}


def _dag_target_cache_policy(subject: DagSubject, experiment_dir: Path | None) -> str:
    root = Path(experiment_dir or tempfile.mkdtemp(prefix="kigumi-bench-admission-")).resolve()
    probe_id = artifacts.sha({"probe": subject.identity()})
    context = TrialContext("probe", 0, probe_id, root / "probe-project", root / "probe-evidence")
    context.project_root.mkdir(parents=True, exist_ok=True)
    context.evidence_root.mkdir(parents=True, exist_ok=True)
    dag = subject.factory(context)
    return dag._nodes[subject.target].cache


def _callable_digest(function: Callable[..., Any] | None) -> str:
    if function is None:
        raise TypeError("Cannot fingerprint a missing callable")
    try:
        source = inspect.getsource(function)
    except (OSError, TypeError) as error:
        raise ValueError(
            "AgentSubject callables must have inspectable source; use external_fingerprint "
            "for external state, not opaque callables"
        ) from error
    return artifacts.sha(source)


def _trial_file_path(value: Any) -> str:
    if not isinstance(value, str) or not value or "\\" in value or "\x00" in value:
        raise ValueError("AgentSubject file paths must be non-empty POSIX paths")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError(f"Unsafe AgentSubject file path: {value!r}")
    return path.as_posix()


def _latest_agent_failure(evidence_root: Path) -> dict[str, Any]:
    candidates = sorted((evidence_root / "runs").glob("*/failures/agent.json"))
    if not candidates:
        return {}
    path = candidates[-1]
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"failure_path": str(path)}
    return {"failure_path": str(path), "failure": value}
