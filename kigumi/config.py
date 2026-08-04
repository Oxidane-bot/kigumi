"""Minimal explicit project configuration for kigumi integrations."""

from __future__ import annotations

import os
import stat
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from ._safe_io import _secure_directory_absolute


def _safe_configured_path(path: str | Path) -> Path:
    """Return an absolute configured path without following user symlinks.

    Configuration paths are often handed to a later descriptor-relative reader.
    Resolving them here would erase a user-controlled symlink before that reader
    can reject it.  Keep the path lexical, normalize only the explicitly trusted
    macOS system aliases, and reject every existing symlink component.
    """
    absolute = _secure_directory_absolute(Path(path))
    probe = Path(absolute.anchor)
    for component in absolute.parts[1:]:
        if component in {"", "."}:
            continue
        probe /= component
        try:
            info = probe.lstat()
        except FileNotFoundError:
            # Once a component is absent, lower components cannot already be
            # present in the same path.  The eventual secure creator/reader
            # remains responsible for races while those components are made.
            break
        if stat.S_ISLNK(info.st_mode):
            raise ValueError(f"Configured path must not contain a symlink: {probe}")
        if probe != absolute and not stat.S_ISDIR(info.st_mode):
            raise ValueError(f"Configured path component must be a directory: {probe}")
    return absolute


def _validate_agent_profile_name(name: object) -> None:
    path = Path(name) if isinstance(name, str) else None
    if (
        path is None
        or not name.strip()
        or "@" in name
        or "/" in name
        or "\\" in name
        or path.name != name
        or name in {".", ".."}
    ):
        raise ValueError(
            "Agent profile names must be non-empty, contain no '@', and be a single "
            "relative path component"
        )


@dataclass(frozen=True)
class AgentProfileConfig:
    """Project-level binding from a profile name to a Pi Agent capsule."""

    capsule: str
    runtime: Literal["pi"]
    expected_version: str
    command: tuple[str, ...] = ("pi",)
    session_carry: bool = False
    session_max_bytes: int = 2 * 1024 * 1024

    def __post_init__(self) -> None:
        for field_name in ("capsule", "expected_version"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"Agent profile {field_name} must be a non-empty string")
            object.__setattr__(self, field_name, value.strip())
        if self.runtime != "pi":
            raise ValueError('Agent profile runtime must be "pi"')
        if (
            not isinstance(self.command, (list, tuple))
            or not self.command
            or not all(isinstance(part, str) and part for part in self.command)
        ):
            raise ValueError("Agent profile command must be a non-empty list of non-empty strings")
        object.__setattr__(self, "command", tuple(self.command))
        if not isinstance(self.session_carry, bool):
            raise TypeError("Agent profile session_carry must be a bool")
        if (
            isinstance(self.session_max_bytes, bool)
            or not isinstance(self.session_max_bytes, int)
            or self.session_max_bytes <= 0
        ):
            raise ValueError("Agent profile session_max_bytes must be positive")

    @classmethod
    def from_mapping(cls, name: str, values: Mapping[str, Any]) -> AgentProfileConfig:
        """Parse one TOML profile table with strict, user-facing errors."""
        _validate_agent_profile_name(name)
        known = {
            "capsule",
            "runtime",
            "command",
            "expected_version",
            "session_carry",
            "session_max_bytes",
        }
        unknown = sorted(set(values) - known)
        if unknown:
            raise ValueError(
                f"Unknown Agent profile configuration keys for {name!r}: {', '.join(unknown)}"
            )
        required = ("capsule", "runtime", "expected_version")
        missing = [key for key in required if key not in values]
        if missing:
            raise ValueError(
                f"Agent profile {name!r} is missing required keys: {', '.join(missing)}"
            )
        return cls(
            capsule=values["capsule"],
            runtime=values["runtime"],
            expected_version=values["expected_version"],
            command=values.get("command", ("pi",)),
            session_carry=values.get("session_carry", False),
            session_max_bytes=values.get("session_max_bytes", 2 * 1024 * 1024),
        )


@dataclass
class KigumiConfig:
    """Project-relative kigumi paths with resolved absolute-path accessors."""

    prompts_dir: str = "prompts"
    artifacts_dir: str = "artifacts"
    llm_cache_dir: str = "artifacts/_llm"
    source_dirs: list[str] = field(default_factory=lambda: ["nodes", "lib"])
    env_file: str = ".env"
    agent_slots: int = 1
    agent_lock_dir: str = "artifacts/_locks/agents"
    agent_slot_timeout_seconds: float = 300.0
    dag_entry: str | None = None
    agent_profiles: dict[str, AgentProfileConfig] = field(default_factory=dict)
    """``module:callable`` returning the project's ``Dag``, enabling ``kigumi plan``.

    Optional on purpose: without it every project-operations command still works,
    because those read artifacts from disk and never import project code. Only the
    graph commands need the in-memory graph, so only they require this key.
    """
    project_root: Path = field(default_factory=Path.cwd, repr=False)

    def __post_init__(self) -> None:
        overrides: tuple[tuple[str, str, type[int] | type[float]], ...] = (
            ("agent_slots", "KIGUMI_AGENT_SLOTS", int),
            (
                "agent_slot_timeout_seconds",
                "KIGUMI_AGENT_SLOT_TIMEOUT_SECONDS",
                float,
            ),
        )
        for field_name, environment_name, parser in overrides:
            raw = os.getenv(environment_name)
            if raw is None:
                continue
            try:
                setattr(self, field_name, parser(raw.strip()))
            except ValueError as error:
                raise ValueError(f"{environment_name} must be a valid number") from error
        lock_override = os.getenv("KIGUMI_AGENT_LOCK_DIR")
        if lock_override is not None:
            if not lock_override.strip():
                raise ValueError("KIGUMI_AGENT_LOCK_DIR must not be empty")
            self.agent_lock_dir = lock_override.strip()
        if (
            isinstance(self.agent_slots, bool)
            or not isinstance(self.agent_slots, int)
            or self.agent_slots < 1
        ):
            raise ValueError("agent_slots must be at least 1")
        if (
            isinstance(self.agent_slot_timeout_seconds, bool)
            or not isinstance(self.agent_slot_timeout_seconds, int | float)
            or self.agent_slot_timeout_seconds <= 0
        ):
            raise ValueError("agent_slot_timeout_seconds must be positive")
        if not isinstance(self.agent_lock_dir, str) or not self.agent_lock_dir:
            raise ValueError("agent_lock_dir must be a non-empty path")
        if self.dag_entry is not None:
            if not isinstance(self.dag_entry, str) or not self.dag_entry.strip():
                raise ValueError("dag_entry must be a non-empty string")
            self.dag_entry = self.dag_entry.strip()
            # Fail here rather than at import time: a malformed target is a config
            # error, and the message should name the expected shape.
            module, separator, attribute = self.dag_entry.partition(":")
            if not separator or not module.strip() or not attribute.strip():
                raise ValueError(
                    f"dag_entry must look like 'module:callable', got {self.dag_entry!r}"
                )
        if not isinstance(self.agent_profiles, Mapping):
            raise ValueError("agent_profiles must be a table")
        profiles: dict[str, AgentProfileConfig] = {}
        for name, value in self.agent_profiles.items():
            if isinstance(value, AgentProfileConfig):
                _validate_agent_profile_name(name)
                profile = value
            elif isinstance(value, Mapping):
                profile = AgentProfileConfig.from_mapping(name, value)
            else:
                raise ValueError(f"Agent profile {name!r} must be a table")
            profiles[name] = profile
        self.agent_profiles = profiles

    def resolve(self, path: str | Path) -> Path:
        """Resolve a configured project-relative path to an absolute path."""
        candidate = Path(path)
        if candidate.is_absolute():
            return candidate.resolve()
        return (self.project_root / candidate).resolve()

    def resolve_agent_capsule(self, path: str | Path) -> Path:
        """Resolve a profile capsule while retaining the no-symlink boundary."""
        candidate = Path(path)
        if not candidate.is_absolute():
            candidate = self.project_root / candidate
        return _safe_configured_path(candidate)

    @property
    def prompts_path(self) -> Path:
        """The prompt directory, retaining and rejecting user symlinks."""
        candidate = Path(self.prompts_dir)
        if not candidate.is_absolute():
            candidate = self.project_root / candidate
        return _safe_configured_path(candidate)

    @property
    def artifacts_path(self) -> Path:
        """The resolved artifact directory."""
        return self.resolve(self.artifacts_dir)

    @property
    def llm_cache_path(self) -> Path:
        """The resolved L1 LLM caller cache directory."""
        return self.resolve(self.llm_cache_dir)

    @property
    def source_paths(self) -> list[Path]:
        """The resolved source directories."""
        return [self.resolve(source_dir) for source_dir in self.source_dirs]

    @property
    def env_path(self) -> Path:
        """The resolved environment-file path."""
        return self.resolve(self.env_file)

    @property
    def agent_lock_path(self) -> Path:
        """The shared lock root for external-Agent execution capacity."""
        return self.resolve(self.agent_lock_dir)


def find_project_root(start: Path) -> Path | None:
    """Find the nearest ancestor containing ``pyproject.toml``."""
    current = start.resolve()
    if current.is_file():
        current = current.parent
    while True:
        if (current / "pyproject.toml").is_file():
            return current
        if current.parent == current:
            return None
        current = current.parent


def load_config(project_root: Path) -> KigumiConfig | None:
    """Load an explicitly activated ``[tool.kigumi]`` table, if present."""
    config_path = project_root / "pyproject.toml"
    if not config_path.is_file():
        return None
    with config_path.open("rb") as handle:
        document = tomllib.load(handle)
    tool = document.get("tool", {})
    if not isinstance(tool, dict) or "kigumi" not in tool:
        return None
    values = tool["kigumi"]
    if not isinstance(values, dict):
        raise ValueError("[tool.kigumi] must be a table")
    known = {
        "prompts_dir",
        "artifacts_dir",
        "llm_cache_dir",
        "source_dirs",
        "env_file",
        "agent_slots",
        "agent_lock_dir",
        "agent_slot_timeout_seconds",
        "dag_entry",
        "agent_profiles",
    }
    unknown = sorted(set(values) - known)
    if unknown:
        raise ValueError(f"Unknown kigumi configuration keys: {', '.join(unknown)}")
    return KigumiConfig(project_root=project_root.resolve(), **values)


def load_env(env_path: Path) -> list[str]:
    """Load missing process variables from a simple project-local ``.env`` file."""
    if not env_path.is_file():
        return []
    loaded: list[str] = []
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key and key not in os.environ:
            os.environ[key] = value
            loaded.append(key)
    return sorted(loaded)
