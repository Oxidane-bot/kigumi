"""Minimal explicit project configuration for kigumi integrations."""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path


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

    def resolve(self, path: str | Path) -> Path:
        """Resolve a configured project-relative path to an absolute path."""
        candidate = Path(path)
        if candidate.is_absolute():
            return candidate.resolve()
        return (self.project_root / candidate).resolve()

    @property
    def prompts_path(self) -> Path:
        """The resolved prompt directory."""
        return self.resolve(self.prompts_dir)

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
