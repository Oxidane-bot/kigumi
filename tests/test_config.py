from __future__ import annotations

import os
from pathlib import Path

import pytest

from kigumi.config import find_project_root, load_config, load_env


def test_load_config_returns_none_without_kigumi_table(tmp_path: Path) -> None:
    """教训 zero_config: 未采用 kigumi 的项目不能被插件激活。"""
    assert load_config(tmp_path) is None
    (tmp_path / "pyproject.toml").write_text("[project]\nname = 'plain'\n", encoding="utf-8")

    assert load_config(tmp_path) is None
    assert find_project_root(tmp_path / "nested") == tmp_path


def test_empty_kigumi_table_activates_defaults(tmp_path: Path) -> None:
    """教训 explicit_activation: 空表是选择默认守卫行为的明确动作。"""
    (tmp_path / "pyproject.toml").write_text("[tool.kigumi]\n", encoding="utf-8")

    config = load_config(tmp_path)

    assert config is not None
    assert config.prompts_dir == "prompts"
    assert config.source_dirs == ["nodes", "lib"]
    assert config.prompts_path == (tmp_path / "prompts").resolve()
    assert config.artifacts_path == (tmp_path / "artifacts").resolve()
    assert config.llm_cache_dir == "artifacts/_llm"
    assert config.llm_cache_path == (tmp_path / "artifacts" / "_llm").resolve()
    assert config.agent_slots == 1
    assert config.agent_lock_path == (tmp_path / "artifacts" / "_locks" / "agents").resolve()
    assert config.agent_slot_timeout_seconds == 300


def test_agent_capacity_environment_overrides_project_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "pyproject.toml").write_text(
        """
[tool.kigumi]
agent_slots = 2
agent_lock_dir = "project-locks"
agent_slot_timeout_seconds = 12
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("KIGUMI_AGENT_SLOTS", "4")
    monkeypatch.setenv("KIGUMI_AGENT_LOCK_DIR", str(tmp_path / "machine-locks"))
    monkeypatch.setenv("KIGUMI_AGENT_SLOT_TIMEOUT_SECONDS", "3.5")

    config = load_config(tmp_path)

    assert config is not None
    assert config.agent_slots == 4
    assert config.agent_lock_path == (tmp_path / "machine-locks").resolve()
    assert config.agent_slot_timeout_seconds == 3.5


def test_unknown_config_key_fails_loudly(tmp_path: Path) -> None:
    """教训 config_typo: 拼错配置键不能静默关闭守卫。"""
    (tmp_path / "pyproject.toml").write_text(
        "[tool.kigumi]\npromtps_dir = 'wrong'\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Unknown kigumi configuration keys: promtps_dir"):
        load_config(tmp_path)


def test_load_env_fills_only_missing_process_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """教训 env_priority: 进程环境优先，.env 只能补齐未设置的键。"""
    env_path = tmp_path / ".env"
    env_path.write_text(
        "# comment\nEXISTING=file-value\nNEW_VALUE = fresh\nQUOTED='keep quotes'\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("EXISTING", "process-value")
    monkeypatch.delenv("NEW_VALUE", raising=False)
    monkeypatch.delenv("QUOTED", raising=False)

    assert load_env(env_path) == ["NEW_VALUE", "QUOTED"]
    assert load_env(tmp_path / "missing.env") == []
    assert os.environ["EXISTING"] == "process-value"
    assert os.environ["NEW_VALUE"] == "fresh"
    assert os.environ["QUOTED"] == "'keep quotes'"
