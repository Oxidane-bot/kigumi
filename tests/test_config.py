from __future__ import annotations

import os
from pathlib import Path

import pytest

from kigumi.config import KigumiConfig, find_project_root, load_config, load_env
from kigumi.pi import PiModelConfig, PiProviderConfig


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


def test_source_dirs_rejects_bare_string_and_invalid_entries(tmp_path: Path) -> None:
    with pytest.raises(
        ValueError,
        match=r"source_dirs must be a list of non-empty strings, got str",
    ):
        KigumiConfig(project_root=tmp_path, source_dirs="src")  # type: ignore[arg-type]

    for source_dirs in (None, [""], ["src", 1], {"src"}):
        with pytest.raises(ValueError, match="source_dirs"):
            KigumiConfig(project_root=tmp_path, source_dirs=source_dirs)  # type: ignore[arg-type]


def test_source_dirs_accepts_tuples_and_empty_lists(tmp_path: Path) -> None:
    assert KigumiConfig(project_root=tmp_path, source_dirs=()).source_paths == []
    assert KigumiConfig(project_root=tmp_path, source_dirs=("src",)).source_paths == [
        (tmp_path / "src").resolve()
    ]


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


def test_configured_paths_reject_user_symlink_components(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    linked = tmp_path / "prompts-link"
    try:
        linked.symlink_to(outside, target_is_directory=True)
    except OSError as error:
        pytest.skip(f"target filesystem does not support directory symlinks: {error}")

    config = KigumiConfig(
        project_root=tmp_path,
        prompts_dir="prompts-link",
    )
    with pytest.raises(ValueError, match="symlink"):
        _ = config.prompts_path


def test_unknown_config_key_fails_loudly(tmp_path: Path) -> None:
    """教训 config_typo: 拼错配置键不能静默关闭守卫。"""
    (tmp_path / "pyproject.toml").write_text(
        "[tool.kigumi]\npromtps_dir = 'wrong'\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Unknown kigumi configuration keys: promtps_dir"):
        load_config(tmp_path)


def test_load_config_parses_agent_profiles_and_pi_defaults(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text(
        """
[tool.kigumi.agent_profiles.writer]
capsule = "agents/writer"
runtime = "pi"
expected_version = "0.83.0"
""",
        encoding="utf-8",
    )

    config = load_config(tmp_path)

    assert config is not None
    profile = config.agent_profiles["writer"]
    assert profile.capsule == "agents/writer"
    assert profile.runtime == "pi"
    assert profile.command == ("pi",)
    assert profile.expected_version == "0.83.0"
    assert profile.session_carry is False
    assert profile.session_max_bytes == 2 * 1024 * 1024


def test_agent_profile_config_accepts_explicit_runtime_options(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text(
        """
[tool.kigumi.agent_profiles.reviewer]
capsule = "agents/reviewer"
runtime = "pi"
command = ["pi", "--verbose"]
expected_version = "0.83.1"
session_carry = true
session_max_bytes = 4096
""",
        encoding="utf-8",
    )

    config = load_config(tmp_path)

    assert config is not None
    profile = config.agent_profiles["reviewer"]
    assert profile.command == ("pi", "--verbose")
    assert profile.session_carry is True
    assert profile.session_max_bytes == 4096


def test_agent_profile_config_parses_typed_pi_providers(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text(
        """
[tool.kigumi.agent_profiles.writer]
capsule = "agents/writer"
runtime = "pi"
expected_version = "0.83.0"

[[tool.kigumi.agent_profiles.writer.providers]]
id = "custom-gateway"
api = "openai-responses"
base_url = "https://gateway.example/v1"
api_key_env = "CUSTOM_GATEWAY_API_KEY"

[[tool.kigumi.agent_profiles.writer.providers.models]]
id = "custom-model"
""",
        encoding="utf-8",
    )

    config = load_config(tmp_path)

    assert config is not None
    assert config.agent_profiles["writer"].providers == (
        PiProviderConfig(
            id="custom-gateway",
            api="openai-responses",
            base_url="https://gateway.example/v1",
            api_key_env="CUSTOM_GATEWAY_API_KEY",
            models=(PiModelConfig(id="custom-model"),),
        ),
    )


def test_agent_profile_typed_provider_rejects_unknown_provider_and_model_keys(
    tmp_path: Path,
) -> None:
    (tmp_path / "pyproject.toml").write_text(
        """
[tool.kigumi.agent_profiles.writer]
capsule = "agents/writer"
runtime = "pi"
expected_version = "0.83.0"

[[tool.kigumi.agent_profiles.writer.providers]]
id = "custom-gateway"
api = "openai-responses"
base_url = "https://gateway.example/v1"
api_key_env = "CUSTOM_GATEWAY_API_KEY"
api_key = "must-not-be-accepted"
models = [{ id = "custom-model" }]
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="Unknown Pi provider configuration keys: api_key"):
        load_config(tmp_path)

    (tmp_path / "pyproject.toml").write_text(
        """
[tool.kigumi.agent_profiles.writer]
capsule = "agents/writer"
runtime = "pi"
expected_version = "0.83.0"

[[tool.kigumi.agent_profiles.writer.providers]]
id = "custom-gateway"
api = "openai-responses"
base_url = "https://gateway.example/v1"
api_key_env = "CUSTOM_GATEWAY_API_KEY"
models = [{ id = "custom-model", name = "unknown" }]
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="Unknown Pi model configuration keys: name"):
        load_config(tmp_path)


def test_agent_profile_config_rejects_unknown_keys_and_invalid_types(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text(
        """
[tool.kigumi.agent_profiles.writer]
capsule = "agents/writer"
runtime = "pi"
expected_version = "0.83.0"
extra_config_files = {}
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="Unknown Agent profile configuration keys"):
        load_config(tmp_path)

    (tmp_path / "pyproject.toml").write_text(
        """
[tool.kigumi.agent_profiles.writer]
capsule = "agents/writer"
runtime = "pi"
expected_version = "0.83.0"
command = "pi"
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="Agent profile command"):
        load_config(tmp_path)


def test_agent_profile_config_rejects_empty_names_and_non_pi_runtime(tmp_path: Path) -> None:
    valid = {
        "capsule": "agents/writer",
        "runtime": "pi",
        "expected_version": "0.83.0",
    }
    with pytest.raises(ValueError, match="Agent profile names"):
        KigumiConfig(project_root=tmp_path, agent_profiles={"": valid})

    (tmp_path / "pyproject.toml").write_text(
        """
[tool.kigumi.agent_profiles.writer]
capsule = "agents/writer"
runtime = "other"
expected_version = "0.83.0"
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match='Agent profile runtime must be "pi"'):
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
