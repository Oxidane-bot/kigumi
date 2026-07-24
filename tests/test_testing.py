from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any

import pytest

from kigumi.testing import CassetteTransport, FakeTransport, ScriptedTransport, skip_unless_env
from kigumi.transport import Response

pytest_plugins = ["pytester"]


def test_fake_transport_replays_configured_responses_and_records_requests() -> None:
    """教训 fake_transport: 假响应必须按序消费，耗尽不能静默复用旧答案。"""
    transport = FakeTransport(["first", Response("second", {}, "stop")])
    messages = [{"role": "user", "content": "hello"}]

    assert transport.complete(messages, "model-a", temperature=0.2).text == "first"
    assert transport.complete([], "model-b").text == "second"
    assert transport.requests == [
        (messages, "model-a", {"temperature": 0.2}),
        ([], "model-b", {}),
    ]
    with pytest.raises(RuntimeError, match="FakeTransport exhausted"):
        transport.complete([], "model-c")


def test_scripted_transport_requires_a_marker_to_fill_an_entire_line() -> None:
    """教训 scripted_anchor: 模板正文的子串不能误撞阶段路由。"""
    transport = ScriptedTransport({"STAGE: draft": "ok"})

    with pytest.raises(AssertionError, match="STAGE: draft"):
        transport.complete([{"role": "user", "content": "说明 STAGE: draft 只是示例"}], "x")

    assert transport.complete([{"role": "user", "content": "STAGE: draft\n正文"}], "x").text == "ok"


def test_scripted_transport_uses_route_definition_order() -> None:
    """教训 scripted_order: 多个完整标记命中时必须有稳定优先级。"""
    transport = ScriptedTransport({"STAGE: first": "first", "STAGE: second": "second"})

    response = transport.complete([{"role": "user", "content": "STAGE: second\nSTAGE: first"}], "x")

    assert response.text == "first"


def test_scripted_transport_reports_markers_and_request_on_miss() -> None:
    """教训 scripted_diagnosis: 漏配路由必须带可直接定位的请求片段。"""
    transport = ScriptedTransport({"STAGE: known": "ok"})

    with pytest.raises(AssertionError) as error:
        transport.complete([{"role": "user", "content": "STAGE: missing request"}], "x")

    assert "STAGE: known" in str(error.value)
    assert "STAGE: missing request" in str(error.value)


def test_scripted_transport_serializes_stateful_responders_under_concurrency() -> None:
    """教训 scripted_lock: responder 计数器不能在并发请求中串线。"""
    counter = 0

    def respond(text: str, model: str) -> str:
        nonlocal counter
        del text, model
        current = counter
        counter += 1
        return str(current)

    transport = ScriptedTransport({"STAGE: count": respond})
    threads = [
        threading.Thread(
            target=lambda: transport.complete([{"role": "user", "content": "STAGE: count"}], "x")
        )
        for _ in range(20)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert counter == 20
    assert len(transport.requests) == 20


def test_scripted_transport_aliases_make_missing_models_visible() -> None:
    """教训 scripted_aliases: fixture 的别名漏配不能回退为静默模型名。"""
    transport = ScriptedTransport({"STAGE: x": "ok"}, aliases={"default": "fixture-default"})

    assert transport.resolve("default") == "fixture-default"
    with pytest.raises(KeyError):
        transport.resolve("heavy")


class RecordingTransport:
    def __init__(self) -> None:
        self.calls = 0

    def resolve(self, model: str) -> str:
        return model

    def complete(
        self,
        messages: list[dict[str, Any]],
        model: str,
        **params: Any,
    ) -> Response:
        self.calls += 1
        return Response(
            "recorded",
            {"total_tokens": 2},
            "stop",
            "trace",
            model,
            model_observed=True,
        )


def test_cassette_replays_in_order_and_records_atomically(tmp_path: Path) -> None:
    """教训 malformed_tapes: 畸形模型响应必须可离线按原序重放。"""
    path = tmp_path / "responses.json"
    recorder = RecordingTransport()
    recording = CassetteTransport(path, record_with=recorder)

    assert recording.complete([], "default").text == "recorded"
    replay = CassetteTransport(path)

    replayed = replay.complete([], "default")
    assert replayed.reasoning == "trace"
    assert replayed.model_observed is True
    with pytest.raises(RuntimeError, match="Cassette exhausted"):
        replay.complete([], "default")
    assert recorder.calls == 1
    assert json.loads(path.read_text(encoding="utf-8"))[0]["text"] == "recorded"


def test_plugin_is_noop_without_kigumi_config(pytester: Any) -> None:
    """教训 no_op: 普通 pytest 项目不能得到 kigumi 生成项或 fixture 行为。"""
    pytester.makepyfile("def test_plain():\n    assert True\n")

    result = pytester.runpytest_subprocess("--collect-only", "-q")

    assert result.ret == 0
    result.stdout.fnmatch_lines(["test_*.py::test_plain"])
    assert "kigumi_dry_render" not in result.stdout.str()
    assert "kigumi_guard" not in result.stdout.str()


def test_plugin_collects_dry_render_and_fixture_when_active(pytester: Any) -> None:
    """教训 dry_render: 每个声明式模板在无模型调用时都必须可完整渲染。"""
    pytester.makefile(".toml", pyproject="[tool.kigumi]\nsource_dirs = []\n")
    prompts = pytester.mkdir("prompts")
    prompts.joinpath("ok.md").write_text("你好 {{name}}", encoding="utf-8")
    pytester.makepyfile(
        "def test_fixture(kigumi_cassette):\n    assert callable(kigumi_cassette)\n"
    )

    collected = pytester.runpytest_subprocess("--collect-only", "-q")
    result = pytester.runpytest_subprocess("-q")

    collected.stdout.fnmatch_lines(["*kigumi_dry_render*", "*kigumi_guard*"])
    result.assert_outcomes(passed=3)


def test_plugin_fails_bad_template_and_guard_violation(pytester: Any) -> None:
    """教训 guard_as_test: 坏槽位和循环裸调必须在测试阶段阻断。"""
    pytester.makefile(".toml", pyproject="[tool.kigumi]\nsource_dirs = ['src']\n")
    prompts = pytester.mkdir("prompts")
    prompts.joinpath("bad.md").write_text("{{BadSlot}}", encoding="utf-8")
    source = pytester.mkdir("src")
    source.joinpath("bad.py").write_text(
        "for item in items:\n    client.call([])\n",
        encoding="utf-8",
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(failed=2)
    result.stdout.fnmatch_lines(["*bad.py:2*"])


def test_plugin_warns_for_waived_guard_finding(pytester: Any) -> None:
    """教训 waiver_visibility: 豁免不是静默通过，测试输出必须可见。"""
    pytester.makefile(".toml", pyproject="[tool.kigumi]\nsource_dirs = ['src']\n")
    source = pytester.mkdir("src")
    source.joinpath("waived.py").write_text(
        "for item in items:\n    client.call([])  # kigumi: raw-llm-ok fixture replay\n",
        encoding="utf-8",
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1, warnings=1)
    result.stdout.fnmatch_lines(["*fixture replay*"])


def test_plugin_skips_live_tests_without_environment_flag(pytester: Any, monkeypatch: Any) -> None:
    """教训 live_budget: 未显式授权时 live 采样必须自动跳过。"""
    monkeypatch.delenv("KIGUMI_LIVE", raising=False)
    pytester.makefile(".toml", pyproject="[tool.kigumi]\nsource_dirs = []\n")
    pytester.makepyfile("import pytest\n@pytest.mark.live\ndef test_live():\n    assert True\n")

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1, skipped=1)


def test_plugin_guard_checks_only_decorated_raw_io_and_warns_for_its_waiver(
    pytester: Any,
) -> None:
    """教训 raw_io_guard_plugin: helper 读取合法，节点豁免须单独告警。"""
    pytester.makefile(".toml", pyproject="[tool.kigumi]\nsource_dirs = ['src']\n")
    source = pytester.mkdir("src")
    source.joinpath("nodes.py").write_text(
        """
def helper():
    return open("fixture.txt").read()

@dag.node("waived")
def waived(inputs, ctx):
    return open("fixture.txt").read()  # kigumi: raw-io-ok fixture setup
""",
        encoding="utf-8",
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1, warnings=1)
    result.stdout.fnmatch_lines(["*fixture setup*"])


def test_plugin_guard_reports_raw_io_in_decorated_node(pytester: Any) -> None:
    """教训 raw_io_guard_plugin: pytest 守卫必须挡住节点体的未声明读取。"""
    pytester.makefile(".toml", pyproject="[tool.kigumi]\nsource_dirs = ['src']\n")
    source = pytester.mkdir("src")
    source.joinpath("nodes.py").write_text(
        """
@pipeline.scan("items")
def scan(inputs, context):
    return Path("input.txt").read_text()
""",
        encoding="utf-8",
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(["*Raw file reads inside node functions:*"])


def test_cassette_rejects_replay_with_different_request(tmp_path: Path) -> None:
    """教训 tape_matching: 只按序重放会把换了顺序的调用静默配错答案。"""
    path = tmp_path / "responses.json"
    recording = CassetteTransport(path, record_with=RecordingTransport())
    recording.complete([{"role": "user", "content": "甲"}], "default")

    replay = CassetteTransport(path)
    with pytest.raises(RuntimeError, match="request mismatch"):
        replay.complete([{"role": "user", "content": "乙"}], "default")

    matched = CassetteTransport(path)
    assert matched.complete([{"role": "user", "content": "甲"}], "default").text == "recorded"


def test_cassette_rejects_legacy_tapes_without_request_sha(tmp_path: Path) -> None:
    """教训 tape_compat: 无指纹磁带必须报错重录,不得静默按序重放。"""
    path = tmp_path / "legacy.json"
    path.write_text(
        json.dumps(
            [
                {
                    "text": "legacy",
                    "usage": {},
                    "finish_reason": "stop",
                    "reasoning": None,
                    "model": "default",
                }
            ]
        ),
        encoding="utf-8",
    )

    replay = CassetteTransport(path)
    with pytest.raises(RuntimeError, match="re-record"):
        replay.complete([{"role": "user", "content": "任意"}], "default")


def test_skip_unless_env_lists_every_missing_environment_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """教训 live_env_gate: 任一真实请求凭证缺失都必须零成本跳过。"""
    monkeypatch.delenv("KIGUMI_TEST_PRESENT", raising=False)
    monkeypatch.delenv("KIGUMI_TEST_MISSING", raising=False)
    monkeypatch.setenv("KIGUMI_TEST_PRESENT", "configured")

    marker = skip_unless_env("KIGUMI_TEST_PRESENT", "KIGUMI_TEST_MISSING")

    assert marker.name == "skipif"
    assert marker.args == (True,)
    assert marker.kwargs["reason"] == "Missing required environment variables: KIGUMI_TEST_MISSING"


def test_skip_unless_env_does_not_skip_when_all_variables_are_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """教训 live_env_gate_ready: 已提供凭证时 helper 不得拦截 live 用例。"""
    monkeypatch.setenv("KIGUMI_TEST_FIRST", "yes")
    monkeypatch.setenv("KIGUMI_TEST_SECOND", "yes")

    marker = skip_unless_env("KIGUMI_TEST_FIRST", "KIGUMI_TEST_SECOND")

    assert marker.args == (False,)
    assert "Missing" not in marker.kwargs["reason"]


def test_skip_unless_env_requires_at_least_one_name() -> None:
    """教训 live_env_gate_contract: 空条件无法表达真实请求授权。"""
    with pytest.raises(ValueError, match="at least one"):
        skip_unless_env()
