"""Pure AST checks for unsafe raw calls made inside node functions."""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from pathlib import Path

_WAIVER_PATTERN = re.compile(r"#\s*kigumi:\s*raw-llm-ok(?P<reason>.*?)\s*$")
_RAW_IO_WAIVER_PATTERN = re.compile(r"#\s*kigumi:\s*raw-io-ok(?P<reason>.*?)\s*$")


@dataclass(frozen=True)
class Finding:
    """One loop-local raw LLM call and its optional source-line waiver."""

    path: Path
    lineno: int
    snippet: str
    waived: bool
    waiver_reason: str | None


@dataclass(frozen=True)
class RawIOFinding:
    """节点体内一次直接文件读取及其可选的行尾豁免。"""

    path: Path
    lineno: int
    snippet: str
    waived: bool
    waiver_reason: str | None


def waiver_reasons(text: str) -> list[str]:
    """Return every waiver reason text in *text*, in line order, including duplicates."""
    return [
        match.group("reason").strip()
        for line in text.splitlines()
        if (match := _WAIVER_PATTERN.search(line))
    ]


def raw_io_waiver_reasons(text: str) -> list[str]:
    """Return raw-I/O waiver reasons without mixing them with raw-LLM waivers."""
    return [
        match.group("reason").strip()
        for line in text.splitlines()
        if (match := _RAW_IO_WAIVER_PATTERN.search(line))
    ]


def check_source(text: str, path: Path) -> list[Finding]:
    """Find ``.call`` and ``.llm`` method calls nested beneath any loop."""
    lines = text.splitlines()
    tree = ast.parse(text, filename=str(path))
    visitor = _LoopCallVisitor(path, lines)
    visitor.visit(tree)
    return visitor.findings


def check_paths(source_dirs: list[Path]) -> list[Finding]:
    """Recursively check Python files in supplied directories, skipping absent paths."""
    findings: list[Finding] = []
    for source_dir in source_dirs:
        if not source_dir.is_dir():
            continue
        for path in sorted(source_dir.rglob("*.py")):
            findings.extend(check_source(path.read_text(encoding="utf-8"), path))
    return findings


def check_raw_io_node_paths(source_dirs: list[Path]) -> list[RawIOFinding]:
    """检查项目源码中带 DAG 装饰器的顶层节点函数体。

    项目级守卫无法拿到运行时注册表，只能用装饰器做保守筛选；注册环仍是精确权威来源。
    """
    findings: list[RawIOFinding] = []
    for source_dir in source_dirs:
        if not source_dir.is_dir():
            continue
        for path in sorted(source_dir.rglob("*.py")):
            findings.extend(check_raw_io_node_source(path.read_text(encoding="utf-8"), path))
    return findings


def check_raw_io_node_source(text: str, path: Path) -> list[RawIOFinding]:
    """检查一个模块内带 DAG 装饰器的顶层节点函数体。"""
    lines = text.splitlines()
    tree = ast.parse(text, filename=str(path))
    findings: list[RawIOFinding] = []
    for statement in tree.body:
        if not isinstance(statement, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        if not any(_is_node_decorator(decorator) for decorator in statement.decorator_list):
            continue
        visitor = _RawIOVisitor(path, lines, _last_parameter_name(statement.args))
        visitor.visit_function_body(statement)
        findings.extend(visitor.findings)
    return findings


def check_raw_io_source(
    text: str,
    path: Path,
    *,
    context_name: str = "ctx",
) -> list[RawIOFinding]:
    """找出一个节点函数体内绕过上下文方法的直接文件读取。

    只检查传入的节点函数体，不递归扫描其 helper；helper 可以在受控边界外合法读取文件。
    """
    lines = text.splitlines()
    tree = ast.parse(text, filename=str(path))
    visitor = _RawIOVisitor(path, lines, context_name)
    visitor.visit(tree)
    return visitor.findings


class _LoopCallVisitor(ast.NodeVisitor):
    def __init__(self, path: Path, lines: list[str]) -> None:
        self.path = path
        self.lines = lines
        self.loop_depth = 0
        self.findings: list[Finding] = []

    def visit_For(self, node: ast.For) -> None:  # noqa: N802 -- ast visitor protocol.
        self._visit_loop(node)

    def visit_While(self, node: ast.While) -> None:  # noqa: N802 -- ast visitor protocol.
        self._visit_loop(node)

    def visit_AsyncFor(self, node: ast.AsyncFor) -> None:  # noqa: N802 -- ast visitor protocol.
        self._visit_loop(node)

    # 推导式也是循环:[ctx.llm(p) for p in ...] 是最典型的绕行写法。
    def visit_ListComp(self, node: ast.ListComp) -> None:  # noqa: N802 -- ast visitor protocol.
        self._visit_loop(node)

    def visit_SetComp(self, node: ast.SetComp) -> None:  # noqa: N802 -- ast visitor protocol.
        self._visit_loop(node)

    def visit_DictComp(self, node: ast.DictComp) -> None:  # noqa: N802 -- ast visitor protocol.
        self._visit_loop(node)

    def visit_GeneratorExp(self, node: ast.GeneratorExp) -> None:  # noqa: N802 -- ast visitor protocol.
        self._visit_loop(node)

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802 -- ast visitor protocol.
        if (
            self.loop_depth
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in {"call", "llm"}
        ):
            snippet = self.lines[node.lineno - 1].strip()
            waiver = _WAIVER_PATTERN.search(self.lines[node.lineno - 1])
            reason = waiver.group("reason").strip() if waiver else None
            waiver_reason = reason if reason else "豁免必须写理由" if waiver else None
            self.findings.append(
                Finding(
                    path=self.path,
                    lineno=node.lineno,
                    snippet=snippet,
                    waived=bool(reason),
                    waiver_reason=waiver_reason,
                )
            )
        self.generic_visit(node)

    def _visit_loop(self, node: ast.AST) -> None:
        self.loop_depth += 1
        self.generic_visit(node)
        self.loop_depth -= 1


class _RawIOVisitor(ast.NodeVisitor):
    def __init__(self, path: Path, lines: list[str], context_name: str) -> None:
        self.path = path
        self.lines = lines
        self.context_name = context_name
        self.findings: list[RawIOFinding] = []
        self._function_depth = 0

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802 -- ast protocol.
        self._visit_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802 -- ast protocol.
        self._visit_function(node)

    def visit_Lambda(self, node: ast.Lambda) -> None:  # noqa: N802 -- ast protocol.
        # lambda 是 helper 的一种紧凑写法，节点守卫的边界同样不向其中扩展。
        if self._function_depth == 0:
            self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802 -- ast protocol.
        if self._function_depth and self._is_raw_read(node):
            snippet = self.lines[node.lineno - 1].strip()
            waiver = _RAW_IO_WAIVER_PATTERN.search(self.lines[node.lineno - 1])
            reason = waiver.group("reason").strip() if waiver else None
            self.findings.append(
                RawIOFinding(
                    path=self.path,
                    lineno=node.lineno,
                    snippet=snippet,
                    waived=bool(reason),
                    waiver_reason=reason if reason else "豁免必须写理由" if waiver else None,
                )
            )
        self.generic_visit(node)

    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        if self._function_depth:
            return
        self.visit_function_body(node)

    def visit_function_body(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        """Visit only the outer body of one known node function."""
        self._function_depth += 1
        for statement in node.body:
            self.visit(statement)
        self._function_depth -= 1

    def _is_raw_read(self, node: ast.Call) -> bool:
        if isinstance(node.func, ast.Name):
            return node.func.id == "open"
        if not isinstance(node.func, ast.Attribute):
            return False
        # path.open() 与 read_text/read_bytes 是同一类绕过;误报(如库对象的
        # open 方法)可用 raw-io-ok 豁免。
        if node.func.attr not in {"open", "read_text", "read_bytes"}:
            return False
        return not (
            isinstance(node.func.value, ast.Name) and node.func.value.id == self.context_name
        )


def _is_node_decorator(decorator: ast.expr) -> bool:
    """Return whether a decorator is one of the DAG node factory calls."""
    return (
        isinstance(decorator, ast.Call)
        and isinstance(decorator.func, ast.Attribute)
        and decorator.func.attr in {"node", "map", "scan", "foreach", "agent"}
    )


def _last_parameter_name(arguments: ast.arguments) -> str:
    """Mirror registration's last-signature-parameter context convention."""
    parameters = [*arguments.posonlyargs, *arguments.args]
    if arguments.vararg is not None:
        parameters.append(arguments.vararg)
    parameters.extend(arguments.kwonlyargs)
    if arguments.kwarg is not None:
        parameters.append(arguments.kwarg)
    return parameters[-1].arg if parameters else "ctx"
