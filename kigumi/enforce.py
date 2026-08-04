"""Pure AST checks for unsafe raw calls made inside node functions."""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

_WAIVER_PATTERN = re.compile(r"#\s*kigumi:\s*raw-llm-ok(?P<reason>.*?)\s*$")
_RAW_IO_WAIVER_PATTERN = re.compile(r"#\s*kigumi:\s*raw-io-ok(?P<reason>.*?)\s*$")
_DYNAMIC_BUILTIN_NAMES = frozenset({"eval", "exec", "__import__", "globals", "locals"})
_DYNAMIC_CALLABLE_NAMES = _DYNAMIC_BUILTIN_NAMES | {"getattr"}
_RAW_METHOD_NAMES = frozenset({"open", "read_text", "read_bytes"})


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
    """节点体内一次 raw read 或不可证明的动态执行及其豁免状态。"""

    path: Path
    lineno: int
    snippet: str
    waived: bool
    waiver_reason: str | None
    _col_offset: int = field(default=0, init=False, repr=False, compare=False)


class _CallableKind(Enum):
    """The only callable classifications the raw-I/O guard needs."""

    UNKNOWN = "unknown"
    RAW = "raw"
    DYNAMIC = "dynamic"
    OPAQUE = "opaque"


@dataclass
class _ScopeState:
    """Facts inherited while following one locally reachable execution scope."""

    functions: dict[str, ast.FunctionDef | ast.AsyncFunctionDef] = field(default_factory=dict)
    lambdas: dict[str, ast.Lambda] = field(default_factory=dict)
    classes: dict[str, ast.ClassDef] = field(default_factory=dict)
    raw_aliases: set[str] = field(default_factory=set)
    dynamic_aliases: set[str] = field(default_factory=set)
    builtin_module_aliases: set[str] = field(default_factory=lambda: {"builtins"})
    instance_aliases: dict[str, str] = field(default_factory=dict)
    method_aliases: dict[str, tuple[str, str]] = field(default_factory=dict)
    parameter_kinds: dict[str, _CallableKind] = field(default_factory=dict)


@dataclass
class _ScopeFacts(ast.NodeVisitor):
    """Definitions and binding facts collected from one lexical scope."""

    functions: dict[str, ast.FunctionDef | ast.AsyncFunctionDef] = field(default_factory=dict)
    classes: dict[str, ast.ClassDef] = field(default_factory=dict)
    lambdas: dict[str, ast.Lambda] = field(default_factory=dict)
    assignments: list[tuple[str, ast.expr]] = field(default_factory=list)
    bound_names: set[str] = field(default_factory=set)
    imported_raw_aliases: set[str] = field(default_factory=set)
    imported_dynamic_aliases: set[str] = field(default_factory=set)
    imported_builtin_module_aliases: set[str] = field(default_factory=set)
    deferred_unpackings: list[tuple[ast.AST, ast.expr]] = field(default_factory=list)
    deferred_iterations: list[tuple[ast.AST, ast.expr]] = field(default_factory=list)

    def visit(self, node: ast.AST) -> None:
        """Collect facts using the standard AST visitor protocol."""
        ast.NodeVisitor.visit(self, node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802 -- ast protocol.
        self.functions[node.name] = node
        self.bound_names.add(node.name)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802 -- ast protocol.
        self.functions[node.name] = node
        self.bound_names.add(node.name)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: N802 -- ast protocol.
        # The class body is a separate scope; it is scanned only if reached by
        # the node's execution path.
        self.classes[node.name] = node
        self.bound_names.add(node.name)

    def visit_Lambda(self, node: ast.Lambda) -> None:  # noqa: N802 -- ast protocol.
        # Lambda bodies are separate scopes and are handled by the raw-I/O
        # visitor only when the binding/callback is reachable.
        del node

    def visit_Assign(self, node: ast.Assign) -> None:  # noqa: N802 -- ast protocol.
        for target in node.targets:
            self.bound_names.update(_target_names(target))
            self._record_target_assignment(target, node.value)
        self.visit(node.value)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:  # noqa: N802 -- ast protocol.
        for name in _target_names(node.target):
            self.bound_names.add(name)
        if node.value is not None:
            self._record_target_assignment(node.target, node.value)
        if node.value is not None:
            self.visit(node.value)

    def visit_NamedExpr(self, node: ast.NamedExpr) -> None:  # noqa: N802 -- ast protocol.
        for name in _target_names(node.target):
            self.bound_names.add(name)
        self._record_target_assignment(node.target, node.value)
        self.visit(node.value)

    def visit_For(self, node: ast.For) -> None:  # noqa: N802 -- ast protocol.
        self.bound_names.update(_target_names(node.target))
        pairs = _iter_binding_pairs(node.target, node.iter)
        if pairs:
            self.assignments.extend(pairs)
        else:
            self.deferred_iterations.append((node.target, node.iter))
        self.visit(node.iter)
        for statement in [*node.body, *node.orelse]:
            self.visit(statement)

    def visit_AsyncFor(self, node: ast.AsyncFor) -> None:  # noqa: N802 -- ast protocol.
        self.bound_names.update(_target_names(node.target))
        pairs = _iter_binding_pairs(node.target, node.iter)
        if pairs:
            self.assignments.extend(pairs)
        else:
            self.deferred_iterations.append((node.target, node.iter))
        self.visit(node.iter)
        for statement in [*node.body, *node.orelse]:
            self.visit(statement)

    def visit_With(self, node: ast.With) -> None:  # noqa: N802 -- ast protocol.
        for item in node.items:
            self.visit(item.context_expr)
            if item.optional_vars is not None:
                self.bound_names.update(_target_names(item.optional_vars))
        for statement in node.body:
            self.visit(statement)

    def visit_AsyncWith(self, node: ast.AsyncWith) -> None:  # noqa: N802 -- ast protocol.
        self.visit_With(node)  # type: ignore[arg-type] -- same fields in the AST.

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:  # noqa: N802 -- ast protocol.
        if node.name is not None:
            self.bound_names.add(node.name)
        if node.type is not None:
            self.visit(node.type)
        for statement in node.body:
            self.visit(statement)

    def visit_Import(self, node: ast.Import) -> None:  # noqa: N802 -- ast protocol.
        for alias in node.names:
            local_name = alias.asname or alias.name.split(".")[0]
            self.bound_names.add(local_name)
            if alias.name == "builtins":
                self.imported_builtin_module_aliases.add(local_name)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:  # noqa: N802 -- ast protocol.
        for alias in node.names:
            local_name = alias.asname or alias.name
            self.bound_names.add(local_name)
            if node.module == "builtins" and alias.name == "open":
                self.imported_raw_aliases.add(local_name)
            elif node.module == "builtins" and alias.name in _DYNAMIC_CALLABLE_NAMES:
                self.imported_dynamic_aliases.add(local_name)

    def _record_target_assignment(self, target: ast.AST, value: ast.expr) -> None:
        pairs = _target_value_pairs(target, value)
        if pairs:
            for name, expression in pairs:
                self.assignments.append((name, expression))
                if isinstance(expression, ast.Lambda):
                    self.lambdas[name] = expression
        elif isinstance(target, (ast.Tuple, ast.List, ast.Starred)):
            self.deferred_unpackings.append((target, value))


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
    module_aliases = _collect_callable_aliases(
        _collect_scope_facts(tree.body),
        context_name=None,
        inherited_raw_aliases=None,
        inherited_dynamic_aliases=None,
        inherited_builtin_module_aliases=None,
        parameter_names=set(),
    )
    findings: list[RawIOFinding] = []
    for statement in tree.body:
        if not isinstance(statement, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        if not any(_is_node_decorator(decorator) for decorator in statement.decorator_list):
            continue
        visitor = _RawIOVisitor(path, lines, _last_parameter_name(statement.args))
        visitor.visit_function_body(
            statement,
            state=_ScopeState(
                raw_aliases=module_aliases.raw,
                dynamic_aliases=module_aliases.dynamic,
                builtin_module_aliases=module_aliases.builtin_modules,
            ),
        )
        findings.extend(visitor.findings)
    _sort_raw_io_findings(findings)
    return findings


def check_raw_io_source(
    text: str,
    path: Path,
    *,
    context_name: str = "ctx",
) -> list[RawIOFinding]:
    """找出节点及其可达局部 helper/lambda 中绕过上下文方法的文件读取。"""
    lines = text.splitlines()
    tree = ast.parse(text, filename=str(path))
    visitor = _RawIOVisitor(path, lines, context_name)
    visitor.visit(tree)
    _sort_raw_io_findings(visitor.findings)
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
    """Scan one node and the locally defined helpers it can reach.

    A plain ``NodeVisitor`` deliberately skips nested function bodies, which made
    a helper or lambda an easy way to hide a raw read.  This visitor first scans a
    function's own body, records local definitions, and then follows local names
    used by that body.  Unreferenced local helpers remain out of scope while
    direct lambda callbacks and nested helper calls are included.  Callable
    aliases and opaque callable expressions are handled by the same pass so a
    node cannot hide a raw read behind ``open_alias(...)`` or
    ``globals()[name](...)``.
    """

    def __init__(self, path: Path, lines: list[str], context_name: str | None) -> None:
        self.path = path
        self.lines = lines
        self.context_name = context_name
        self.findings: list[RawIOFinding] = []
        self._scanned_functions: set[int] = set()
        self._scanned_classes: set[int] = set()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802 -- ast protocol.
        self.visit_function_body(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802 -- ast protocol.
        self.visit_function_body(node)

    def visit_Module(self, node: ast.Module) -> None:  # noqa: N802 -- ast protocol.
        aliases = _collect_callable_aliases(
            _collect_scope_facts(node.body),
            context_name=self.context_name,
            inherited_raw_aliases=None,
            inherited_dynamic_aliases=None,
            inherited_builtin_module_aliases=None,
            parameter_names=set(),
        )
        for statement in node.body:
            if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)):
                self._scan_function(
                    statement,
                    state=_ScopeState(
                        raw_aliases=aliases.raw,
                        dynamic_aliases=aliases.dynamic,
                        builtin_module_aliases=aliases.builtin_modules,
                    ),
                    is_root=True,
                )

    def visit_function_body(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        *,
        state: _ScopeState | None = None,
    ) -> None:
        """Visit one node function and every locally reachable helper."""
        self._scan_function(
            node,
            state=state or _ScopeState(),
            is_root=True,
        )

    def _scan_function(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda,
        *,
        state: _ScopeState | None = None,
        is_root: bool = False,
    ) -> None:
        identity = id(node)
        if identity in self._scanned_functions:
            return
        self._scanned_functions.add(identity)
        state = state or _ScopeState()

        body: list[ast.stmt] | list[ast.expr] = (
            [node.body] if isinstance(node, ast.Lambda) else node.body
        )
        parameter_names = _parameter_names(node.args)
        context_name = (
            self.context_name if is_root or self.context_name not in parameter_names else None
        )
        instance_aliases = {
            name: class_name
            for name, class_name in state.instance_aliases.items()
            if name not in parameter_names
        }
        method_aliases = {
            name: method_reference
            for name, method_reference in state.method_aliases.items()
            if name not in parameter_names
        }

        facts = _collect_scope_facts(body)
        functions = dict(state.functions)
        functions.update(facts.functions)
        classes = dict(state.classes)
        classes.update(facts.classes)
        lambdas = dict(state.lambdas)
        lambdas.update(facts.lambdas)
        for name, expression in _expanded_callable_assignments(facts):
            if isinstance(expression, ast.Lambda):
                lambdas[name] = expression

        aliases = _collect_callable_aliases(
            facts,
            context_name=context_name,
            inherited_raw_aliases=state.raw_aliases,
            inherited_dynamic_aliases=state.dynamic_aliases,
            inherited_builtin_module_aliases=state.builtin_module_aliases,
            parameter_names=parameter_names,
            parameter_kinds=state.parameter_kinds,
        )
        direct = _DirectRawIOVisitor(
            self.path,
            self.lines,
            context_name,
            raw_aliases=aliases.raw,
            dynamic_aliases=aliases.dynamic,
            builtin_module_aliases=aliases.builtin_modules,
            instance_aliases=instance_aliases,
            method_aliases=method_aliases,
        )
        for statement in body:
            direct.visit(statement)
        self.findings.extend(direct.findings)

        reachable: set[ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda] = set(
            direct.referenced_lambdas
        )
        reachable_classes: set[ast.ClassDef] = set()
        for name in direct.referenced_names:
            function = functions.get(name)
            if function is not None:
                reachable.add(function)
            callback = lambdas.get(name)
            if callback is not None:
                reachable.add(callback)
            class_definition = classes.get(name)
            if class_definition is not None:
                reachable_classes.add(class_definition)
        pending = list(reachable)
        while pending:
            function = pending.pop()
            for default_lambda in direct.default_lambdas.get(id(function), ()):
                if default_lambda not in reachable:
                    reachable.add(default_lambda)
                    pending.append(default_lambda)
        child_state = _ScopeState(
            functions=functions,
            lambdas=lambdas,
            classes=classes,
            raw_aliases=aliases.raw,
            dynamic_aliases=aliases.dynamic,
            builtin_module_aliases=aliases.builtin_modules,
            instance_aliases=direct.instance_aliases,
            method_aliases=direct.method_aliases,
        )
        for function in reachable:
            function_name = (
                function.name
                if isinstance(function, ast.FunctionDef | ast.AsyncFunctionDef)
                else None
            )
            self._scan_function(
                function,
                state=_ScopeState(
                    functions=child_state.functions,
                    lambdas=child_state.lambdas,
                    classes=child_state.classes,
                    raw_aliases=child_state.raw_aliases,
                    dynamic_aliases=child_state.dynamic_aliases,
                    builtin_module_aliases=child_state.builtin_module_aliases,
                    instance_aliases=child_state.instance_aliases,
                    method_aliases=child_state.method_aliases,
                    parameter_kinds=_parameter_kinds_for_function(
                        function,
                        direct.call_arguments.get(function_name, ())
                        if function_name is not None
                        else (),
                        context_name=context_name,
                        raw_aliases=child_state.raw_aliases,
                        dynamic_aliases=child_state.dynamic_aliases,
                        builtin_module_aliases=child_state.builtin_module_aliases,
                    ),
                ),
            )
        for class_definition in reachable_classes:
            self._scan_class(
                class_definition,
                state=child_state,
                requested_methods=direct.referenced_class_methods.get(class_definition.name, set()),
            )

    def _scan_class(
        self,
        node: ast.ClassDef,
        *,
        state: _ScopeState,
        requested_methods: set[str],
    ) -> None:
        identity = id(node)
        if identity in self._scanned_classes:
            return
        self._scanned_classes.add(identity)

        body = node.body
        facts = _collect_scope_facts(body)
        classes = dict(state.classes)
        classes.update(facts.classes)
        aliases = _collect_callable_aliases(
            facts,
            context_name=self.context_name,
            inherited_raw_aliases=state.raw_aliases,
            inherited_dynamic_aliases=state.dynamic_aliases,
            inherited_builtin_module_aliases=state.builtin_module_aliases,
            parameter_names=set(),
            parameter_kinds={},
        )
        direct = _DirectRawIOVisitor(
            self.path,
            self.lines,
            self.context_name,
            raw_aliases=aliases.raw,
            dynamic_aliases=aliases.dynamic,
            builtin_module_aliases=aliases.builtin_modules,
            instance_aliases=state.instance_aliases,
            method_aliases=state.method_aliases,
        )
        for statement in body:
            direct.visit(statement)
        self.findings.extend(direct.findings)

        class_state = _ScopeState(
            functions=state.functions,
            lambdas=state.lambdas,
            classes=classes,
            raw_aliases=aliases.raw,
            dynamic_aliases=aliases.dynamic,
            builtin_module_aliases=aliases.builtin_modules,
            instance_aliases=direct.instance_aliases,
            method_aliases=direct.method_aliases,
        )
        methods = facts.functions
        for method_name in requested_methods:
            method = methods.get(method_name)
            if method is not None:
                self._scan_function(method, state=class_state)

        for class_name, class_definition in facts.classes.items():
            if class_name in direct.referenced_names:
                self._scan_class(
                    class_definition,
                    state=class_state,
                    requested_methods=direct.referenced_class_methods.get(class_name, set()),
                )


class _DirectRawIOVisitor(ast.NodeVisitor):
    """Scan one function body without entering nested function bodies."""

    def __init__(
        self,
        path: Path,
        lines: list[str],
        context_name: str | None,
        *,
        raw_aliases: set[str],
        dynamic_aliases: set[str],
        builtin_module_aliases: set[str],
        instance_aliases: dict[str, str] | None = None,
        method_aliases: dict[str, tuple[str, str]] | None = None,
    ) -> None:
        self.path = path
        self.lines = lines
        self.context_name = context_name
        self.raw_aliases = raw_aliases
        self.dynamic_aliases = dynamic_aliases
        self.builtin_module_aliases = builtin_module_aliases
        self.findings: list[RawIOFinding] = []
        self.referenced_names: set[str] = set()
        self.referenced_lambdas: set[ast.Lambda] = set()
        self.default_lambdas: dict[int, set[ast.Lambda]] = {}
        self.referenced_class_methods: dict[str, set[str]] = {}
        self.call_arguments: dict[str, list[_CallableCall]] = {}
        self._owned_opaque_subscripts: set[int] = set()
        self.instance_aliases = dict(instance_aliases or {})
        self.method_aliases = dict(method_aliases or {})
        self._visiting_call_target = False
        self._dynamic_call_target_owned = False

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802 -- ast protocol.
        self._record_default_lambdas(node)
        self._visit_defaults(node.args)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802 -- ast protocol.
        self._record_default_lambdas(node)
        self._visit_defaults(node.args)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: N802 -- ast protocol.
        # A class declaration creates a new execution/lookup scope that the
        # node contract does not permit.  This is structural, so raw-io waivers
        # cannot turn it back into an allowed node.
        self._append_structural_finding(node)
        del node

    def visit_Assign(self, node: ast.Assign) -> None:  # noqa: N802 -- ast protocol.
        for target in node.targets:
            self._record_assignment(target, node.value)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:  # noqa: N802 -- ast protocol.
        if node.value is not None:
            self._record_assignment(node.target, node.value)
        self.generic_visit(node)

    def visit_NamedExpr(self, node: ast.NamedExpr) -> None:  # noqa: N802 -- ast protocol.
        self._record_assignment(node.target, node.value)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802 -- ast protocol.
        if isinstance(node.func, ast.Name):
            self.call_arguments.setdefault(node.func.id, []).append(
                _CallableCall(
                    positional=tuple(
                        callable_kind
                        for argument in node.args
                        for callable_kind in self._classify_positional_argument(argument)
                    ),
                    keywords={
                        keyword.arg: self._classify_callable(_unwrap_starred(keyword.value))
                        for keyword in node.keywords
                        if keyword.arg is not None
                    },
                )
            )
        dynamic_call = self._is_dynamic_call(node)
        if dynamic_call:
            if not self._dynamic_call_target_owned:
                self._append_structural_finding(node)
        elif self._is_raw_read(node):
            self._append_raw_finding(node)

        self._record_class_method_reference(node.func)

        # A raw callable passed as a callback is still an execution path.  An
        # opaque callable expression is rejected even when it is only passed to
        # a higher-order function, because the AST cannot prove what it runs.
        for argument in self._callback_arguments(node):
            self._record_class_method_reference(argument)
            callable_kind = self._classify_callable(argument)
            if callable_kind is _CallableKind.RAW:
                self._append_raw_finding(argument)
            elif _is_hard_dynamic_kind(callable_kind):
                if _is_builtin_dict_lookup(argument, self.builtin_module_aliases):
                    self._owned_opaque_subscripts.add(id(argument))
                self._append_structural_finding(argument)

        if isinstance(node.func, ast.Lambda):
            self.referenced_lambdas.add(node.func)
        for argument in [*node.args, *(keyword.value for keyword in node.keywords)]:
            if isinstance(argument, ast.Lambda):
                self.referenced_lambdas.add(argument)

        # Visit the call target for helper/class reachability, but suppress its
        # bare dynamic-reference finding: the enclosing Call already owns the
        # single finding for ``globals()``/``builtins.globals()``/an alias call.
        previous_call_target_state = self._visiting_call_target
        previous_dynamic_call_target_state = self._dynamic_call_target_owned
        self._visiting_call_target = True
        self._dynamic_call_target_owned = previous_dynamic_call_target_state or dynamic_call
        try:
            self.visit(node.func)
        finally:
            self._visiting_call_target = previous_call_target_state
            self._dynamic_call_target_owned = previous_dynamic_call_target_state
        for argument in [*node.args, *(keyword.value for keyword in node.keywords)]:
            self.visit(argument)

    def visit_Lambda(self, node: ast.Lambda) -> None:  # noqa: N802 -- ast protocol.
        # Defaults execute when the lambda is created, even if its body is never
        # called.  The body itself remains a separate scope and is scanned only
        # when the lambda is reachable from an executed call/callback.
        self._record_default_lambdas(node)
        self._visit_defaults(node.args)

    def visit_Name(self, node: ast.Name) -> None:  # noqa: N802 -- ast protocol.
        if isinstance(node.ctx, ast.Load):
            self.referenced_names.add(node.id)
            if not self._visiting_call_target and _is_dynamic_callable_name(
                node.id, self.dynamic_aliases
            ):
                self._append_structural_finding(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:  # noqa: N802 -- ast protocol.
        self._record_class_method_reference(node)
        if not self._visiting_call_target and _is_dynamic_builtin_attribute(
            node, self.builtin_module_aliases
        ):
            self._append_structural_finding(node)
        self.generic_visit(node)

    def visit_Subscript(self, node: ast.Subscript) -> None:  # noqa: N802 -- ast protocol.
        if (
            not self._visiting_call_target
            and id(node) not in self._owned_opaque_subscripts
            and _is_builtin_dict_lookup(node, self.builtin_module_aliases)
        ):
            self._append_structural_finding(node)
        self.generic_visit(node)

    def _is_raw_read(self, node: ast.Call) -> bool:
        return self._classify_callable(node.func) is _CallableKind.RAW

    def _is_dynamic_call(self, node: ast.Call) -> bool:
        return _is_hard_dynamic_kind(
            self._classify_callable(
                node.func,
                prefer_dynamic_call=True,
            )
        )

    def _classify_callable(
        self,
        expression: ast.AST,
        *,
        prefer_dynamic_call: bool = False,
    ) -> _CallableKind:
        return _classify_callable_expression(
            expression,
            context_name=self.context_name,
            raw_aliases=self.raw_aliases,
            dynamic_aliases=self.dynamic_aliases,
            builtin_module_aliases=self.builtin_module_aliases,
            prefer_dynamic_call=prefer_dynamic_call,
        )

    def _classify_positional_argument(self, argument: ast.expr) -> tuple[_CallableKind, ...]:
        if isinstance(argument, ast.Starred):
            values = _sequence_elements(argument.value)
            if values is not None:
                return tuple(self._classify_callable(value) for value in values)
        return (self._classify_callable(_unwrap_starred(argument)),)

    def _callback_arguments(self, node: ast.Call) -> list[ast.expr]:
        """Return callback positions whose bodies execute during this call."""
        function = node.func
        if isinstance(function, ast.Name) and function.id in {"map", "filter"}:
            return list(node.args[:1])
        if isinstance(function, ast.Attribute) and function.attr in {"map", "filter"}:
            return list(node.args[:1])
        return []

    def _record_class_method_reference(self, expression: ast.AST) -> None:
        if isinstance(expression, ast.Attribute):
            method_reference = self._method_reference(expression)
            if method_reference is None:
                return
            class_name, method_name = method_reference
            self.referenced_class_methods.setdefault(class_name, set()).add(method_name)
        elif isinstance(expression, ast.Name):
            method_reference = self.method_aliases.get(expression.id)
            if method_reference is not None:
                class_name, method_name = method_reference
                self.referenced_class_methods.setdefault(class_name, set()).add(method_name)
                return
            self.referenced_class_methods.setdefault(expression.id, set()).add("__init__")

    def _record_assignment(self, target: ast.AST, value: ast.expr) -> None:
        for name in _target_names(target):
            self.instance_aliases.pop(name, None)
            self.method_aliases.pop(name, None)
            class_name = _constructor_class_name(value)
            if class_name is not None:
                self.instance_aliases[name] = class_name
                continue
            method_reference = self._method_reference(value)
            if method_reference is not None:
                self.method_aliases[name] = method_reference

    def _method_reference(self, expression: ast.AST) -> tuple[str, str] | None:
        if not isinstance(expression, ast.Attribute):
            return None
        receiver = expression.value
        if isinstance(receiver, ast.Name):
            class_name = self.instance_aliases.get(receiver.id, receiver.id)
        elif isinstance(receiver, ast.Call) and isinstance(receiver.func, ast.Name):
            class_name = receiver.func.id
        else:
            return None
        return class_name, expression.attr

    def _append_raw_finding(self, node: ast.AST) -> None:
        self.findings.append(self._finding(node, allow_waiver=True))

    def _append_structural_finding(self, node: ast.AST) -> None:
        self.findings.append(self._finding(node, allow_waiver=False))

    def _finding(self, node: ast.AST, *, allow_waiver: bool) -> RawIOFinding:
        line = self.lines[node.lineno - 1].strip()
        waiver = (
            _RAW_IO_WAIVER_PATTERN.search(self.lines[node.lineno - 1]) if allow_waiver else None
        )
        reason = waiver.group("reason").strip() if waiver else None
        finding = RawIOFinding(
            path=self.path,
            lineno=node.lineno,
            snippet=line,
            waived=bool(reason),
            waiver_reason=reason if reason else "豁免必须写理由" if waiver else None,
        )
        object.__setattr__(finding, "_col_offset", getattr(node, "col_offset", 0))
        return finding

    def _visit_defaults(self, arguments: ast.arguments) -> None:
        for default in [*arguments.defaults, *(item for item in arguments.kw_defaults if item)]:
            self.visit(default)

    def _record_default_lambdas(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda,
    ) -> None:
        defaults = [*node.args.defaults, *(item for item in node.args.kw_defaults if item)]
        lambdas = {
            candidate
            for default in defaults
            for candidate in ast.walk(default)
            if isinstance(candidate, ast.Lambda)
        }
        if lambdas:
            self.default_lambdas[id(node)] = lambdas


@dataclass(frozen=True)
class _CallableAliases:
    raw: set[str]
    dynamic: set[str]
    builtin_modules: set[str]


@dataclass(frozen=True)
class _CallableCall:
    positional: tuple[_CallableKind, ...]
    keywords: dict[str, _CallableKind]


def _parameter_kinds_for_function(
    node: ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda,
    calls: tuple[_CallableCall, ...] | list[_CallableCall],
    *,
    context_name: str | None,
    raw_aliases: set[str],
    dynamic_aliases: set[str],
    builtin_module_aliases: set[str],
) -> dict[str, _CallableKind]:
    """Bind callable facts from calls and statically known parameter defaults."""
    arguments = node.args
    positional_parameters = [*arguments.posonlyargs, *arguments.args]
    all_parameters = [*positional_parameters, *arguments.kwonlyargs]
    keyword_parameters = {parameter.arg for parameter in arguments.kwonlyargs}
    keyword_parameters.update(parameter.arg for parameter in positional_parameters)
    default_kinds: dict[str, _CallableKind] = {}
    result: dict[str, _CallableKind] = {}

    def record(name: str | None, callable_kind: _CallableKind) -> None:
        if name is None or callable_kind is _CallableKind.UNKNOWN:
            return
        result[name] = _merge_callable_kinds(result.get(name), callable_kind)

    def classify_default(expression: ast.expr) -> _CallableKind:
        return _classify_callable_expression(
            expression,
            context_name=context_name,
            raw_aliases=raw_aliases,
            dynamic_aliases=dynamic_aliases,
            builtin_module_aliases=builtin_module_aliases,
        )

    positional_defaults = list(arguments.defaults)
    if positional_defaults:
        for parameter, default in zip(
            positional_parameters[-len(positional_defaults) :], positional_defaults, strict=True
        ):
            callable_kind = classify_default(default)
            if callable_kind is not _CallableKind.UNKNOWN:
                default_kinds[parameter.arg] = callable_kind
    for parameter, default in zip(arguments.kwonlyargs, arguments.kw_defaults, strict=True):
        if default is not None:
            callable_kind = classify_default(default)
            if callable_kind is not _CallableKind.UNKNOWN:
                default_kinds[parameter.arg] = callable_kind

    if not calls:
        return default_kinds

    for call in calls:
        supplied: dict[str, _CallableKind] = {}

        for index, callable_kind in enumerate(call.positional):
            if index < len(positional_parameters):
                name = positional_parameters[index].arg
            elif arguments.vararg is not None:
                name = arguments.vararg.arg
            else:
                name = None
            if name is not None:
                supplied[name] = _merge_callable_kinds(supplied.get(name), callable_kind)
        for name, callable_kind in call.keywords.items():
            if name in keyword_parameters:
                parameter_name = name
            elif arguments.kwarg is not None:
                parameter_name = arguments.kwarg.arg
            else:
                parameter_name = None
            if parameter_name is not None:
                supplied[parameter_name] = _merge_callable_kinds(
                    supplied.get(parameter_name), callable_kind
                )

        for parameter in all_parameters:
            record(
                parameter.arg,
                supplied.get(
                    parameter.arg,
                    default_kinds.get(parameter.arg, _CallableKind.UNKNOWN),
                ),
            )
        if arguments.vararg is not None:
            record(arguments.vararg.arg, supplied.get(arguments.vararg.arg, _CallableKind.UNKNOWN))
        if arguments.kwarg is not None:
            record(arguments.kwarg.arg, supplied.get(arguments.kwarg.arg, _CallableKind.UNKNOWN))
    return result


def _merge_callable_kinds(
    current: _CallableKind | None,
    incoming: _CallableKind,
) -> _CallableKind:
    """Join call-site facts conservatively; a hard dynamic fact wins over raw."""
    if current is None or current is _CallableKind.UNKNOWN:
        return incoming
    if incoming is _CallableKind.UNKNOWN:
        return current
    if _is_hard_dynamic_kind(current) or _is_hard_dynamic_kind(incoming):
        if current is _CallableKind.OPAQUE or incoming is _CallableKind.OPAQUE:
            return _CallableKind.OPAQUE
        return _CallableKind.DYNAMIC
    return _CallableKind.RAW


def _unwrap_starred(expression: ast.AST) -> ast.AST:
    return expression.value if isinstance(expression, ast.Starred) else expression


def _collect_scope_facts(body: list[ast.stmt] | list[ast.expr]) -> _ScopeFacts:
    facts = _ScopeFacts()
    for statement in body:
        facts.visit(statement)
    return facts


def _collect_callable_aliases(
    facts: _ScopeFacts,
    *,
    context_name: str | None,
    inherited_raw_aliases: set[str] | None,
    inherited_dynamic_aliases: set[str] | None,
    inherited_builtin_module_aliases: set[str] | None,
    parameter_names: set[str],
    parameter_kinds: dict[str, _CallableKind] | None = None,
) -> _CallableAliases:
    raw = set(inherited_raw_aliases or ()) - facts.bound_names - parameter_names
    raw.update(facts.imported_raw_aliases)
    dynamic = set(inherited_dynamic_aliases or ()) - facts.bound_names - parameter_names
    dynamic.update(facts.imported_dynamic_aliases)
    dynamic.difference_update(raw)
    builtin_modules = set(inherited_builtin_module_aliases or {"builtins"})
    builtin_modules.difference_update(facts.bound_names)
    builtin_modules.difference_update(parameter_names)
    builtin_modules.update(facts.imported_builtin_module_aliases)
    for name, callable_kind in (parameter_kinds or {}).items():
        if name in facts.bound_names:
            continue
        if callable_kind is _CallableKind.RAW:
            raw.add(name)
            dynamic.discard(name)
        elif _is_hard_dynamic_kind(callable_kind):
            dynamic.add(name)
            raw.discard(name)
    assignments = _expanded_callable_assignments(facts)
    changed = True
    while changed:
        changed = False
        for name, expression in assignments:
            callable_kind = _classify_callable_expression(
                expression,
                context_name=context_name,
                raw_aliases=raw,
                dynamic_aliases=dynamic,
                builtin_module_aliases=builtin_modules,
            )
            if callable_kind is _CallableKind.RAW:
                if name not in raw or name in dynamic:
                    raw.add(name)
                    dynamic.discard(name)
                    changed = True
            elif _is_hard_dynamic_kind(callable_kind) and (name not in dynamic or name in raw):
                dynamic.add(name)
                raw.discard(name)
                changed = True
            if (
                _is_builtin_module_binding(expression, builtin_modules)
                and name not in builtin_modules
            ):
                builtin_modules.add(name)
                changed = True
    return _CallableAliases(raw=raw, dynamic=dynamic, builtin_modules=builtin_modules)


def _expanded_callable_assignments(facts: _ScopeFacts) -> list[tuple[str, ast.expr]]:
    """Expand statically resolvable sequence aliases before classifying bindings."""
    assignments = list(facts.assignments)
    seen = {_assignment_key(name, expression) for name, expression in assignments}
    sequence_aliases: dict[str, list[ast.expr]] = {}

    def add_assignment(name: str, expression: ast.expr) -> bool:
        key = _assignment_key(name, expression)
        if key in seen:
            return False
        seen.add(key)
        assignments.append((name, expression))
        return True

    changed = True
    while changed:
        changed = False
        for name, expression in assignments:
            element = _resolve_static_sequence_element(expression, sequence_aliases)
            if element is not None:
                changed |= add_assignment(name, element)
            values = _resolve_static_sequence(expression, sequence_aliases)
            if values is None:
                continue
            if name not in sequence_aliases:
                sequence_aliases[name] = values
                changed = True

        for target, value in facts.deferred_unpackings:
            values = _resolve_static_sequence(value, sequence_aliases)
            if values is None:
                continue
            for name, expression in _target_value_pairs_from_elements(target, values):
                changed |= add_assignment(name, expression)

        for target, iterable in facts.deferred_iterations:
            values = _resolve_static_sequence(iterable, sequence_aliases)
            if values is None:
                continue
            if isinstance(target, ast.Name):
                pairs = [(target.id, value) for value in values]
            else:
                pairs = [
                    pair
                    for value in values
                    for pair in _target_value_pairs_from_elements(target, [value])
                ]
            for name, expression in pairs:
                changed |= add_assignment(name, expression)

    return assignments


def _assignment_key(name: str, expression: ast.expr) -> tuple[str, str]:
    return name, ast.dump(expression, annotate_fields=True, include_attributes=False)


def _resolve_static_sequence(
    expression: ast.AST,
    sequence_aliases: dict[str, list[ast.expr]],
) -> list[ast.expr] | None:
    direct_values = _sequence_elements(expression)
    if direct_values is not None:
        return direct_values
    if isinstance(expression, ast.Name):
        return sequence_aliases.get(expression.id)
    return None


def _resolve_static_sequence_element(
    expression: ast.AST,
    sequence_aliases: dict[str, list[ast.expr]],
) -> ast.expr | None:
    if not isinstance(expression, ast.Subscript):
        return None
    values = _resolve_static_sequence(expression.value, sequence_aliases)
    if values is None or not isinstance(expression.slice, ast.Constant):
        return None
    index = expression.slice.value
    if not isinstance(index, int) or isinstance(index, bool):
        return None
    try:
        return values[index]
    except IndexError:
        return None


def _target_value_pairs_from_elements(
    target: ast.AST,
    values: list[ast.expr],
) -> list[tuple[str, ast.expr]]:
    if isinstance(target, ast.Name):
        return [(target.id, values[0])] if len(values) == 1 else []
    if isinstance(target, ast.Starred):
        return _target_value_pairs_from_elements(target.value, values)
    if not isinstance(target, (ast.Tuple, ast.List)):
        return []
    pairs: list[tuple[str, ast.expr]] = []
    for target_element, value_element in zip(target.elts, values, strict=False):
        nested_values = _sequence_elements(value_element)
        if isinstance(target_element, (ast.Tuple, ast.List)) and nested_values is not None:
            pairs.extend(_target_value_pairs_from_elements(target_element, nested_values))
        else:
            pairs.extend(_target_value_pairs_from_elements(target_element, [value_element]))
    return pairs


def _classify_callable_expression(
    expression: ast.AST,
    *,
    context_name: str | None,
    raw_aliases: set[str],
    dynamic_aliases: set[str],
    builtin_module_aliases: set[str],
    prefer_dynamic_call: bool = False,
) -> _CallableKind:
    """Classify a callable expression once for all raw-I/O decisions."""
    if (
        prefer_dynamic_call
        and isinstance(expression, ast.Call | ast.Subscript)
        and _is_dynamic_lookup_expression(
            expression,
            dynamic_aliases=dynamic_aliases,
            builtin_module_aliases=builtin_module_aliases,
        )
    ):
        return _CallableKind.DYNAMIC

    if isinstance(expression, ast.Name):
        if expression.id == "open" or expression.id in raw_aliases:
            return _CallableKind.RAW
        if _is_dynamic_callable_name(expression.id, dynamic_aliases):
            return _CallableKind.DYNAMIC
        return _CallableKind.UNKNOWN

    if isinstance(expression, ast.Attribute):
        if expression.attr in _RAW_METHOD_NAMES and not (
            isinstance(expression.value, ast.Name)
            and expression.value.id == context_name
            and expression.attr in {"read_text", "read_bytes"}
        ):
            return _CallableKind.RAW
        if _is_dynamic_builtin_attribute(expression, builtin_module_aliases):
            return _CallableKind.DYNAMIC
        return _CallableKind.UNKNOWN

    if isinstance(expression, ast.NamedExpr):
        callable_kind = _classify_callable_expression(
            expression.value,
            context_name=context_name,
            raw_aliases=raw_aliases,
            dynamic_aliases=dynamic_aliases,
            builtin_module_aliases=builtin_module_aliases,
            prefer_dynamic_call=False,
        )
        return callable_kind if callable_kind is _CallableKind.RAW else _CallableKind.UNKNOWN

    if isinstance(expression, ast.Call) and _is_raw_getattr_expression(expression):
        return _CallableKind.RAW

    if isinstance(expression, ast.Call | ast.Subscript) and _is_dynamic_lookup_expression(
        expression,
        dynamic_aliases=dynamic_aliases,
        builtin_module_aliases=builtin_module_aliases,
    ):
        return (
            _CallableKind.OPAQUE if isinstance(expression, ast.Subscript) else _CallableKind.DYNAMIC
        )

    return _CallableKind.UNKNOWN


def _is_raw_getattr_expression(expression: ast.Call) -> bool:
    if not (
        isinstance(expression.func, ast.Name)
        and expression.func.id == "getattr"
        and len(expression.args) >= 2
        and isinstance(expression.args[1], ast.Constant)
    ):
        return False
    method = expression.args[1].value
    return method in _RAW_METHOD_NAMES


def _is_hard_dynamic_kind(callable_kind: _CallableKind) -> bool:
    return callable_kind in {_CallableKind.DYNAMIC, _CallableKind.OPAQUE}


def _is_dynamic_lookup_expression(
    expression: ast.Call | ast.Subscript,
    *,
    dynamic_aliases: set[str],
    builtin_module_aliases: set[str],
) -> bool:
    if isinstance(expression, ast.Subscript):
        return _is_dynamic_lookup_call(
            expression.value,
            dynamic_aliases=dynamic_aliases,
            builtin_module_aliases=builtin_module_aliases,
        )
    return _is_dynamic_lookup_call(
        expression,
        dynamic_aliases=dynamic_aliases,
        builtin_module_aliases=builtin_module_aliases,
    )


def _is_dynamic_lookup_call(
    expression: ast.AST,
    *,
    dynamic_aliases: set[str],
    builtin_module_aliases: set[str],
) -> bool:
    """Recognize dynamic lookup producers without banning ordinary data reads."""
    if _is_builtin_dict_attribute(expression, builtin_module_aliases):
        return True
    if not isinstance(expression, ast.Call):
        return False
    if isinstance(expression.func, ast.Name):
        return _is_dynamic_callable_name(expression.func.id, dynamic_aliases)
    return _is_dynamic_builtin_attribute(expression.func, builtin_module_aliases)


def _is_dynamic_callable_name(name: str, dynamic_aliases: set[str]) -> bool:
    return name in _DYNAMIC_CALLABLE_NAMES or name in dynamic_aliases


def _is_dynamic_builtin_attribute(
    expression: ast.AST,
    builtin_module_aliases: set[str],
) -> bool:
    return (
        isinstance(expression, ast.Attribute)
        and isinstance(expression.value, ast.Name)
        and expression.value.id in builtin_module_aliases
        and expression.attr in _DYNAMIC_CALLABLE_NAMES
    )


def _is_builtin_dict_attribute(
    expression: ast.AST,
    builtin_module_aliases: set[str],
) -> bool:
    return (
        isinstance(expression, ast.Attribute)
        and isinstance(expression.value, ast.Name)
        and expression.value.id in builtin_module_aliases
        and expression.attr == "__dict__"
    )


def _is_builtin_dict_lookup(
    expression: ast.AST,
    builtin_module_aliases: set[str],
) -> bool:
    return isinstance(expression, ast.Subscript) and _is_builtin_dict_attribute(
        expression.value,
        builtin_module_aliases,
    )


def _is_builtin_module_binding(expression: ast.AST, builtin_module_aliases: set[str]) -> bool:
    return isinstance(expression, ast.Name) and expression.id in builtin_module_aliases


def _constructor_class_name(expression: ast.AST) -> str | None:
    """Return the syntactic class name for a simple ``Reader()`` binding."""
    if isinstance(expression, ast.Call) and isinstance(expression.func, ast.Name):
        return expression.func.id
    return None


def _parameter_names(arguments: ast.arguments) -> set[str]:
    return {
        parameter.arg
        for parameter in [
            *arguments.posonlyargs,
            *arguments.args,
            *arguments.kwonlyargs,
            *([arguments.vararg] if arguments.vararg else []),
            *([arguments.kwarg] if arguments.kwarg else []),
        ]
    }


def _sort_raw_io_findings(findings: list[RawIOFinding]) -> None:
    findings.sort(
        key=lambda finding: (
            finding.lineno,
            finding._col_offset,
            finding.snippet,
            finding.waived,
            finding.waiver_reason or "",
        )
    )


def _sequence_elements(expression: ast.AST) -> list[ast.expr] | None:
    if isinstance(expression, (ast.Tuple, ast.List, ast.Set)):
        return list(expression.elts)
    return None


def _target_value_pairs(target: ast.AST, value: ast.AST) -> list[tuple[str, ast.expr]]:
    if isinstance(target, ast.Name):
        return [(target.id, value)] if isinstance(value, ast.expr) else []
    if isinstance(target, ast.Starred):
        return _target_value_pairs(target.value, value)
    if not isinstance(target, (ast.Tuple, ast.List)):
        return []
    values = _sequence_elements(value)
    if values is None:
        return []
    pairs: list[tuple[str, ast.expr]] = []
    for target_element, value_element in zip(target.elts, values, strict=False):
        pairs.extend(_target_value_pairs(target_element, value_element))
    return pairs


def _iter_binding_pairs(target: ast.AST, iterable: ast.AST) -> list[tuple[str, ast.expr]]:
    values = _sequence_elements(iterable)
    if values is None:
        return []
    if isinstance(target, ast.Name):
        return [(target.id, value) for value in values]
    pairs: list[tuple[str, ast.expr]] = []
    for value in values:
        pairs.extend(_target_value_pairs(target, value))
    return pairs


def _target_names(target: ast.AST) -> set[str]:
    if isinstance(target, ast.Name):
        return {target.id}
    if isinstance(target, (ast.Tuple, ast.List)):
        names: set[str] = set()
        for element in target.elts:
            names.update(_target_names(element))
        return names
    if isinstance(target, ast.Starred):
        return _target_names(target.value)
    return set()


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
