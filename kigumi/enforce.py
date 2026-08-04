"""Pure AST checks for unsafe raw calls made inside node functions."""

from __future__ import annotations

import ast
import io
import re
import tokenize
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

_WAIVER_PATTERN = re.compile(r"#\s*kigumi:\s*raw-llm-ok(?=\s|$)(?P<reason>.*?)\s*$")
_RAW_IO_WAIVER_PATTERN = re.compile(r"#\s*kigumi:\s*raw-io-ok(?=\s|$)(?P<reason>.*?)\s*$")
_DYNAMIC_BUILTIN_NAMES = frozenset(
    {"eval", "exec", "__import__", "globals", "locals", "vars", "__builtins__"}
)
_DYNAMIC_CALLABLE_NAMES = _DYNAMIC_BUILTIN_NAMES | {"getattr"}
_LOOP_MODEL_METHOD_NAMES = frozenset({"call", "llm"})
_LOOP_HIGHER_ORDER_NAMES = frozenset({"map", "filter"})
_RAW_METHOD_NAMES = frozenset({"open", "read_text", "read_bytes"})
_DYNAMIC_DICT_METHOD_NAMES = frozenset({"get", "__getitem__"})


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


def _source_comments(text: str) -> dict[int, str]:
    """Return tokenizer-confirmed source comments keyed by physical line."""
    comments: dict[int, str] = {}
    try:
        tokens = tokenize.generate_tokens(io.StringIO(text).readline)
        for token in tokens:
            if token.type == tokenize.COMMENT:
                comments[token.start[0]] = token.string
    except (StopIteration, tokenize.TokenError):
        # Callers parse the source with ast first. This fallback keeps the
        # standalone waiver helpers best-effort for incomplete editor buffers
        # without treating string contents as comments.
        pass
    return comments


def _waiver_match(
    comments: dict[int, str],
    lineno: int,
    pattern: re.Pattern[str],
) -> re.Match[str] | None:
    comment = comments.get(lineno)
    return pattern.fullmatch(comment) if comment is not None else None


class _CallableKind(Enum):
    """The only callable classifications the raw-I/O guard needs."""

    UNKNOWN = "unknown"
    RAW = "raw"
    IMPORT = "import"
    DYNAMIC = "dynamic"
    OPAQUE = "opaque"


_StaticContainer = tuple[ast.expr, ...] | dict[object, ast.expr]


@dataclass
class _ScopeState:
    """Facts inherited while following one locally reachable execution scope."""

    functions: dict[str, ast.FunctionDef | ast.AsyncFunctionDef] = field(default_factory=dict)
    lambdas: dict[str, ast.Lambda] = field(default_factory=dict)
    classes: dict[str, ast.ClassDef] = field(default_factory=dict)
    raw_aliases: set[str] = field(default_factory=set)
    dynamic_aliases: set[str] = field(default_factory=set)
    builtin_module_aliases: set[str] = field(default_factory=lambda: {"builtins", "__builtins__"})
    instance_aliases: dict[str, str] = field(default_factory=dict)
    method_aliases: dict[str, tuple[str, str]] = field(default_factory=dict)
    parameter_kinds: dict[str, _CallableKind] = field(default_factory=dict)
    sequence_aliases: dict[str, _StaticContainer] = field(default_factory=dict)
    context_name: str | None = None
    import_aliases: set[str] = field(default_factory=set)


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
    imported_import_aliases: set[str] = field(default_factory=set)
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
            elif alias.name in {"importlib", "sys"}:
                self.imported_dynamic_aliases.add(local_name)
                # Keep imported module names out of the bare-name dynamic
                # finding path while retaining their dynamic lookup facts.
                self.imported_import_aliases.add(local_name)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:  # noqa: N802 -- ast protocol.
        for alias in node.names:
            local_name = alias.asname or alias.name
            self.bound_names.add(local_name)
            if node.module == "builtins" and alias.name == "open":
                self.imported_raw_aliases.add(local_name)
            elif node.module == "builtins" and alias.name in _DYNAMIC_CALLABLE_NAMES:
                self.imported_dynamic_aliases.add(local_name)
                if alias.name == "__import__":
                    self.imported_import_aliases.add(local_name)
            elif node.module == "importlib" and alias.name == "import_module":
                self.imported_import_aliases.add(local_name)
            elif node.module == "sys" and alias.name == "modules":
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
    comments = _source_comments(text)
    return [
        match.group("reason").strip()
        for lineno in sorted(comments)
        if (match := _waiver_match(comments, lineno, _WAIVER_PATTERN))
    ]


def raw_io_waiver_reasons(text: str) -> list[str]:
    """Return raw-I/O waiver reasons without mixing them with raw-LLM waivers."""
    comments = _source_comments(text)
    return [
        match.group("reason").strip()
        for lineno in sorted(comments)
        if (match := _waiver_match(comments, lineno, _RAW_IO_WAIVER_PATTERN))
    ]


def check_source(text: str, path: Path) -> list[Finding]:
    """Find ``.call`` and ``.llm`` method calls nested beneath any loop."""
    lines = text.splitlines()
    comments = _source_comments(text)
    tree = ast.parse(text, filename=str(path))
    visitor = _LoopCallVisitor(path, lines, comments, tree)
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
    comments = _source_comments(text)
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
        visitor = _RawIOVisitor(
            path,
            lines,
            _last_parameter_name(statement.args),
            comments,
        )
        visitor.visit_function_body(
            statement,
            state=_ScopeState(
                raw_aliases=module_aliases.raw,
                dynamic_aliases=module_aliases.dynamic,
                builtin_module_aliases=module_aliases.builtin_modules,
                sequence_aliases=module_aliases.sequence_aliases,
                import_aliases=module_aliases.import_aliases,
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
    comments = _source_comments(text)
    tree = ast.parse(text, filename=str(path))
    visitor = _RawIOVisitor(path, lines, context_name, comments)
    visitor.visit(tree)
    _sort_raw_io_findings(visitor.findings)
    return visitor.findings


class _LoopCallVisitor(ast.NodeVisitor):
    def __init__(
        self,
        path: Path,
        lines: list[str],
        comments: dict[int, str],
        tree: ast.AST,
    ) -> None:
        self.path = path
        self.lines = lines
        self.comments = comments
        self.loop_depth = 0
        self.callback_depth = 0
        self.findings: list[Finding] = []
        (
            self.model_aliases,
            self.model_callback_names,
        ) = _collect_loop_model_facts(tree)

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
        higher_order = self._is_model_higher_order_callback(node)
        if not self.callback_depth and (
            (self.loop_depth and self._is_model_call(node)) or higher_order
        ):
            self._append_finding(node)
        previous_callback_depth = self.callback_depth
        if higher_order:
            self.callback_depth += 1
        try:
            self.generic_visit(node)
        finally:
            self.callback_depth = previous_callback_depth

    def _is_model_call(self, node: ast.Call) -> bool:
        return _is_loop_model_callable(node.func, self.model_aliases)

    def _is_model_higher_order_callback(self, node: ast.Call) -> bool:
        if not (isinstance(node.func, ast.Name) and node.func.id in _LOOP_HIGHER_ORDER_NAMES):
            return False
        callback = _first_loop_callback_argument(node.args)
        if callback is None:
            return False
        if isinstance(callback, ast.Lambda):
            return _contains_loop_model_call(
                callback.body,
                self.model_aliases,
                self.model_callback_names,
            )
        return _is_loop_model_callable(callback, self.model_aliases) or (
            isinstance(callback, ast.Name) and callback.id in self.model_callback_names
        )

    def _append_finding(self, node: ast.AST) -> None:
        snippet = self.lines[node.lineno - 1].strip()
        waiver = _waiver_match(self.comments, node.lineno, _WAIVER_PATTERN)
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

    def _visit_loop(self, node: ast.AST) -> None:
        self.loop_depth += 1
        self.generic_visit(node)
        self.loop_depth -= 1


def _collect_loop_model_facts(tree: ast.AST) -> tuple[set[str], set[str]]:
    """Collect the small, syntactic model-call facts used by the loop guard.

    This intentionally proves only direct ``.call``/``.llm`` aliases, dynamic
    ``getattr`` aliases, and named callbacks whose bodies contain one of those
    calls.  It is a finite AST boundary, not an attempt to evaluate arbitrary
    Python callables.
    """
    assignments = [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.Assign, ast.AnnAssign, ast.NamedExpr))
    ]
    aliases: set[str] = set()
    changed = True
    while changed:
        changed = False
        for node in assignments:
            if isinstance(node, ast.Assign):
                targets = node.targets
                value = node.value
            else:
                targets = [node.target]
                value = node.value
            if value is None or not _is_loop_model_callable(value, aliases):
                continue
            for target in targets:
                for name in _target_names(target):
                    if name not in aliases:
                        aliases.add(name)
                        changed = True

    callback_names: set[str] = set()
    callback_bindings = [
        node for node in assignments if isinstance(node, ast.Assign | ast.AnnAssign)
    ]
    changed = True
    while changed:
        changed = False
        for node in ast.walk(tree):
            if (
                isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and _contains_loop_model_call(node.body, aliases, callback_names)
                and node.name not in callback_names
            ):
                callback_names.add(node.name)
                changed = True
        for node in callback_bindings:
            if isinstance(node, ast.Assign):
                targets = node.targets
                value = node.value
            else:
                targets = [node.target]
                value = node.value
            if not _contains_loop_model_call(value, aliases, callback_names):
                continue
            for target in targets:
                for name in _target_names(target):
                    if name not in callback_names:
                        callback_names.add(name)
                        changed = True
    return aliases, callback_names


def _contains_loop_model_call(
    node: ast.AST | list[ast.AST] | None,
    aliases: set[str],
    callback_names: set[str],
) -> bool:
    if node is None:
        return False
    roots = node if isinstance(node, list) else [node]
    return any(
        isinstance(candidate, ast.Call)
        and (
            _is_loop_model_callable(candidate.func, aliases)
            or (isinstance(candidate.func, ast.Name) and candidate.func.id in callback_names)
        )
        for root in roots
        for candidate in ast.walk(root)
    )


def _is_loop_model_callable(expression: ast.AST, aliases: set[str]) -> bool:
    if isinstance(expression, ast.Attribute):
        return expression.attr in _LOOP_MODEL_METHOD_NAMES
    if isinstance(expression, ast.Name):
        return expression.id in aliases
    return (
        isinstance(expression, ast.Call)
        and isinstance(expression.func, ast.Name)
        and expression.func.id == "getattr"
    )


def _first_loop_callback_argument(arguments: list[ast.expr]) -> ast.expr | None:
    if not arguments:
        return None
    callback = arguments[0]
    if isinstance(callback, ast.Starred) and isinstance(
        callback.value, (ast.List, ast.Tuple, ast.Set)
    ):
        return callback.value.elts[0] if callback.value.elts else None
    return callback


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

    def __init__(
        self,
        path: Path,
        lines: list[str],
        context_name: str | None,
        comments: dict[int, str],
    ) -> None:
        self.path = path
        self.lines = lines
        self.context_name = context_name
        self.comments = comments
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
                        sequence_aliases=aliases.sequence_aliases,
                        import_aliases=aliases.import_aliases,
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
        if is_root and not state.parameter_kinds:
            state.parameter_kinds.update(
                _parameter_kinds_for_function(
                    node,
                    (),
                    context_name=self.context_name,
                    raw_aliases=state.raw_aliases,
                    dynamic_aliases=state.dynamic_aliases,
                    builtin_module_aliases=state.builtin_module_aliases,
                    sequence_aliases=state.sequence_aliases,
                    import_aliases=state.import_aliases,
                )
            )

        body: list[ast.stmt] | list[ast.expr] = (
            [node.body] if isinstance(node, ast.Lambda) else node.body
        )
        parameter_names = _parameter_names(node.args)
        if is_root:
            context_name = self.context_name
        else:
            context_name = state.context_name if self.context_name not in parameter_names else None
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
        expanded_assignments, _ = _expanded_callable_bindings(
            facts,
            inherited_sequence_aliases=state.sequence_aliases,
        )
        for name, expression in expanded_assignments:
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
            inherited_sequence_aliases=state.sequence_aliases,
            inherited_import_aliases=state.import_aliases,
        )
        direct = _DirectRawIOVisitor(
            self.path,
            self.lines,
            context_name,
            self.comments,
            raw_aliases=aliases.raw,
            dynamic_aliases=aliases.dynamic,
            builtin_module_aliases=aliases.builtin_modules,
            instance_aliases=instance_aliases,
            method_aliases=method_aliases,
            sequence_aliases=aliases.sequence_aliases,
            import_aliases=aliases.import_aliases,
        )
        if is_root:
            # The module evaluates a top-level node's definition expressions
            # before the node can run: decorators, defaults and annotations
            # are part of the raw-I/O boundary too.
            direct._visit_definition_expressions(node)
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
            sequence_aliases=aliases.sequence_aliases,
            context_name=direct.context_name,
            import_aliases=aliases.import_aliases,
        )
        for function in reachable:
            function_name = _callable_binding_name(function, lambdas)
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
                    sequence_aliases=child_state.sequence_aliases,
                    context_name=child_state.context_name,
                    import_aliases=child_state.import_aliases,
                    parameter_kinds=_parameter_kinds_for_function(
                        function,
                        direct.call_arguments.get(function_name, ())
                        if function_name is not None
                        else (),
                        context_name=child_state.context_name,
                        raw_aliases=child_state.raw_aliases,
                        dynamic_aliases=child_state.dynamic_aliases,
                        builtin_module_aliases=child_state.builtin_module_aliases,
                        sequence_aliases=child_state.sequence_aliases,
                        import_aliases=child_state.import_aliases,
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
            inherited_sequence_aliases=state.sequence_aliases,
            inherited_import_aliases=state.import_aliases,
        )
        direct = _DirectRawIOVisitor(
            self.path,
            self.lines,
            self.context_name,
            self.comments,
            raw_aliases=aliases.raw,
            dynamic_aliases=aliases.dynamic,
            builtin_module_aliases=aliases.builtin_modules,
            instance_aliases=state.instance_aliases,
            method_aliases=state.method_aliases,
            sequence_aliases=aliases.sequence_aliases,
            import_aliases=aliases.import_aliases,
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
            sequence_aliases=aliases.sequence_aliases,
            context_name=direct.context_name,
            import_aliases=aliases.import_aliases,
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
        comments: dict[int, str],
        *,
        raw_aliases: set[str],
        dynamic_aliases: set[str],
        builtin_module_aliases: set[str],
        instance_aliases: dict[str, str] | None = None,
        method_aliases: dict[str, tuple[str, str]] | None = None,
        sequence_aliases: dict[str, _StaticContainer] | None = None,
        import_aliases: set[str] | None = None,
    ) -> None:
        self.path = path
        self.lines = lines
        self.context_name = context_name
        self.comments = comments
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
        self.sequence_aliases = dict(sequence_aliases or {})
        self.import_aliases = set(import_aliases or ())
        self._visiting_call_target = False
        self._dynamic_call_target_owned = False
        self._opaque_subscript_target_owned = False

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802 -- ast protocol.
        self._record_default_lambdas(node)
        self._visit_definition_expressions(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802 -- ast protocol.
        self._record_default_lambdas(node)
        self._visit_definition_expressions(node)

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
        for target in node.targets:
            self._invalidate_context_target(target)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:  # noqa: N802 -- ast protocol.
        if node.value is not None:
            self._record_assignment(node.target, node.value)
        self.generic_visit(node)
        self._invalidate_context_target(node.target)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:  # noqa: N802 -- ast protocol.
        self.generic_visit(node)
        self._invalidate_context_target(node.target)

    def visit_NamedExpr(self, node: ast.NamedExpr) -> None:  # noqa: N802 -- ast protocol.
        self._record_assignment(node.target, node.value)
        self.generic_visit(node)
        self._invalidate_context_target(node.target)

    def visit_ListComp(self, node: ast.ListComp) -> None:  # noqa: N802 -- ast protocol.
        self._visit_comprehension(node.generators, (node.elt,))

    def visit_SetComp(self, node: ast.SetComp) -> None:  # noqa: N802 -- ast protocol.
        self._visit_comprehension(node.generators, (node.elt,))

    def visit_GeneratorExp(self, node: ast.GeneratorExp) -> None:  # noqa: N802 -- ast protocol.
        self._visit_comprehension(node.generators, (node.elt,))

    def visit_DictComp(self, node: ast.DictComp) -> None:  # noqa: N802 -- ast protocol.
        self._visit_comprehension(node.generators, (node.key, node.value))

    def _visit_comprehension(
        self,
        generators: list[ast.comprehension],
        tail: tuple[ast.expr, ...],
    ) -> None:
        previous_context_name = self.context_name
        try:
            for generator in generators:
                self.visit(generator.iter)
                self._invalidate_context_target(generator.target)
                for condition in generator.ifs:
                    self.visit(condition)
            for expression in tail:
                self.visit(expression)
        finally:
            # Comprehension targets live in their own implicit scope and do not
            # rebind the node context after the comprehension completes.
            self.context_name = previous_context_name

    def visit_Match(self, node: ast.Match) -> None:  # noqa: N802 -- ast protocol.
        self.visit(node.subject)
        outer_context_name = self.context_name
        context_rebound = False
        for case in node.cases:
            self.context_name = outer_context_name
            self.visit(case.pattern)
            if outer_context_name in _pattern_bound_names(case.pattern):
                self.context_name = None
            if case.guard is not None:
                self.visit(case.guard)
            for statement in case.body:
                self.visit(statement)
            context_rebound |= self.context_name is None
        # Match bindings are visible after the match statement, so any case
        # that can bind the context invalidates the controlled-read exemption.
        self.context_name = None if context_rebound else outer_context_name

    def visit_For(self, node: ast.For) -> None:  # noqa: N802 -- ast protocol.
        self.visit(node.iter)
        self._invalidate_context_target(node.target)
        for statement in [*node.body, *node.orelse]:
            self.visit(statement)

    def visit_AsyncFor(self, node: ast.AsyncFor) -> None:  # noqa: N802 -- ast protocol.
        self.visit(node.iter)
        self._invalidate_context_target(node.target)
        for statement in [*node.body, *node.orelse]:
            self.visit(statement)

    def visit_With(self, node: ast.With) -> None:  # noqa: N802 -- ast protocol.
        for item in node.items:
            self.visit(item.context_expr)
            if item.optional_vars is not None:
                self._invalidate_context_target(item.optional_vars)
        for statement in node.body:
            self.visit(statement)

    def visit_AsyncWith(self, node: ast.AsyncWith) -> None:  # noqa: N802 -- ast protocol.
        self.visit_With(node)  # type: ignore[arg-type] -- same fields in the AST.

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:  # noqa: N802 -- ast protocol.
        if node.type is not None:
            self.visit(node.type)
        if node.name is not None:
            self._invalidate_context_target(ast.Name(id=node.name, ctx=ast.Store()))
        for statement in node.body:
            self.visit(statement)

    def visit_Import(self, node: ast.Import) -> None:  # noqa: N802 -- ast protocol.
        self.generic_visit(node)
        for alias in node.names:
            self._invalidate_context_target(
                ast.Name(id=alias.asname or alias.name.split(".")[0], ctx=ast.Store())
            )

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:  # noqa: N802 -- ast protocol.
        self.generic_visit(node)
        for alias in node.names:
            self._invalidate_context_target(
                ast.Name(id=alias.asname or alias.name, ctx=ast.Store())
            )

    def visit_Delete(self, node: ast.Delete) -> None:  # noqa: N802 -- ast protocol.
        self.generic_visit(node)
        for target in node.targets:
            self._invalidate_context_target(target)

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802 -- ast protocol.
        if isinstance(node.func, ast.Name):
            self.call_arguments.setdefault(node.func.id, []).append(
                _CallableCall(
                    positional=tuple(
                        callable_kind
                        for argument in self._expanded_positional_arguments(node.args)
                        for callable_kind in self._classify_positional_argument(argument)
                    ),
                    keywords={
                        name: self._classify_callable(_unwrap_starred(value))
                        for name, value in self._expanded_keyword_arguments(node.keywords)
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
                if isinstance(argument, ast.Subscript):
                    self._owned_opaque_subscripts.add(id(argument))
                self._append_structural_finding(argument)

        for expression in [
            node.func,
            *node.args,
            *(keyword.value for keyword in node.keywords),
        ]:
            for callable_expression in self._static_callable_expressions(expression):
                if isinstance(callable_expression, ast.Lambda):
                    self.referenced_lambdas.add(callable_expression)
        for keyword in node.keywords:
            if keyword.arg is None:
                for _, value in self._expanded_keyword_arguments([keyword]):
                    for callable_expression in self._static_callable_expressions(value):
                        if isinstance(callable_expression, ast.Lambda):
                            self.referenced_lambdas.add(callable_expression)

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
            if (
                not self._visiting_call_target
                and not self._opaque_subscript_target_owned
                and node.id not in self.import_aliases
                and _is_dynamic_callable_name(node.id, self.dynamic_aliases)
            ):
                self._append_structural_finding(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:  # noqa: N802 -- ast protocol.
        self._record_class_method_reference(node)
        if not self._visiting_call_target and (
            _is_dynamic_builtin_attribute(node, self.builtin_module_aliases)
            or _is_dynamic_builtin_namespace_attribute(node, self.builtin_module_aliases)
            or _is_dynamic_dict_lookup_attribute(
                node,
                dynamic_aliases=self.dynamic_aliases,
                builtin_module_aliases=self.builtin_module_aliases,
            )
        ):
            self._append_structural_finding(node)
        self.generic_visit(node)

    def visit_Subscript(self, node: ast.Subscript) -> None:  # noqa: N802 -- ast protocol.
        hard_dynamic = _is_hard_dynamic_kind(self._classify_callable(node))
        if (
            not self._visiting_call_target
            and id(node) not in self._owned_opaque_subscripts
            and hard_dynamic
            and not (
                isinstance(node.value, ast.Call)
                and _is_dynamic_lookup_call(
                    node.value,
                    dynamic_aliases=self.dynamic_aliases,
                    builtin_module_aliases=self.builtin_module_aliases,
                )
            )
        ):
            self._append_structural_finding(node)
        previous_opaque_subscript_state = self._opaque_subscript_target_owned
        self._opaque_subscript_target_owned = previous_opaque_subscript_state or hard_dynamic
        try:
            self.generic_visit(node)
        finally:
            self._opaque_subscript_target_owned = previous_opaque_subscript_state

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
            sequence_aliases=self.sequence_aliases,
            import_aliases=self.import_aliases,
            prefer_dynamic_call=prefer_dynamic_call,
        )

    def _classify_positional_argument(self, argument: ast.expr) -> tuple[_CallableKind, ...]:
        if isinstance(argument, ast.Starred):
            values = _resolve_static_sequence(argument.value, self.sequence_aliases)
            if values is not None:
                return tuple(self._classify_callable(value) for value in values)
        return (self._classify_callable(_unwrap_starred(argument)),)

    def _expanded_keyword_arguments(
        self,
        keywords: list[ast.keyword],
    ) -> list[tuple[str, ast.expr]]:
        expanded: list[tuple[str, ast.expr]] = []
        for keyword in keywords:
            if keyword.arg is not None:
                expanded.append((keyword.arg, keyword.value))
                continue
            values = _resolve_static_mapping(keyword.value, self.sequence_aliases)
            if values is not None:
                expanded.extend(
                    (key, value) for key, value in values.items() if isinstance(key, str)
                )
        return expanded

    def _callback_arguments(self, node: ast.Call) -> list[ast.expr]:
        """Return callback positions whose bodies execute during this call."""
        function = node.func
        if isinstance(function, ast.Name) and function.id in {"map", "filter"}:
            return self._expanded_positional_arguments(node.args)[:1]
        if isinstance(function, ast.Attribute) and function.attr in {"map", "filter"}:
            return self._expanded_positional_arguments(node.args)[:1]
        return []

    def _expanded_positional_arguments(self, arguments: list[ast.expr]) -> list[ast.expr]:
        expanded: list[ast.expr] = []
        for argument in arguments:
            if isinstance(argument, ast.Starred):
                values = _resolve_static_sequence(argument.value, self.sequence_aliases)
                if values is not None:
                    expanded.extend(values)
                    continue
            expanded.append(argument)
        return expanded

    def _static_callable_expressions(self, expression: ast.AST) -> list[ast.expr]:
        if isinstance(expression, ast.Starred):
            values = _resolve_static_sequence(expression.value, self.sequence_aliases)
            return [
                callable_expression
                for value in values or ()
                for callable_expression in self._static_callable_expressions(value)
            ]
        if isinstance(expression, ast.Lambda):
            return [expression]
        if isinstance(expression, ast.Subscript):
            element = _resolve_static_container_element(expression, self.sequence_aliases)
            if element is not None:
                return self._static_callable_expressions(element)
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
            _waiver_match(self.comments, node.lineno, _RAW_IO_WAIVER_PATTERN)
            if allow_waiver
            else None
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

    def _visit_definition_expressions(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
    ) -> None:
        """Visit expressions evaluated while a nested function is defined."""
        for decorator in node.decorator_list:
            self.visit(decorator)
        self._visit_defaults(node.args)
        annotations = [
            parameter.annotation
            for parameter in [
                *node.args.posonlyargs,
                *node.args.args,
                *node.args.kwonlyargs,
                *([node.args.vararg] if node.args.vararg is not None else []),
                *([node.args.kwarg] if node.args.kwarg is not None else []),
            ]
            if parameter.annotation is not None
        ]
        annotations.extend(annotation for annotation in [node.returns] if annotation is not None)
        for annotation in annotations:
            self.visit(annotation)
        self._invalidate_context_target(ast.Name(id=node.name, ctx=ast.Store()))

    def _visit_defaults(self, arguments: ast.arguments) -> None:
        for default in [*arguments.defaults, *(item for item in arguments.kw_defaults if item)]:
            self.visit(default)

    def _invalidate_context_target(self, target: ast.AST) -> None:
        if self.context_name is not None and self.context_name in _target_names(target):
            self.context_name = None

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
    sequence_aliases: dict[str, _StaticContainer]
    import_aliases: set[str]


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
    sequence_aliases: dict[str, _StaticContainer] | None = None,
    import_aliases: set[str] | None = None,
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
            sequence_aliases=sequence_aliases or {},
            import_aliases=import_aliases or set(),
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
    if current is _CallableKind.IMPORT and incoming is _CallableKind.IMPORT:
        return _CallableKind.IMPORT
    if current is _CallableKind.IMPORT or incoming is _CallableKind.IMPORT:
        if current is _CallableKind.OPAQUE or incoming is _CallableKind.OPAQUE:
            return _CallableKind.OPAQUE
        return _CallableKind.DYNAMIC
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
    inherited_sequence_aliases: dict[str, _StaticContainer] | None = None,
    inherited_import_aliases: set[str] | None = None,
) -> _CallableAliases:
    raw = set(inherited_raw_aliases or ()) - facts.bound_names - parameter_names
    raw.update(facts.imported_raw_aliases)
    dynamic = set(inherited_dynamic_aliases or ()) - facts.bound_names - parameter_names
    import_aliases = set(inherited_import_aliases or ()) - facts.bound_names - parameter_names
    import_aliases.update(facts.imported_import_aliases)
    dynamic.update(import_aliases)
    dynamic.update(facts.imported_dynamic_aliases)
    dynamic.difference_update(raw)
    builtin_modules = set(inherited_builtin_module_aliases or {"builtins", "__builtins__"})
    builtin_modules.difference_update(facts.bound_names)
    builtin_modules.difference_update(parameter_names)
    builtin_modules.update(facts.imported_builtin_module_aliases)
    for name, callable_kind in (parameter_kinds or {}).items():
        if name in facts.bound_names:
            continue
        if callable_kind is _CallableKind.RAW:
            raw.add(name)
            dynamic.discard(name)
            import_aliases.discard(name)
        elif callable_kind is _CallableKind.IMPORT:
            import_aliases.add(name)
            dynamic.add(name)
            raw.discard(name)
        elif _is_hard_dynamic_kind(callable_kind):
            dynamic.add(name)
            raw.discard(name)
            import_aliases.discard(name)
    inherited_sequences: dict[str, _StaticContainer] = {
        name: values
        for name, values in (inherited_sequence_aliases or {}).items()
        if name not in facts.bound_names and name not in parameter_names
    }
    effective_context_name = (
        None if context_name is not None and context_name in facts.bound_names else context_name
    )
    assignments, sequence_aliases = _expanded_callable_bindings(
        facts,
        inherited_sequence_aliases=inherited_sequences,
    )
    changed = True
    while changed:
        changed = False
        for name, expression in assignments:
            callable_kind = _classify_callable_expression(
                expression,
                context_name=effective_context_name,
                raw_aliases=raw,
                dynamic_aliases=dynamic,
                builtin_module_aliases=builtin_modules,
                sequence_aliases=sequence_aliases,
                import_aliases=import_aliases,
            )
            if callable_kind is _CallableKind.RAW:
                if name not in raw or name in dynamic:
                    raw.add(name)
                    dynamic.discard(name)
                    import_aliases.discard(name)
                    changed = True
            elif callable_kind is _CallableKind.IMPORT:
                if name not in import_aliases or name not in dynamic:
                    import_aliases.add(name)
                    dynamic.add(name)
                    raw.discard(name)
                    changed = True
            elif _is_hard_dynamic_kind(callable_kind) and (name not in dynamic or name in raw):
                dynamic.add(name)
                raw.discard(name)
                import_aliases.discard(name)
                changed = True
            if (
                _is_builtin_module_binding(expression, builtin_modules)
                and name not in builtin_modules
            ):
                builtin_modules.add(name)
                changed = True
    return _CallableAliases(
        raw=raw,
        dynamic=dynamic,
        builtin_modules=builtin_modules,
        sequence_aliases=sequence_aliases,
        import_aliases=import_aliases,
    )


def _expanded_callable_assignments(facts: _ScopeFacts) -> list[tuple[str, ast.expr]]:
    """Expand statically resolvable sequence aliases before classifying bindings."""
    assignments, _ = _expanded_callable_bindings(facts)
    return assignments


def _expanded_callable_bindings(
    facts: _ScopeFacts,
    *,
    inherited_sequence_aliases: dict[str, _StaticContainer] | None = None,
) -> tuple[list[tuple[str, ast.expr]], dict[str, _StaticContainer]]:
    """Return callable assignments plus statically proven sequence/dict bindings."""
    assignments = list(facts.assignments)
    seen = {_assignment_key(name, expression) for name, expression in assignments}
    sequence_aliases: dict[str, _StaticContainer] = {
        name: values
        for name, values in (inherited_sequence_aliases or {}).items()
        if name not in facts.bound_names
    }

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
            element = _resolve_static_container_element(expression, sequence_aliases)
            if element is not None:
                changed |= add_assignment(name, element)
            values = _resolve_static_container(expression, sequence_aliases)
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

    # A local name with any unresolved reassignment is not a static container.
    # This keeps ``unknown[index]`` unknown instead of turning one earlier
    # literal assignment into an unbounded false positive.
    for name in set(facts.bound_names):
        expressions = [expression for binding, expression in assignments if binding == name]
        if expressions and any(
            _resolve_static_container(expression, sequence_aliases) is None
            for expression in expressions
        ):
            sequence_aliases.pop(name, None)

    return assignments, sequence_aliases


def _assignment_key(name: str, expression: ast.expr) -> tuple[str, str]:
    return name, ast.dump(expression, annotate_fields=True, include_attributes=False)


def _resolve_static_sequence(
    expression: ast.AST,
    sequence_aliases: dict[str, _StaticContainer],
) -> list[ast.expr] | None:
    direct_values = _sequence_elements(expression)
    if direct_values is not None:
        return direct_values
    if isinstance(expression, ast.Name):
        values = sequence_aliases.get(expression.id)
        return list(values) if isinstance(values, tuple) else None
    return None


def _resolve_static_mapping(
    expression: ast.AST,
    sequence_aliases: dict[str, _StaticContainer],
) -> dict[object, ast.expr] | None:
    direct_values = _mapping_elements(expression, sequence_aliases)
    if direct_values is not None:
        return direct_values
    if isinstance(expression, ast.Name):
        values = sequence_aliases.get(expression.id)
        return dict(values) if isinstance(values, dict) else None
    return None


def _resolve_static_container(
    expression: ast.AST,
    sequence_aliases: dict[str, _StaticContainer],
) -> _StaticContainer | None:
    sequence = _resolve_static_sequence(expression, sequence_aliases)
    if sequence is not None:
        return tuple(sequence)
    return _resolve_static_mapping(expression, sequence_aliases)


def _resolve_static_sequence_element(
    expression: ast.AST,
    sequence_aliases: dict[str, _StaticContainer],
) -> ast.expr | None:
    if not isinstance(expression, ast.Subscript):
        return None
    values = _resolve_static_sequence(expression.value, sequence_aliases)
    if values is None:
        return None
    is_static, index = _static_subscript_key(expression.slice)
    if not is_static or not isinstance(index, int) or isinstance(index, bool):
        return None
    try:
        return values[index]
    except IndexError:
        return None


def _resolve_static_mapping_element(
    expression: ast.AST,
    sequence_aliases: dict[str, _StaticContainer],
) -> ast.expr | None:
    if not isinstance(expression, ast.Subscript):
        return None
    values = _resolve_static_mapping(expression.value, sequence_aliases)
    if values is None:
        return None
    is_static, key = _static_subscript_key(expression.slice)
    if not is_static:
        return None
    return values.get(key)


def _resolve_static_container_element(
    expression: ast.AST,
    sequence_aliases: dict[str, _StaticContainer],
) -> ast.expr | None:
    return _resolve_static_sequence_element(
        expression,
        sequence_aliases,
    ) or _resolve_static_mapping_element(expression, sequence_aliases)


def _static_subscript_key(expression: ast.AST) -> tuple[bool, object]:
    if isinstance(expression, ast.Constant):
        try:
            hash(expression.value)
        except TypeError:
            return False, None
        return True, expression.value
    if (
        isinstance(expression, ast.UnaryOp)
        and isinstance(expression.op, (ast.USub, ast.UAdd))
        and isinstance(expression.operand, ast.Constant)
    ):
        value = expression.operand.value
        if isinstance(value, int) and not isinstance(value, bool):
            return True, -value if isinstance(expression.op, ast.USub) else value
    return False, None


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
    sequence_aliases: dict[str, _StaticContainer],
    import_aliases: set[str],
    prefer_dynamic_call: bool = False,
) -> _CallableKind:
    """Classify a callable expression once for all raw-I/O decisions."""
    if _is_import_callable_expression(
        expression,
        import_aliases=import_aliases,
        dynamic_aliases=dynamic_aliases,
        builtin_module_aliases=builtin_module_aliases,
    ) or (
        isinstance(expression, ast.Call)
        and _is_import_callable_expression(
            expression.func,
            import_aliases=import_aliases,
            dynamic_aliases=dynamic_aliases,
            builtin_module_aliases=builtin_module_aliases,
        )
    ):
        return _CallableKind.IMPORT
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

    if isinstance(expression, ast.Subscript):
        if _is_dynamic_builtin_namespace_subscript(expression):
            return _CallableKind.OPAQUE
        element = _resolve_static_container_element(expression, sequence_aliases)
        if element is not None:
            return _classify_callable_expression(
                element,
                context_name=context_name,
                raw_aliases=raw_aliases,
                dynamic_aliases=dynamic_aliases,
                builtin_module_aliases=builtin_module_aliases,
                sequence_aliases=sequence_aliases,
                import_aliases=import_aliases,
                prefer_dynamic_call=prefer_dynamic_call,
            )
        if isinstance(expression.value, ast.Name):
            if expression.value.id in raw_aliases:
                return _CallableKind.RAW
            if expression.value.id in dynamic_aliases:
                return _CallableKind.OPAQUE

    if isinstance(expression, ast.Attribute):
        if _is_dynamic_builtin_namespace_attribute(expression, builtin_module_aliases):
            return _CallableKind.DYNAMIC
        if _is_dynamic_dict_lookup_attribute(
            expression,
            dynamic_aliases=dynamic_aliases,
            builtin_module_aliases=builtin_module_aliases,
        ):
            return _CallableKind.DYNAMIC
        if expression.attr in {*_RAW_METHOD_NAMES, "__dict__", "modules"} and (
            _is_dynamic_value_expression(
                expression.value,
                dynamic_aliases=dynamic_aliases,
                builtin_module_aliases=builtin_module_aliases,
            )
        ):
            return _CallableKind.DYNAMIC
        if (
            expression.attr == "__dict__"
            and isinstance(expression.value, ast.Name)
            and expression.value.id in builtin_module_aliases
        ):
            return _CallableKind.DYNAMIC
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
            sequence_aliases=sequence_aliases,
            import_aliases=import_aliases,
            prefer_dynamic_call=False,
        )
        return callable_kind if callable_kind is _CallableKind.RAW else _CallableKind.UNKNOWN

    if isinstance(expression, ast.Call | ast.Subscript) and _is_dynamic_lookup_expression(
        expression,
        dynamic_aliases=dynamic_aliases,
        builtin_module_aliases=builtin_module_aliases,
    ):
        return (
            _CallableKind.OPAQUE if isinstance(expression, ast.Subscript) else _CallableKind.DYNAMIC
        )

    return _CallableKind.UNKNOWN


def _is_hard_dynamic_kind(callable_kind: _CallableKind) -> bool:
    return callable_kind in {_CallableKind.DYNAMIC, _CallableKind.OPAQUE}


def _is_dynamic_lookup_expression(
    expression: ast.Call | ast.Subscript,
    *,
    dynamic_aliases: set[str],
    builtin_module_aliases: set[str],
) -> bool:
    if isinstance(expression, ast.Subscript):
        if _is_dynamic_builtin_namespace_subscript(expression):
            return True
        if _is_builtin_dict_subscript(expression, builtin_module_aliases):
            return _is_builtin_dict_lookup(expression, builtin_module_aliases)
        if isinstance(expression.value, ast.Name) and expression.value.id in dynamic_aliases:
            return True
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
    if _is_dynamic_dict_lookup_attribute(
        expression,
        dynamic_aliases=dynamic_aliases,
        builtin_module_aliases=builtin_module_aliases,
    ):
        return True
    if isinstance(expression, ast.Call) and _is_dynamic_dict_lookup_attribute(
        expression.func,
        dynamic_aliases=dynamic_aliases,
        builtin_module_aliases=builtin_module_aliases,
    ):
        return True
    if _is_builtin_dict_attribute(expression, builtin_module_aliases):
        return True
    if _is_dynamic_builtin_namespace_attribute(expression, builtin_module_aliases):
        return True
    if _is_dynamic_dict_attribute(expression, dynamic_aliases):
        return True
    if _is_dynamic_module_attribute(expression, dynamic_aliases):
        return True
    if isinstance(expression, ast.Name):
        return expression.id in dynamic_aliases
    if isinstance(expression, ast.Attribute) and expression.attr == "__dict__":
        return _is_dynamic_value_expression(
            expression.value,
            dynamic_aliases=dynamic_aliases,
            builtin_module_aliases=builtin_module_aliases,
        )
    if not isinstance(expression, ast.Call):
        return False
    if isinstance(expression.func, ast.Call | ast.Subscript):
        return _is_dynamic_lookup_expression(
            expression.func,
            dynamic_aliases=dynamic_aliases,
            builtin_module_aliases=builtin_module_aliases,
        )
    if isinstance(expression.func, ast.Name):
        return _is_dynamic_callable_name(expression.func.id, dynamic_aliases)
    if _is_dynamic_import_attribute(expression.func, dynamic_aliases):
        return True
    return _is_dynamic_builtin_attribute(expression.func, builtin_module_aliases)


def _is_dynamic_value_expression(
    expression: ast.AST,
    *,
    dynamic_aliases: set[str],
    builtin_module_aliases: set[str],
) -> bool:
    if isinstance(expression, ast.Name):
        return expression.id in dynamic_aliases
    if isinstance(expression, ast.Call | ast.Subscript):
        return _is_dynamic_lookup_expression(
            expression,
            dynamic_aliases=dynamic_aliases,
            builtin_module_aliases=builtin_module_aliases,
        )
    if isinstance(expression, ast.Attribute) and expression.attr in {"__dict__", "modules"}:
        return _is_dynamic_lookup_call(
            expression,
            dynamic_aliases=dynamic_aliases,
            builtin_module_aliases=builtin_module_aliases,
        )
    return False


def _is_import_callable_expression(
    expression: ast.AST,
    *,
    import_aliases: set[str],
    dynamic_aliases: set[str],
    builtin_module_aliases: set[str],
) -> bool:
    if isinstance(expression, ast.Name):
        return expression.id in import_aliases
    if (
        isinstance(expression, ast.Attribute)
        and expression.attr == "import_module"
        and isinstance(expression.value, ast.Name)
        and expression.value.id in dynamic_aliases
    ):
        return True
    if isinstance(expression, ast.Subscript) and _is_dynamic_import_lookup(
        expression, dynamic_aliases
    ):
        return True
    return _is_builtin_dict_import_lookup(expression, builtin_module_aliases)


def _is_dynamic_import_lookup(expression: ast.Subscript, dynamic_aliases: set[str]) -> bool:
    if not isinstance(expression.value, ast.Name) or expression.value.id not in dynamic_aliases:
        return False
    is_static, key = _static_subscript_key(expression.slice)
    return is_static and key == "__import__"


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


def _is_dynamic_builtin_namespace_attribute(
    expression: ast.AST,
    builtin_module_aliases: set[str],
) -> bool:
    return (
        isinstance(expression, ast.Attribute)
        and isinstance(expression.value, ast.Name)
        and expression.value.id == "__builtins__"
        and expression.value.id in builtin_module_aliases
    )


def _is_dynamic_builtin_namespace_subscript(expression: ast.AST) -> bool:
    return (
        isinstance(expression, ast.Subscript)
        and isinstance(expression.value, ast.Name)
        and expression.value.id == "__builtins__"
    )


def _is_dynamic_dict_attribute(expression: ast.AST, dynamic_aliases: set[str]) -> bool:
    return (
        isinstance(expression, ast.Attribute)
        and isinstance(expression.value, ast.Name)
        and expression.value.id in dynamic_aliases
        and expression.attr == "__dict__"
    )


def _is_dynamic_dict_lookup_attribute(
    expression: ast.AST,
    *,
    dynamic_aliases: set[str],
    builtin_module_aliases: set[str],
) -> bool:
    return (
        isinstance(expression, ast.Attribute)
        and expression.attr in _DYNAMIC_DICT_METHOD_NAMES
        and _is_dynamic_value_expression(
            expression.value,
            dynamic_aliases=dynamic_aliases,
            builtin_module_aliases=builtin_module_aliases,
        )
    )


def _is_dynamic_module_attribute(expression: ast.AST, dynamic_aliases: set[str]) -> bool:
    return (
        isinstance(expression, ast.Attribute)
        and isinstance(expression.value, ast.Name)
        and expression.value.id in dynamic_aliases
        and expression.attr == "modules"
    )


def _is_dynamic_import_attribute(expression: ast.AST, dynamic_aliases: set[str]) -> bool:
    return (
        isinstance(expression, ast.Attribute)
        and expression.attr == "import_module"
        and isinstance(expression.value, ast.Name)
        and expression.value.id in dynamic_aliases
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
    if not _is_builtin_dict_subscript(expression, builtin_module_aliases):
        return False
    assert isinstance(expression, ast.Subscript)
    key = expression.slice
    return (
        isinstance(key, ast.Constant)
        and isinstance(key.value, str)
        and (key.value == "open" or key.value in _DYNAMIC_CALLABLE_NAMES)
    )


def _is_builtin_dict_import_lookup(
    expression: ast.AST,
    builtin_module_aliases: set[str],
) -> bool:
    if not _is_builtin_dict_subscript(expression, builtin_module_aliases):
        return False
    assert isinstance(expression, ast.Subscript)
    key = expression.slice
    return isinstance(key, ast.Constant) and key.value == "__import__"


def _is_builtin_dict_subscript(
    expression: ast.AST,
    builtin_module_aliases: set[str],
) -> bool:
    return isinstance(expression, ast.Subscript) and _is_builtin_dict_attribute(
        expression.value,
        builtin_module_aliases,
    )


def _is_builtin_dict_raw_lookup(
    expression: ast.AST,
    builtin_module_aliases: set[str],
) -> bool:
    if not _is_builtin_dict_subscript(expression, builtin_module_aliases):
        return False
    assert isinstance(expression, ast.Subscript)
    key = expression.slice
    return isinstance(key, ast.Constant) and key.value == "open"


def _is_builtin_module_binding(expression: ast.AST, builtin_module_aliases: set[str]) -> bool:
    return isinstance(expression, ast.Name) and expression.id in builtin_module_aliases


def _constructor_class_name(expression: ast.AST) -> str | None:
    """Return the syntactic class name for a simple ``Reader()`` binding."""
    if isinstance(expression, ast.Call) and isinstance(expression.func, ast.Name):
        return expression.func.id
    return None


def _callable_binding_name(
    function: ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda,
    lambdas: dict[str, ast.Lambda],
) -> str | None:
    if isinstance(function, ast.FunctionDef | ast.AsyncFunctionDef):
        return function.name
    for name, candidate in lambdas.items():
        if candidate is function:
            return name
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


def _mapping_elements(
    expression: ast.AST,
    sequence_aliases: dict[str, _StaticContainer] | None = None,
) -> dict[object, ast.expr] | None:
    if not isinstance(expression, ast.Dict):
        return None
    result: dict[object, ast.expr] = {}
    for key, value in zip(expression.keys, expression.values, strict=True):
        if key is None:
            unpacked = _resolve_static_mapping(value, sequence_aliases or {})
            if unpacked is None:
                return None
            result.update(unpacked)
            continue
        is_static, static_key = _static_subscript_key(key)
        if not is_static:
            return None
        result[static_key] = value
    return result


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


def _pattern_bound_names(pattern: ast.AST) -> set[str]:
    """Return names introduced by a structural-pattern match."""
    if isinstance(pattern, ast.MatchAs):
        names = {pattern.name} if pattern.name is not None else set()
        if pattern.pattern is not None:
            names.update(_pattern_bound_names(pattern.pattern))
        return names
    if isinstance(pattern, ast.MatchStar):
        return {pattern.name} if pattern.name is not None else set()
    if isinstance(pattern, ast.MatchMapping):
        names = set()
        if pattern.rest is not None:
            names.add(pattern.rest)
        for child in pattern.patterns:
            names.update(_pattern_bound_names(child))
        return names
    if isinstance(pattern, ast.MatchSequence):
        names: set[str] = set()
        for child in pattern.patterns:
            names.update(_pattern_bound_names(child))
        return names
    if isinstance(pattern, ast.MatchClass):
        names: set[str] = set()
        for child in [*pattern.patterns, *pattern.kwd_patterns]:
            names.update(_pattern_bound_names(child))
        return names
    if isinstance(pattern, ast.MatchOr):
        names: set[str] = set()
        for child in pattern.patterns:
            names.update(_pattern_bound_names(child))
        return names
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
