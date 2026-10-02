"""Static scan of every direct ``resolved_series`` writer (E1-V7).

A ``resolved_series`` row claims "GRID knew ``value`` for ``obs_date`` from
``release_date`` / ``vintage_date`` on". Only ``normalization/resolver.py``
derives those dates from a real pull (``raw_series.pull_timestamp``). A writer
that stamps ``release_date`` and/or ``vintage_date`` with the row's own
``obs_date`` backdates every historical row it loads: history fetched today
looks, to every point-in-time reader, as if it had been known on each past
day (the ``spy_full`` 2021-03-26.. fill, coingecko's ``pull_history``).

Per ``INSERT INTO resolved_series`` outside the resolver, this module reports:

* no explicit column list, or a dynamic (f-string) one: unverifiable;
* ``release_date`` / ``vintage_date`` missing: both are NOT NULL with no
  default, so the insert can never succeed (a dead writer);
* ``ON CONFLICT ... DO UPDATE``: a re-run rewrites a stored vintage in place;
* ``release_date`` / ``vintage_date`` bound to the same expression as
  ``obs_date``: a backdated vintage. A same-day snapshot (all three are the
  run's own date: an expression calling ``today()`` / ``now()`` /
  ``utcnow()`` or ``CURRENT_DATE``, a name assigned from one, or a name whose
  identifier says ``today``) is not a backdate;
* a binding the scan cannot resolve (positional ``%s`` parameters that are
  not a literal tuple at the call, named parameters whose values cannot be
  found): unverifiable, never passed.

Bindings are resolved from the SQL text itself (``:od, :od, :od``), from the
literal tuple/dict passed at the ``execute`` call, or (named parameters bound
through a variable) from the dict literals in the same module carrying those
keys. ``.sql`` files are scanned too (``INSERT ... SELECT`` expressions are
compared textually).
"""

from __future__ import annotations

import ast
import functools
import os
import re
from dataclasses import dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RESOLVER = "normalization/resolver.py"
# Unlike the raw_series scan, migrations/ is scanned: a data migration that
# backfills resolved_series is exactly the kind of writer this gate is for.
_SKIP_TOP = frozenset({"tests", "evals", "pwa", "notebooks", "data", "outputs", "output", "docs",
                       "node_modules", ".git", ".github", ".claude", "server_log"})
_INSERT = re.compile(
    r"insert\s+into\s+(?:public\.)?resolved_series\b\s*(\((?P<cols>[^)]*)\))?"
    r"(?P<rest>.*?)(?=insert\s+into|\Z)",
    re.I | re.S,
)
_REWRITE = re.compile(r"on\s+conflict[^;]*?do\s+update", re.I | re.S)
_POSITIONAL = re.compile(r"%s|\?")
_NAMED = re.compile(r"^(?::(?P<a>[A-Za-z_]\w*)|%\((?P<b>[A-Za-z_]\w*)\)s)$")
#: Callables that return the current date/time: methods (date.today(),
#: datetime.now(tz), datetime.utcnow(), pd.Timestamp.now()) and the module-
#: local helper names GRID uses for "now in UTC". Nothing else counts.
_TODAY_METHODS = frozenset({"today", "now", "utcnow"})
_TODAY_FUNCTIONS = frozenset({"_utc_now", "utc_now", "utcnow"})
_SQL_TODAY = re.compile(r"\b(current_date|current_timestamp|localtimestamp)\b|\bnow\(\)", re.I)
_DATE_COLS = ("release_date", "vintage_date")
EXECUTE_ATTRS = frozenset({"execute", "executemany", "exec_driver_sql"})
UNVERIFIABLE = "unverifiable"
# Wrappers that do not change which date a value is: x::date, CAST(x AS date),
# date(x) / date_trunc('day', x) of the same thing. Stripped before comparing.
_CAST_SUFFIX = re.compile(r"::\s*[a-z_]+(?:\s+(?:with|without)\s+time\s+zone)?(?:\s*\(\s*\d+\s*\))?\s*$", re.I)
_CAST_CALL = re.compile(r"^cast\s*\((?P<x>.*)\s+as\s+[a-z_ ]+(?:\(\s*\d+\s*\))?\s*\)$", re.I | re.S)
_DATE_CALL = re.compile(r"^(?:date\s*\(|date_trunc\s*\(\s*'day'\s*,)\s*(?P<x>.*)\)\s*$", re.I | re.S)

#: Writers whose same-value obs/release/vintage binding was reviewed and is a
#: same-day snapshot the scan cannot prove statically (the date arrives as a
#: parameter). Keyed by (file, function); a backdate finding inside that
#: function is not reported. Adding an entry is a reviewed E1 change.
REVIEWED_SAME_DAY: dict[tuple[str, str], str] = {
    ("ingestion/options.py", "_push_to_resolved"): (
        "today_str is the run's own session date (OptionsPuller.pull_all: now = _utc_now(); "
        "today_str = now.date().isoformat(), and the pull is skipped outside that equity session); "
        "obs = release = vintage = the day the chain was captured"),
}


@dataclass(frozen=True)
class Problem:
    where: str
    kind: str  # "backdate" | "rewrite" | "missing" | "unverifiable"
    detail: str

    def __str__(self) -> str:
        return f"{self.where}: {self.detail}"


@dataclass
class _Site:
    path: str
    line: int
    sql: str
    columns: tuple[str, ...] | None
    exprs: tuple[str, ...] | None
    positional_base: list[int] = field(default_factory=list)
    rewrite: bool = False


# ── SQL text parsing ────────────────────────────────────────────────────


def _balanced(s: str, start: int) -> int:
    """Index just past the ``)`` matching the ``(`` at ``s[start]``; -1 if unbalanced."""
    depth = 0
    for i in range(start, len(s)):
        if s[i] == "(":
            depth += 1
        elif s[i] == ")":
            depth -= 1
            if depth == 0:
                return i + 1
    return -1


def _split_top(s: str) -> list[str]:
    out, depth, cur = [], 0, ""
    for ch in s:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            out.append(cur.strip())
            cur = ""
        else:
            cur += ch
    if cur.strip():
        out.append(cur.strip())
    return out


def _top_level_keyword(s: str, keyword: str) -> int:
    """Offset of the first top-level (paren depth 0) ``keyword`` in ``s``; -1 if absent."""
    depth = 0
    pat = re.compile(rf"\b{keyword}\b", re.I)
    for i, ch in enumerate(s):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif depth == 0 and pat.match(s, i) and (i == 0 or not (s[i - 1].isalnum() or s[i - 1] == "_")):
            return i
    return -1


def _strip_alias(expr: str) -> str:
    return re.sub(r"\s+as\s+[A-Za-z_]\w*\s*$", "", expr.strip(), flags=re.I)


def _norm(expr: str) -> str:
    return re.sub(r"\s+", " ", expr.strip().lower())


def _expressions(rest: str) -> tuple[list[str], int] | None:
    """(expressions, offset of the list in ``rest``) for ``VALUES (...)`` or ``SELECT ...``."""
    m = re.match(r"\s*values\s*", rest, re.I)
    if m:
        start = m.end()
        if start >= len(rest) or rest[start] != "(":
            return None
        end = _balanced(rest, start)
        if end < 0:
            return None
        return _split_top(rest[start + 1:end - 1]), start + 1
    m = re.match(r"\s*select\s+", rest, re.I)
    if m:
        body = rest[m.end():]
        stop = len(body)
        for kw in ("from", "on", "returning", "where", "group", "order", "limit"):
            at = _top_level_keyword(body, kw)
            if at >= 0:
                stop = min(stop, at)
        stop = min(stop, body.find(";") if ";" in body else stop)
        return [_strip_alias(e) for e in _split_top(body[:stop])], m.end()
    return None


def scan_sql(rel: str, line: int | None, sql: str) -> list[_Site]:
    """Insert sites in one SQL string; ``line=None`` numbers lines within ``sql`` (a .sql file)."""
    sites = []
    for m in _INSERT.finditer(sql):
        rest = m.group("rest") or ""
        if m.group("cols") is None and rest.strip() and not re.match(r"\s*(values|select)\b", rest, re.I):
            continue  # prose ("... and insert into resolved_series."), not a statement
        cols = tuple(_norm(c) for c in m.group("cols").split(",")) if m.group("cols") else None
        parsed = _expressions(rest)
        exprs = tuple(parsed[0]) if parsed else None
        base = len(_POSITIONAL.findall(sql[:m.start()]))
        bases = []
        if exprs is not None:
            running = base + len(_POSITIONAL.findall(rest[:parsed[1]]))
            for e in exprs:
                bases.append(running)
                running += len(_POSITIONAL.findall(e))
        at = line if line is not None else sql[:m.start()].count("\n") + 1
        sites.append(_Site(rel, at, sql, cols, exprs, bases, bool(_REWRITE.search(rest))))
    return sites


# ── Python AST: where each SQL string is executed, and with what ───────


def _string_nodes(tree: ast.AST):
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            yield node, node.value
        elif isinstance(node, ast.JoinedStr):
            yield node, "".join(v.value if isinstance(v, ast.Constant) else "{dynamic}" for v in node.values)


class _Module:
    def __init__(self, rel: str, source: str):
        self.rel = rel
        self.tree = ast.parse(source)
        self.parent: dict[ast.AST, ast.AST] = {}
        for node in ast.walk(self.tree):
            for child in ast.iter_child_nodes(node):
                self.parent[child] = node

    def ancestors(self, node: ast.AST):
        while node in self.parent:
            node = self.parent[node]
            yield node

    def function_of(self, node: ast.AST) -> ast.AST:
        for anc in self.ancestors(node):
            if isinstance(anc, (ast.FunctionDef, ast.AsyncFunctionDef)):
                return anc
        return self.tree

    def execute_calls(self, string_node: ast.AST) -> list[ast.Call]:
        """``execute``-style calls that run this SQL string (directly or via a variable)."""
        calls = []
        for anc in self.ancestors(string_node):
            if isinstance(anc, ast.Call) and isinstance(anc.func, ast.Attribute) and anc.func.attr in EXECUTE_ATTRS:
                calls.append(anc)
                return calls
            if isinstance(anc, (ast.Assign, ast.AnnAssign)):
                targets = anc.targets if isinstance(anc, ast.Assign) else [anc.target]
                names = {t.id for t in targets if isinstance(t, ast.Name)}
                for node in ast.walk(self.tree):
                    if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                            and node.func.attr in EXECUTE_ATTRS and node.args):
                        first = node.args[0]
                        if isinstance(first, ast.Call) and first.args:
                            first = first.args[0]  # text(sql)
                        if isinstance(first, ast.Name) and first.id in names:
                            calls.append(node)
                return calls
            if isinstance(anc, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Module)):
                return calls
        return calls

    @staticmethod
    def params_of(call: ast.Call) -> ast.AST | None:
        if len(call.args) > 1:
            return call.args[1]
        for kw in call.keywords:
            if kw.arg in ("parameters", "params", "vars", "args"):
                return kw.value
        return None

    def dict_literals_with(self, keys: set[str]) -> list[ast.Dict]:
        out = []
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Dict):
                names = {k.value for k in node.keys if isinstance(k, ast.Constant) and isinstance(k.value, str)}
                if keys <= names:
                    out.append(node)
        return out

    def _bindings_of(self, name: str, scope: ast.AST) -> list[ast.AST] | None:
        """Values assigned to ``name`` in ``scope``; None if it is bound any other way
        (a parameter, a loop or comprehension target, a with target, unpacking, a walrus)."""
        values: list[ast.AST] = []
        if isinstance(scope, (ast.FunctionDef, ast.AsyncFunctionDef)):
            args = scope.args
            params = [*args.posonlyargs, *args.args, *args.kwonlyargs]
            params += [a for a in (args.vararg, args.kwarg) if a is not None]
            if any(a.arg == name for a in params):
                return None
        for node in ast.walk(scope):
            if isinstance(node, ast.Assign):
                for t in node.targets:
                    if isinstance(t, ast.Name) and t.id == name:
                        values.append(node.value)
                    elif any(isinstance(n, ast.Name) and n.id == name for n in ast.walk(t)):
                        return None  # tuple unpacking: value unknown
            elif isinstance(node, (ast.AnnAssign, ast.AugAssign)):
                if isinstance(node.target, ast.Name) and node.target.id == name:
                    if isinstance(node, ast.AugAssign) or node.value is None:
                        return None
                    values.append(node.value)
            elif isinstance(node, (ast.For, ast.AsyncFor, ast.comprehension)):
                if any(isinstance(n, ast.Name) and n.id == name for n in ast.walk(node.target)):
                    return None
            elif isinstance(node, ast.withitem) and node.optional_vars is not None:
                if any(isinstance(n, ast.Name) and n.id == name for n in ast.walk(node.optional_vars)):
                    return None
            elif isinstance(node, ast.NamedExpr) and node.target.id == name:
                return None
        return values

    def canonical(self, expr: ast.AST, depth: int = 0) -> str:
        """``ast.dump`` of ``expr`` after following single-assignment name aliases
        (``rel = d`` then ``rel`` is ``d``), so an alias cannot hide a backdate."""
        if depth < 5 and isinstance(expr, ast.Name):
            values = self._bindings_of(expr.id, self.function_of(expr))
            if values is not None and len(values) == 1 and isinstance(values[0], ast.Name):
                return self.canonical(values[0], depth + 1)
        return ast.dump(expr)

    def is_today(self, expr: ast.AST, depth: int = 0) -> bool:
        """True only when ``expr`` is provably the run's own date (a same-day snapshot).

        Accepted: a direct ``today()`` / ``now()`` / ``utcnow()`` call (any
        receiver: ``date.today()``, ``datetime.now(tz)``, ``_utc_now()``),
        ``.date()`` / ``.isoformat()`` / ``.strftime(...)`` / ``str()`` of one,
        or a local name every assignment of which is one of those. Arithmetic
        (``date.today() - timedelta(days=i)``), parameters and loop targets
        are not the run's date.
        """
        if depth > 6:
            return False
        if isinstance(expr, ast.Call):
            fn = expr.func
            if isinstance(fn, ast.Attribute):
                if fn.attr in _TODAY_METHODS:
                    return True
                if fn.attr in ("date", "isoformat", "strftime"):
                    return self.is_today(fn.value, depth + 1)
                return False
            if isinstance(fn, ast.Name):
                if fn.id in _TODAY_FUNCTIONS:
                    return True
                if fn.id == "str" and len(expr.args) == 1:
                    return self.is_today(expr.args[0], depth + 1)
            return False
        if isinstance(expr, ast.Name):
            values = self._bindings_of(expr.id, self.function_of(expr))
            return bool(values) and all(self.is_today(v, depth + 1) for v in values)
        return False


def _value_for(d: ast.Dict, key: str) -> ast.AST | None:
    for k, v in zip(d.keys, d.values):
        if isinstance(k, ast.Constant) and k.value == key:
            return v
    return None


def _strip_casts(expr: str) -> str:
    """``x::date``, ``CAST(x AS date)``, ``date(x)``, ``(x)`` -> ``x`` (repeatedly)."""
    e = expr.strip()
    while True:
        before = e
        e = _CAST_SUFFIX.sub("", e).strip()
        m = _CAST_CALL.match(e) or _DATE_CALL.match(e)
        if m and _balanced(e, e.index("(")) == len(e):
            e = m.group("x").strip()
        if e.startswith("(") and _balanced(e, 0) == len(e):
            e = e[1:-1].strip()
        if e == before:
            return e


def _binding(expr: str, base: int) -> tuple[str, object]:
    """('named', name) | ('pos', index) | ('sql', normalised text) | ('dynamic', expr).

    Casts are stripped first, so ``%s::date`` binds like ``%s`` and
    ``t.obs_date::date`` compares equal to ``t.obs_date``. A SQL expression
    that still holds a placeholder (``COALESCE(%s, x)``) is 'dynamic':
    unverifiable, never passed. ``base`` is the index of the first
    positional placeholder in ``expr``.
    """
    e = _strip_casts(expr)
    if "{dynamic}" in e:
        return ("dynamic", e)
    m = _NAMED.match(e)
    if m:
        return ("named", m.group("a") or m.group("b"))
    if _POSITIONAL.fullmatch(e):
        return ("pos", base)
    if _POSITIONAL.search(e) or re.search(r"(?<![:\w]):[A-Za-z_]\w*|%\(\w+\)s", e):
        return ("dynamic", e)
    return ("sql", _norm(e))


def site_problems(site: _Site, module: _Module | None, string_node: ast.AST | None) -> list[Problem]:
    where = f"{site.path}:{site.line}"
    if site.columns is None:
        return [Problem(where, UNVERIFIABLE, "INSERT INTO resolved_series without an explicit column list")]
    if any("{dynamic}" in c for c in site.columns):
        return [Problem(where, UNVERIFIABLE, "dynamic column list cannot be verified")]
    problems: list[Problem] = []
    if site.rewrite:
        problems.append(Problem(where, "rewrite", "ON CONFLICT ... DO UPDATE rewrites a stored vintage in place"))
    missing = [c for c in _DATE_COLS if c not in site.columns]
    if missing:
        problems.append(Problem(where, "missing", f"does not set {', '.join(missing)} (NOT NULL, no default: "
                                                 "the insert cannot succeed)"))
    if "obs_date" not in site.columns:
        problems.append(Problem(where, "missing", "does not set obs_date"))
        return problems
    if site.exprs is None or len(site.exprs) != len(site.columns):
        problems.append(Problem(where, UNVERIFIABLE, "VALUES/SELECT list does not line up with the column list"))
        return problems

    def bind(col: str):
        i = site.columns.index(col)
        return _binding(site.exprs[i], site.positional_base[i])

    obs = bind("obs_date")
    for col in _DATE_COLS:
        if col not in site.columns:
            continue
        b = bind(col)
        verdict = _same_binding(obs, b, site, module, string_node)
        if verdict and module is not None and string_node is not None:
            fn = module.function_of(string_node)
            if (site.path, getattr(fn, "name", "")) in REVIEWED_SAME_DAY:
                continue
        if verdict is None:
            problems.append(Problem(where, UNVERIFIABLE, f"cannot verify where {col} comes from"))
        elif verdict:
            problems.append(Problem(where, "backdate", f"{col} is stamped with the row's obs_date (backdated vintage)"))
    return problems


def _same_binding(obs, other, site: _Site, module: _Module | None, string_node) -> bool | None:
    """True: same non-today value (backdate). False: different or same-day. None: unverifiable."""
    if obs[0] == "dynamic" or other[0] == "dynamic":
        return None
    if obs[0] == "sql" and other[0] == "sql":
        if obs[1] != other[1]:
            # coalesce(t.as_of, t.obs) falls back to the obs date: not provably different
            if re.search(rf"(?<![\w.]){re.escape(str(obs[1]))}(?!\w)", str(other[1])):
                return None
            return False
        return not _SQL_TODAY.search(obs[1])
    if other[0] == "sql":
        # A placeholder-free SQL expression for the vintage (CURRENT_DATE, a
        # filing-date or pull-timestamp column) against a bound obs_date. It
        # cannot be the bound value unless it names an obs date column.
        if _SQL_TODAY.search(str(other[1])):
            return False
        return None if re.search(r"obs", str(other[1])) else False
    if obs[0] == "sql":
        return None  # a bound vintage against a SQL obs_date: not resolvable statically
    if module is None or string_node is None:
        return None
    calls = module.execute_calls(string_node)
    if obs[0] == "named" and other[0] == "named":
        if obs[1] == other[1]:
            # Same placeholder name: same value, unless that value is today's date.
            values, _ = _named_values(module, calls, {obs[1]})
            if values and all(module.is_today(v[obs[1]]) for v in values):
                return False
            return True
        values, from_fallback = _named_values(module, calls, {obs[1], other[1]})
        if not values:
            return None
        verdicts = {module.canonical(v[obs[1]]) == module.canonical(v[other[1]]) and not module.is_today(v[obs[1]])
                    for v in values}
        if from_fallback and len(verdicts) > 1:
            return None  # module dicts with these keys disagree: which one is bound is unknown
        return True in verdicts
    if obs[0] == "pos" and other[0] == "pos":
        if not calls:
            return None
        verdicts = []
        for call in calls:
            params = module.params_of(call)
            if not isinstance(params, (ast.Tuple, ast.List)) or any(isinstance(e, ast.Starred) for e in params.elts):
                return None
            if max(obs[1], other[1]) >= len(params.elts):
                return None
            a, b = params.elts[obs[1]], params.elts[other[1]]
            verdicts.append(module.canonical(a) == module.canonical(b) and not module.is_today(a))
        return any(verdicts)
    return None


def _named_values(module: _Module, calls: list[ast.Call],
                  keys: set[str]) -> tuple[list[dict[str, ast.AST]], bool]:
    """(values bound to ``keys``, whether they came from the module-wide fallback).

    A literal dict at the ``execute`` call wins. Otherwise every dict literal
    in the module carrying all the keys is a candidate (a batch built
    elsewhere); the caller treats disagreeing candidates as unverifiable.
    """
    found: list[dict[str, ast.AST]] = []
    for call in calls:
        params = module.params_of(call)
        if isinstance(params, ast.Dict):
            vals = {k: _value_for(params, k) for k in keys}
            if all(v is not None for v in vals.values()):
                found.append(vals)
    if found:
        return found, False
    for d in module.dict_literals_with(keys):
        found.append({k: _value_for(d, k) for k in keys})
    return found, True


# ── repository walk ─────────────────────────────────────────────────────


@functools.lru_cache(maxsize=1)
def sources() -> dict[str, str]:
    """Repo-relative path -> text for every scanned .py/.sql/.sh file (read once)."""
    out: dict[str, str] = {}
    for dirpath, dirnames, filenames in os.walk(REPO):
        rel = Path(dirpath).relative_to(REPO)
        if rel == Path("."):
            dirnames[:] = [d for d in dirnames if d not in _SKIP_TOP]
        dirnames[:] = [d for d in dirnames if d not in ("__pycache__", "node_modules", ".git", ".venv", "venv")]
        for name in filenames:
            if name.endswith((".py", ".sql", ".sh")):
                path = Path(dirpath) / name
                try:
                    out[path.relative_to(REPO).as_posix()] = path.read_text(encoding="utf-8")
                except (UnicodeDecodeError, OSError):
                    continue
    return out


def scan_python(rel: str, source: str) -> list[Problem] | None:
    """Problems per writer module; None when the module writes no resolved_series row."""
    if "resolved_series" not in source.lower():
        return None
    module = _Module(rel, source)
    sites = []
    for node, s in _string_nodes(module.tree):
        for site in scan_sql(rel, getattr(node, "lineno", 0), s):
            sites.append((site, node))
    if not sites:
        return None
    return [p for site, node in sites for p in site_problems(site, module, node)]


def scan_sql_file(rel: str, text: str) -> list[Problem] | None:
    sites = scan_sql(rel, None, text)
    if not sites:
        return None
    return [p for site in sites for p in site_problems(site, None, None)]


def all_writers(files: dict[str, str] | None = None) -> dict[str, list[Problem]]:
    """Every direct resolved_series writer outside the resolver -> its problems ([] = clean)."""
    out: dict[str, list[Problem]] = {}
    for rel, text in sorted((files if files is not None else sources()).items()):
        if rel == RESOLVER:
            continue
        if rel.endswith(".py"):
            problems = scan_python(rel, text)
        elif rel.endswith((".sql", ".sh")):
            problems = scan_sql_file(rel, text)
        else:
            continue
        if problems is not None:
            out[rel] = problems
    return out


_HIDDEN = re.compile(
    r"""to_sql\(\s*(?:name\s*=\s*)?['"]resolved_series['"]|Table\(\s*['"]resolved_series['"]|"""
    r"""__tablename__\s*=\s*['"]resolved_series['"]|\bcopy\s+(?:public\.)?resolved_series\b|"""
    r"""copy_(?:from|records_to_table)\([^)]*['"](?:public\.)?resolved_series['"]""",
    re.I,
)


def hidden_writers(files: dict[str, str] | None = None) -> list[str]:
    """Writers the SQL scan cannot see (ORM/pandas/COPY), refused outright."""
    return sorted(rel for rel, text in (files if files is not None else sources()).items()
                  if "resolved_series" in text and _HIDDEN.search(text))
