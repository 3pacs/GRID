"""GRID Tests runner routing for the stacked Schwab PR #932 (disposable CI).

``.github/workflows/test.yml`` chooses between a self-hosted ``tests`` runner
(persistent PostgreSQL, reset by ``DROP SCHEMA public CASCADE``) and a hosted
``ubuntu-latest`` VM (ephemeral Postgres container). The hosted path is forced
for a short allow-list of events; this file pins the allow-list entry added for
PR #932 (branch ``claude/schwab-integration-r6-20261031``) and proves, with a
small evaluator of the GitHub Actions expression subset the workflow uses,
that:

* the matching event routes every job to ``ubuntu-latest``, starts the
  ephemeral Postgres step and skips the persistent reset, whether
  ``vars.TEST_RUNNER`` is empty or set;
* every negative control (wrong repository, head repository, PR number, base,
  branch or event; an ordinary other PR; pushes) routes exactly as the
  predicate without the #932 disjunct did -- the prior alternatives (push to
  main, #543, #857, #806) and the ordinary self-hosted path are untouched;
* the four expressions (``env.ON_SELF_HOSTED`` and the three ``runs-on``)
  carry byte-identical predicates and the trigger events are unchanged.

The evaluator follows the documented semantics: ``&&``/``||`` return an
operand (not a boolean), ``!`` negates truthiness (``null``, ``false``, ``0``
and ``''`` are falsy), ``==`` compares strings case-insensitively and otherwise
coerces both sides to numbers (``null`` -> 0). Only the operators and
functions test.yml actually uses are implemented.
"""

from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

WORKFLOW = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "test.yml"

PR932_BRANCH = "claude/schwab-integration-r6-20261031"

# The predicate before #918 and #932 (push to main, #543, #857, #806).
_PRE_918_ALTERNATIVES = (
    "((github.event_name == 'push' && github.ref == 'refs/heads/main') || "
    "(github.event_name == 'pull_request' && github.event.pull_request.number == 543 "
    "&& github.head_ref == 'fix/digest-confidence-and-freshness-honesty') || "
    "(github.event_name == 'pull_request' && github.repository == '3pacs/GRID' "
    "&& github.event.pull_request.number == 857 "
    "&& github.event.pull_request.head.repo.full_name == '3pacs/GRID' "
    "&& github.event.pull_request.base.ref == 'main' "
    "&& github.head_ref == 'codex/hermes-finra-v8-caller-composition-20261017') || "
    "(github.event_name == 'pull_request' && github.repository == '3pacs/GRID' "
    "&& github.event.pull_request.head.repo.full_name == '3pacs/GRID' "
    "&& github.event.pull_request.number == 806 "
    "&& github.event.pull_request.base.ref == 'main' "
    "&& github.head_ref == 'draft/options-single-writer-20261002'))"
)

# PR #918's hosted entry, adopted on main 29845179 (merge of 5bb533e5) and kept verbatim.
PR918_CONJUNCTION = (
    "(github.event_name == 'pull_request' && github.repository == '3pacs/GRID' "
    "&& github.event.pull_request.number == 918 "
    "&& github.event.pull_request.head.repo.full_name == '3pacs/GRID' "
    "&& github.event.pull_request.base.ref == 'main' "
    "&& github.head_ref == 'codex/oct25-publication-thesis-native-validation-20261027')"
)
# The hosted group as it stands on main after #918: prior alternatives wrapped once, #918 appended.
PRIOR_ALTERNATIVES = f"({_PRE_918_ALTERNATIVES} || {PR918_CONJUNCTION})"

# The one conjunction this entry adds. Same-repository PR #932 only, from the
# named branch, targeting main.
PR932_CONJUNCTION = (
    "(github.event_name == 'pull_request' && github.repository == '3pacs/GRID' "
    "&& github.event.pull_request.number == 932 "
    "&& github.event.pull_request.head.repo.full_name == '3pacs/GRID' "
    "&& github.event.pull_request.base.ref == 'main' "
    f"&& github.head_ref == '{PR932_BRANCH}')"
)

HOSTED = "ubuntu-latest"
SELF_HOSTED = ["self-hosted", "tests"]
JOBS = ("backend-tests", "frontend-build", "lint")


def _templates(predicate: str) -> dict[str, str]:
    return {
        "ON_SELF_HOSTED": f"!{predicate} && vars.TEST_RUNNER != ''",
        "runs-on": (
            f"{predicate} && 'ubuntu-latest' || "
            "(vars.TEST_RUNNER != '' && fromJSON('[\"self-hosted\",\"tests\"]') || 'ubuntu-latest')"
        ),
    }


CANDIDATE = _templates(f"({PRIOR_ALTERNATIVES} || {PR932_CONJUNCTION})")
BASELINE = _templates(PRIOR_ALTERNATIVES)


# --------------------------------------------------------------------------
# A small evaluator for the expression subset test.yml uses.
# --------------------------------------------------------------------------

_TOKEN = re.compile(
    r"\s*(?:(?P<str>'(?:[^']|'')*')|(?P<num>\d+(?:\.\d+)?)|(?P<op>&&|\|\||==|!=|!|\(|\))"
    r"|(?P<ident>[A-Za-z_][A-Za-z0-9_.-]*))"
)


def _tokenize(expr: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    pos = 0
    while pos < len(expr):
        m = _TOKEN.match(expr, pos)
        if not m or m.end() == pos:
            if expr[pos:].strip() == "":
                break
            raise ValueError(f"cannot tokenize {expr[pos:pos + 20]!r}")
        pos = m.end()
        kind = m.lastgroup
        assert kind is not None
        out.append((kind, m.group(kind)))
    return out


class _Parser:
    """Precedence (lowest first): ``||``, ``&&``, ``==``/``!=``, ``!``, atoms."""

    def __init__(self, expr: str, ctx: dict[str, Any]) -> None:
        self.tokens = _tokenize(expr)
        self.pos = 0
        self.ctx = ctx

    def _peek(self) -> tuple[str, str] | None:
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def _take(self) -> tuple[str, str]:
        tok = self.tokens[self.pos]
        self.pos += 1
        return tok

    def parse(self) -> Any:
        value = self._or()
        if self._peek() is not None:
            raise ValueError(f"trailing tokens at {self.pos}: {self.tokens[self.pos:]}")
        return value

    def _or(self) -> Any:
        left = self._and()
        while self._peek() == ("op", "||"):
            self._take()
            right = self._and()
            left = left if _truthy(left) else right
        return left

    def _and(self) -> Any:
        left = self._eq()
        while self._peek() == ("op", "&&"):
            self._take()
            right = self._eq()
            left = right if _truthy(left) else left
        return left

    def _eq(self) -> Any:
        left = self._unary()
        while self._peek() in (("op", "=="), ("op", "!=")):
            _, op = self._take()
            right = self._unary()
            equal = _loose_equal(left, right)
            left = equal if op == "==" else not equal
        return left

    def _unary(self) -> Any:
        if self._peek() == ("op", "!"):
            self._take()
            return not _truthy(self._unary())
        return self._atom()

    def _atom(self) -> Any:
        kind, text = self._take()
        if kind == "op" and text == "(":
            value = self._or()
            if self._take() != ("op", ")"):
                raise ValueError("expected )")
            return value
        if kind == "str":
            return text[1:-1].replace("''", "'")
        if kind == "num":
            return float(text) if "." in text else int(text)
        if kind == "ident":
            if self._peek() == ("op", "("):
                self._take()
                arg = self._or() if self._peek() != ("op", ")") else None
                if self._take() != ("op", ")"):
                    raise ValueError("expected ) after function argument")
                return _call(text, arg)
            if text == "true":
                return True
            if text == "false":
                return False
            if text == "null":
                return None
            return _lookup(self.ctx, text)
        raise ValueError(f"unexpected token {kind} {text!r}")


def _call(name: str, arg: Any) -> Any:
    if name == "fromJSON":
        return json.loads(arg)
    if name == "always":
        return True
    raise ValueError(f"unsupported function {name}")


def _lookup(ctx: dict[str, Any], path: str) -> Any:
    node: Any = ctx
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


def _truthy(value: Any) -> bool:
    if value is None or value is False:
        return False
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value != 0
    if isinstance(value, str):
        return value != ""
    return True


def _to_number(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        if value == "":
            return 0.0
        try:
            return float(value)
        except ValueError:
            return math.nan
    return math.nan


def _loose_equal(left: Any, right: Any) -> bool:
    if isinstance(left, str) and isinstance(right, str):
        return left.lower() == right.lower()
    a, b = _to_number(left), _to_number(right)
    if math.isnan(a) or math.isnan(b):
        return False
    return a == b


def evaluate(expr: str, ctx: dict[str, Any]) -> Any:
    expr = expr.strip()
    if expr.startswith("${{") and expr.endswith("}}"):
        expr = expr[3:-2]
    return _Parser(expr, ctx).parse()


# --------------------------------------------------------------------------
# Event contexts.
# --------------------------------------------------------------------------


def pull_request(
    number: int,
    head_ref: str,
    *,
    head_repo: str = "3pacs/GRID",
    base_ref: str = "main",
    repository: str = "3pacs/GRID",
    event_name: str = "pull_request",
    runner: str = "",
) -> dict[str, Any]:
    return {
        "github": {
            "event_name": event_name,
            "repository": repository,
            "ref": f"refs/pull/{number}/merge",
            "head_ref": head_ref,
            "event": {
                "pull_request": {
                    "number": number,
                    "head": {"repo": {"full_name": head_repo}},
                    "base": {"ref": base_ref},
                }
            },
        },
        "vars": {"TEST_RUNNER": runner},
    }


def push(ref: str, *, runner: str = "", event_name: str = "push") -> dict[str, Any]:
    return {
        "github": {
            "event_name": event_name,
            "repository": "3pacs/GRID",
            "ref": ref,
            "head_ref": "",
            "event": {},
        },
        "vars": {"TEST_RUNNER": runner},
    }


def pr932(**overrides: Any) -> dict[str, Any]:
    kwargs: dict[str, Any] = {"number": 932, "head_ref": PR932_BRANCH}
    kwargs.update(overrides)
    return pull_request(**kwargs)


NEGATIVE_CONTROLS: dict[str, dict[str, Any]] = {
    "wrong repository": pr932(repository="3pacs/GRID-mirror"),
    "wrong head repository (fork)": pr932(head_repo="someone/GRID"),
    "wrong PR number": pr932(number=933),
    "wrong base (develop)": pr932(base_ref="develop"),
    "wrong base (still stacked on the #931 branch)": pr932(base_ref="claude/brave-feynman-f63cd4"),
    "wrong branch": pr932(head_ref="claude/schwab-integration-r7-20261031"),
    "#932 number with the #918 branch": pr932(head_ref="codex/oct25-publication-thesis-native-validation-20261027"),
    "#918 number with the #932 branch": pull_request(918, PR932_BRANCH),
    "wrong event (push to the branch)": push(f"refs/heads/{PR932_BRANCH}"),
    "wrong event (pull_request_target)": pr932(event_name="pull_request_target"),
    "wrong event (workflow_dispatch)": pr932(event_name="workflow_dispatch"),
    "ordinary other PR": pull_request(999, "feature/other"),
    "ordinary push to develop": push("refs/heads/develop"),
}

PRIOR_HOSTED_EVENTS: dict[str, dict[str, Any]] = {
    "push to main": push("refs/heads/main"),
    "#918": pull_request(918, "codex/oct25-publication-thesis-native-validation-20261027"),
    "#543": pull_request(543, "fix/digest-confidence-and-freshness-honesty"),
    "#857": pull_request(857, "codex/hermes-finra-v8-caller-composition-20261017"),
    "#806": pull_request(806, "draft/options-single-writer-20261002"),
}


def _with_runner(ctx: dict[str, Any], runner: str) -> dict[str, Any]:
    out = json.loads(json.dumps(ctx))
    out["vars"]["TEST_RUNNER"] = runner
    return out


# --------------------------------------------------------------------------
# Workflow bytes.
# --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def workflow() -> dict[str, Any]:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def _strip(expr: str) -> str:
    expr = expr.strip()
    assert expr.startswith("${{") and expr.endswith("}}"), expr
    return expr[3:-2].strip()


def _step(workflow: dict[str, Any], name: str) -> dict[str, Any]:
    for step in workflow["jobs"]["backend-tests"]["steps"]:
        if step.get("name") == name:
            return step
    raise AssertionError(f"step {name!r} not found")


def routing(workflow: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
    """What the workflow bytes decide for one event: runners and DB steps."""
    on_self_hosted = evaluate(workflow["env"]["ON_SELF_HOSTED"], ctx)
    assert isinstance(on_self_hosted, bool)
    step_ctx = dict(ctx, env={"ON_SELF_HOSTED": "true" if on_self_hosted else "false"})
    ephemeral = _step(workflow, "Start ephemeral Postgres (hosted runner)")
    reset = _step(workflow, "Reset persistent Postgres (self-hosted)")
    return {
        "ON_SELF_HOSTED": on_self_hosted,
        "runs-on": {job: evaluate(workflow["jobs"][job]["runs-on"], ctx) for job in JOBS},
        "ephemeral_postgres": bool(evaluate(ephemeral["if"], step_ctx)),
        "persistent_reset": bool(evaluate(reset["if"], step_ctx)),
    }


def baseline_routing(workflow: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
    """The same decision under the predicate without the #932 disjunct."""
    stripped = json.loads(json.dumps(workflow))
    stripped["env"]["ON_SELF_HOSTED"] = "${{ " + BASELINE["ON_SELF_HOSTED"] + " }}"
    for job in JOBS:
        stripped["jobs"][job]["runs-on"] = "${{ " + BASELINE["runs-on"] + " }}"
    return routing(stripped, ctx)


HOSTED_ROUTING = {
    "ON_SELF_HOSTED": False,
    "runs-on": {job: HOSTED for job in JOBS},
    "ephemeral_postgres": True,
    "persistent_reset": False,
}
SELF_HOSTED_ROUTING = {
    "ON_SELF_HOSTED": True,
    "runs-on": {job: SELF_HOSTED for job in JOBS},
    "ephemeral_postgres": False,
    "persistent_reset": True,
}


# --------------------------------------------------------------------------
# Exact bytes and events.
# --------------------------------------------------------------------------


def test_predicate_bytes_are_the_candidate_templates(workflow: dict[str, Any]) -> None:
    assert _strip(workflow["env"]["ON_SELF_HOSTED"]) == CANDIDATE["ON_SELF_HOSTED"]
    for job in JOBS:
        assert _strip(workflow["jobs"][job]["runs-on"]) == CANDIDATE["runs-on"], job
    text = WORKFLOW.read_text(encoding="utf-8")
    assert text.count(PR932_CONJUNCTION) == 4
    assert text.count(PRIOR_ALTERNATIVES) == 4
    assert text.count(PR918_CONJUNCTION) == 4


def test_trigger_events_and_jobs_are_unchanged(workflow: dict[str, Any]) -> None:
    on = workflow.get("on", workflow.get(True))
    assert on == {"push": {"branches": ["main", "develop"]}, "pull_request": {"branches": ["main"]}}
    assert tuple(workflow["jobs"]) == JOBS
    assert _step(workflow, "Start ephemeral Postgres (hosted runner)")["if"] == "env.ON_SELF_HOSTED != 'true'"
    assert _step(workflow, "Reset persistent Postgres (self-hosted)")["if"] == "env.ON_SELF_HOSTED == 'true'"
    assert "DROP SCHEMA IF EXISTS public CASCADE" in _step(workflow, "Reset persistent Postgres (self-hosted)")["run"]


# --------------------------------------------------------------------------
# The matching event.
# --------------------------------------------------------------------------


@pytest.mark.parametrize("runner", ["", "tests"], ids=["TEST_RUNNER empty", "TEST_RUNNER set"])
def test_pr932_targeting_main_is_hosted_and_ephemeral(workflow: dict[str, Any], runner: str) -> None:
    decided = routing(workflow, pr932(runner=runner))
    assert decided == HOSTED_ROUTING
    # And this is a change only when a self-hosted runner is configured:
    # without the entry, a set TEST_RUNNER would have routed #932 to the
    # persistent-reset path.
    before = baseline_routing(workflow, pr932(runner=runner))
    assert before == (SELF_HOSTED_ROUTING if runner else HOSTED_ROUTING)


def test_pr932_entry_binds_every_term(workflow: dict[str, Any]) -> None:
    """Flipping any single term of the conjunction falls back to the baseline."""
    flips = {
        "event_name": pr932(event_name="push", runner="tests"),
        "repository": pr932(repository="3pacs/GRID-mirror", runner="tests"),
        "number": pr932(number=931, runner="tests"),
        "head repo": pr932(head_repo="someone/GRID", runner="tests"),
        "base": pr932(base_ref="develop", runner="tests"),
        "head_ref": pr932(head_ref="claude/brave-feynman-f63cd4", runner="tests"),
    }
    for term, ctx in flips.items():
        assert routing(workflow, ctx) == SELF_HOSTED_ROUTING, term
        assert routing(workflow, ctx) == baseline_routing(workflow, ctx), term


# --------------------------------------------------------------------------
# Negative controls: everything else routes exactly as before.
# --------------------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(NEGATIVE_CONTROLS))
@pytest.mark.parametrize("runner", ["", "tests"], ids=["TEST_RUNNER empty", "TEST_RUNNER set"])
def test_negative_controls_route_as_before(workflow: dict[str, Any], name: str, runner: str) -> None:
    ctx = _with_runner(NEGATIVE_CONTROLS[name], runner)
    decided = routing(workflow, ctx)
    assert decided == baseline_routing(workflow, ctx), name
    assert decided == (SELF_HOSTED_ROUTING if runner else HOSTED_ROUTING), name


@pytest.mark.parametrize("name", sorted(PRIOR_HOSTED_EVENTS))
@pytest.mark.parametrize("runner", ["", "tests"], ids=["TEST_RUNNER empty", "TEST_RUNNER set"])
def test_prior_hosted_alternatives_are_preserved(workflow: dict[str, Any], name: str, runner: str) -> None:
    ctx = _with_runner(PRIOR_HOSTED_EVENTS[name], runner)
    decided = routing(workflow, ctx)
    assert decided == HOSTED_ROUTING, name
    assert decided == baseline_routing(workflow, ctx), name


# --------------------------------------------------------------------------
# Evaluator self-checks (so a wrong evaluator cannot fake the rows above).
# --------------------------------------------------------------------------


def test_evaluator_semantics() -> None:
    assert evaluate("'a' == 'A'", {}) is True
    assert evaluate("github.event.pull_request.number == 932", push("refs/heads/main")) is False
    assert evaluate("null == 0", {}) is True
    assert evaluate("'' == 0", {}) is True
    assert evaluate("'x' == 0", {}) is False
    assert evaluate("!''", {}) is True
    assert evaluate("'' || 'b'", {}) == "b"
    assert evaluate("'a' && 'b'", {}) == "b"
    assert evaluate("false && 'b'", {}) is False
    assert evaluate("(false || 'c') && fromJSON('[\"self-hosted\",\"tests\"]')", {}) == SELF_HOSTED
    assert evaluate("vars.TEST_RUNNER != ''", {"vars": {"TEST_RUNNER": ""}}) is False
    assert evaluate("env.ON_SELF_HOSTED != 'true'", {"env": {"ON_SELF_HOSTED": "false"}}) is True
    with pytest.raises(ValueError):
        evaluate("github.ref == 'a' ?? 1", {})
