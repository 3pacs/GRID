"""Offline #933 routing controls; no runner, database or network is contacted.

Reuse the retained #932 expression evaluator. Compare the actual four workflow
expressions against their exact pre-#933 templates, including every conjunction
term and TEST_RUNNER variants. This models routing, not hosted execution.
"""

from __future__ import annotations

import copy
import importlib.util
import itertools
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "_pr933_routing_evaluator", ROOT / "tests/test_ci_test_workflow_pr932_routing.py"
)
assert spec and spec.loader
prior = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prior)
BRANCH = "claude/release-dir-mode-0755-20261031"
PRE_933 = prior._templates(f"({prior.PRIOR_ALTERNATIVES} || {prior.PR932_CONJUNCTION})")


@pytest.fixture(scope="module")
def workflow():
    return yaml.safe_load((ROOT / ".github/workflows/test.yml").read_text())


def before(workflow, ctx):
    original = copy.deepcopy(workflow)
    original["env"]["ON_SELF_HOSTED"] = "${{ " + PRE_933["ON_SELF_HOSTED"] + " }}"
    for job in prior.JOBS:
        original["jobs"][job]["runs-on"] = "${{ " + PRE_933["runs-on"] + " }}"
    return prior.routing(original, ctx)


# 4 * 2**5 * 3 = 384 modeled event contexts, independently deciding whether
# the complete #933 tuple is present. False-looking nonempty variables matter.
MATRIX = list(itertools.product(
    ("pull_request", "pull_request_target", "push", "workflow_dispatch"),
    ("3pacs/GRID", "other/GRID"), ("3pacs/GRID", "fork/GRID"),
    (933, 934), ("main", "develop"), (BRANCH, "feature/other"),
    ("", "tests", "false"),
))


@pytest.mark.parametrize("event,repo,head_repo,number,base,branch,runner", MATRIX)
def test_all_terms_and_runner_variants(workflow, event, repo, head_repo, number, base, branch, runner):
    ctx = prior.pull_request(number, branch, event_name=event, repository=repo,
                             head_repo=head_repo, base_ref=base, runner=runner)
    matches = (event, repo, head_repo, number, base, branch) == (
        "pull_request", "3pacs/GRID", "3pacs/GRID", 933, "main", BRANCH)
    actual = prior.routing(workflow, ctx)
    assert actual == (prior.HOSTED_ROUTING if matches else before(workflow, ctx))


@pytest.mark.parametrize("runner", ["", "tests", "false"])
def test_prior_routes_preserved(workflow, runner):
    contexts = list(prior.PRIOR_HOSTED_EVENTS.values()) + [
        prior.pr932(), prior.push("refs/heads/develop"), prior.push(f"refs/heads/{BRANCH}"),
        prior.pull_request(999, "ordinary"),
    ]
    for ctx in contexts:
        ctx = prior._with_runner(ctx, runner)
        assert prior.routing(workflow, ctx) == before(workflow, ctx)


@pytest.mark.parametrize("missing", ["repository", "head_ref", "event_name", "number", "head", "base"])
def test_missing_binding_cannot_select_933(workflow, missing):
    ctx = prior.pull_request(933, BRANCH, runner="tests")
    target = ctx["github"] if missing in ("repository", "head_ref", "event_name") else ctx["github"]["event"]["pull_request"]
    target.pop(missing)
    assert prior.routing(workflow, ctx) == before(workflow, ctx) == prior.SELF_HOSTED_ROUTING


def test_variable_absent_still_forces_disposable_for_exact_pr(workflow):
    ctx = prior.pull_request(933, BRANCH)
    ctx.pop("vars")
    assert prior.routing(workflow, ctx) == prior.HOSTED_ROUTING


def test_four_expressions_and_cleanup(workflow):
    text = (ROOT / ".github/workflows/test.yml").read_text()
    assert text.count(prior.PR933_CONJUNCTION) == 4
    assert prior._strip(workflow["env"]["ON_SELF_HOSTED"]) == prior.CANDIDATE["ON_SELF_HOSTED"]
    for job in prior.JOBS:
        assert prior._strip(workflow["jobs"][job]["runs-on"]) == prior.CANDIDATE["runs-on"]
    step = prior._step(workflow, "Stop ephemeral Postgres (hosted runner)")
    assert step["if"] == "always() && env.ON_SELF_HOSTED != 'true'"
    assert "docker rm -f grid-test-pg" in step["run"]
    # Existing cleanup masks docker-rm failure; VM disposal is not a verified
    # per-run cleanup receipt, so this test deliberately makes no such claim.


def test_other_automatic_workflow_boundaries():
    def read(name):
        return yaml.safe_load((ROOT / ".github/workflows" / name).read_text())
    for name in ("oct25-native-compatibility.yml", "quiver-synthetic-pg.yml"):
        data = read(name)
        assert data.get("on", data.get(True)) == {"pull_request": {"branches": ["main"]}}
        assert all(str(job["runs-on"]).startswith("ubuntu-") for job in data["jobs"].values())
        assert data["permissions"] == {"contents": "read"}
    claude = read("claude-code-review.yml")
    assert claude["jobs"]["claude-review"]["if"] == "${{ vars.CLAUDE_REVIEW_ENABLED == 'true' }}"
    assert "environment" not in claude["jobs"]["claude-review"]
    deploy = read("deploy.yml")
    assert "pull_request" not in deploy.get("on", deploy.get(True))
    freeze = read("evals-freeze.yml")
    assert "pull_request_target" in freeze.get("on", freeze.get(True))
    assert freeze["permissions"] == {"contents": "read"}


def test_removed_term_negative_controls_are_detectable(workflow):
    """A predicate weakened by dropping any binding term must change a case."""
    flips = [
        ("github.event_name == 'pull_request'", {"event_name": "pull_request_target"}),
        ("github.repository == '3pacs/GRID'", {"repository": "other/GRID"}),
        ("github.event.pull_request.number == 933", {"number": 934}),
        ("github.event.pull_request.head.repo.full_name == '3pacs/GRID'", {"head_repo": "fork/GRID"}),
        ("github.event.pull_request.base.ref == 'main'", {"base_ref": "develop"}),
        (f"github.head_ref == '{BRANCH}'", {"head_ref": "feature/other"}),
    ]
    for term, overrides in flips:
        args = dict(number=933, head_ref=BRANCH, runner="tests")
        args.update(overrides)
        ctx = prior.pull_request(**args)
        mutant = copy.deepcopy(workflow)
        weakened = prior.PR933_CONJUNCTION.replace(term, "true")
        for obj, key in [(mutant["env"], "ON_SELF_HOSTED")] + [
            (mutant["jobs"][job], "runs-on") for job in prior.JOBS
        ]:
            obj[key] = obj[key].replace(prior.PR933_CONJUNCTION, weakened)
        assert prior.routing(workflow, ctx) == prior.SELF_HOSTED_ROUTING
        assert prior.routing(mutant, ctx) == prior.HOSTED_ROUTING
