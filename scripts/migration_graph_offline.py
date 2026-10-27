"""Read Alembic graph literals with ast; never import or execute revision bodies."""
import ast
import hashlib
import json
from pathlib import Path


FIELDS = {"revision", "down_revision", "depends_on", "branch_labels"}


def graph(directory):
    return graph_sources({p.name: p.read_bytes() for p in Path(directory).glob("*.py")})


def graph_sources(sources):
    nodes = {}
    files = {}
    for name, raw in sorted(sources.items()):
        source = Path(name)
        tree = ast.parse(raw, filename=source.name)
        values = {}
        # Reject assignments to graph metadata in conditional/nested constructs.
        allowed = set()
        for statement in tree.body:
            if isinstance(statement, ast.Expr) and not isinstance(statement.value, ast.Constant):
                raise ValueError("top-level executable expression " + source.name)
            if not isinstance(statement, (ast.Expr, ast.Import, ast.ImportFrom, ast.Assign, ast.AnnAssign, ast.FunctionDef)):
                raise ValueError("unresolved top-level construct " + source.name)
            targets = statement.targets if isinstance(statement, ast.Assign) else [statement.target] if isinstance(statement, ast.AnnAssign) else []
            for target in targets:
                if isinstance(target, ast.Name) and target.id in FIELDS:
                    if target.id in values:
                        raise ValueError("duplicate metadata " + source.name)
                    values[target.id] = ast.literal_eval(statement.value)
                    allowed.add(id(target))
        for target in ast.walk(tree):
            if isinstance(target, ast.Name) and isinstance(target.ctx, ast.Store) and target.id in FIELDS and id(target) not in allowed:
                raise ValueError("dynamic metadata " + source.name)
        if "revision" not in values or "down_revision" not in values:
            raise ValueError("missing metadata " + source.name)
        rev = values["revision"]
        if not isinstance(rev, str) or not rev or rev in nodes:
            raise ValueError("duplicate/invalid revision " + source.name)
        for field in FIELDS - {"revision"}:
            value = values.setdefault(field, None)
            if value is not None and not isinstance(value, str) and not (isinstance(value, (list, tuple)) and all(isinstance(x, str) for x in value)):
                raise ValueError("invalid edges " + source.name)
        nodes[rev] = values
        files[source.name] = hashlib.sha256(raw).hexdigest()
    def refs(v):
        return [] if v is None else [v] if isinstance(v, str) else list(v)
    predecessors = {r: refs(n["down_revision"]) + refs(n["depends_on"]) for r, n in nodes.items()}
    seen, active = set(), set()
    def visit(r):
        if r not in nodes or r in active:
            raise ValueError("missing parent/cycle " + r)
        if r in seen:
            return
        active.add(r)
        for parent in predecessors[r]:
            visit(parent)
        active.remove(r)
        seen.add(r)
    for r in nodes:
        visit(r)
    return {"nodes": nodes, "files": files,
            "heads": sorted(set(nodes) - {r for v in predecessors.values() for r in v}),
            "installed_state": "UNKNOWN_NO_DATABASE_OBSERVATION",
            "executed_revision_bodies": 0}


if __name__ == "__main__":
    import sys
    print(json.dumps(graph(sys.argv[1]), sort_keys=True, indent=2))
