"""Strict prospective workflow/output capability handoff; production CLI absent.

Product modules are separately owned proposals. Their composition and output
review status are not upgraded by this file. Missing state NEVER chooses legacy.
"""
from copy import deepcopy
from pathlib import Path, PurePosixPath
from api_bundle_contract import require, sha


def outside(root,bundle):
    require(isinstance(root,str) and root.startswith('/') and '\\' not in root,'absolute state root')
    p=PurePosixPath(root); b=PurePosixPath(bundle)
    require(isinstance(bundle,str) and b.is_absolute() and str(b)==bundle and '..' not in b.parts,'canonical source root')
    require(str(p)==root and '..' not in p.parts and p!=b and b not in p.parents and p not in b.parents,
            'state/source overlap')
    return root


def state_contract(binding,bundle):
    require(type(binding) is dict and set(binding)=={'schema','workflow','output','composition_receipt_sha256'},'state binding schema')
    require(binding['schema']=='grid.api-state-handoff.v1' and sha(binding['composition_receipt_sha256']),'composition binding')
    workflow=binding['workflow']; output=binding['output']
    require(type(workflow) is dict and set(workflow)=={'state_root','receipt_sha256','source_identity'},'workflow capability fields')
    require(sha(workflow['receipt_sha256']) and isinstance(workflow['source_identity'],str) and workflow['source_identity'].strip(),'workflow identity')
    require(type(output) is dict and set(output)=={'state_root','receipt_sha256'},'output capability fields')
    require(sha(output['receipt_sha256']),'output admission receipt')
    outside(workflow['state_root'],bundle); outside(output['state_root'],bundle)
    # Keep the independently governed workflow selection and mutable output
    # namespace disjoint. No state selection/history is generated here.
    outside(output['state_root'],workflow['state_root'])
    return deepcopy(binding)


def bind_app(binding,bundle,authority,imports):
    """Validate before API import; configure both capabilities before returning app.

The runner must accept ONLY this returned app, never import api.main:app as a
fallback. authority verifies exact composed product blobs plus state UID/ancestry,
selection/content receipts and native custody before imports or configuration.
imports is the admitted module loader, fake in current tests. No server starts.
"""
    bound=state_contract(binding,bundle)
    require(authority.verify_state_handoff(bound,bundle) is True,'state handoff authority refused')
    loader=imports('workflows.loader')
    output=imports('api.routers.ollama')
    # Validate complete selection before importing the application/lifespan.
    loader.read_api_selection(deepcopy(bound['workflow']))
    output.configure_api_output_state(Path(bound['output']['state_root']))
    main=imports('api.main')
    app=main.app
    existing=getattr(app.state,'workflow_state_binding',None)
    require(existing is None or existing==bound['workflow'],'conflicting workflow binding')
    app.state.workflow_state_binding=deepcopy(bound['workflow'])
    require(app.state.workflow_state_binding==bound['workflow'],'workflow capability not attached')
    return app


def serve_bound(binding,bundle,authority,imports,serve):
    """Explicit injected server handoff; no default uvicorn import or CLI."""
    app=bind_app(binding,bundle,authority,imports)
    return serve(app)


def render_bound_program(spec,binding):
    """Fixed bootstrap flow; no authority-supplied arbitrary server program.

api_isolation_runtime_admission is deliberately NOT supplied by this proposal.
Its exact trusted authority factory must be implemented/admitted before use;
missing provider/import failure halts before API import or socket startup.
"""
    bound=state_contract(binding,spec['release_root'])
    paths=[spec['cwd']+'/scripts',*spec['sys_path_exact']]
    return ("import sys,importlib;sys.path="+repr(paths)+";"
            "from api_isolation_bootstrap import serve_bound;"
            "from api_isolation_runtime_admission import load_authority;"
            "authority=load_authority();"
            "serve_bound("+repr(bound)+","+repr(spec['release_root'])+",authority,importlib.import_module,"
            "lambda app: importlib.import_module('uvicorn').run(app,host='0.0.0.0',port=8000))")
