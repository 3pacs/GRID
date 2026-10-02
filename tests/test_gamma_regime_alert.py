"""Execute the actual browser state machine, independently of DOM rendering."""
import json
from pathlib import Path
import re
import shutil
import subprocess

import pytest


def test_gamma_regime_state_machine():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node required to execute browser JavaScript")
    page = Path(__file__).resolve().parents[1] / "collectors/gamma_watch/index.html"
    code = re.search(r'<script id="gamma-regime-alert">(.*?)</script>', page.read_text(encoding="utf-8"), re.S)[1]
    checks = r"""
const assert=require('node:assert/strict');
const now=Date.parse('2026-10-02T16:00:00Z');
function state(min,sign='long') {return {gex:{as_of:new Date(now-min*60000).toISOString(),received_at:new Date(now-1000).toISOString(),net_gex_at_spot:sign==='long'?1e9:-1e9,spot:769,source:'ZeroGEX delayed',regime:sign,market_phase:'open',gamma_flip:766}};}
const initial=gammaRegimeStep(null,state(18,'short'),now);
assert.equal(initial.status,'baseline');assert.equal(initial.event,null);
const change=gammaRegimeStep(initial.baseline,state(16,'long'),now+1000);
assert.equal(change.event.after,'positive');assert.equal(change.event.before,'negative');
assert.equal(gammaRegimeStep(change.baseline,state(16),now+2000).event,null);
assert.equal(gammaRegimeStep(change.baseline,state(40),now+2000).baseline,null);
assert.equal(gammaRegimeStep(null,state(15),now+2000).event,null);
const failed=state(15);failed.gex_error='refresh failed';assert.equal(gammaRegimeStep(change.baseline,failed,now+2000).status,'unavailable');
const mismatch=state(15);mismatch.gex.net_gex_at_spot=-1;assert.equal(gammaRegimeStep(change.baseline,mismatch,now+2000).status,'unavailable');
assert.equal(gammaRegimeStep(change.baseline,state(17,'short'),now+2000).status,'out_of_order');
assert.equal(gammaRegimeStep(change.baseline,state(16,'short'),now+2000).status,'revision_requires_baseline');
assert.equal(gammaRegimeStep(change.baseline,state(15,'short'),now+200000).event,null);
const source=state(15,'short');source.gex.source='Other vendor';assert.equal(gammaRegimeStep(change.baseline,source,now+2000).event,null);
const gap=state(10,'short');assert.equal(gammaRegimeStep(change.baseline,gap,now+2000).event,null);
const future=state(-1);assert.equal(gammaRegimeStep(change.baseline,future,now+2000).status,'unavailable');
const zero=state(15);zero.gex.net_gex_at_spot=0;assert.equal(gammaRegimeStep(change.baseline,zero,now+2000).status,'unavailable');
const closed=state(15);closed.gex.market_phase='closed';assert.equal(gammaRegimeStep(change.baseline,closed,now+2000).status,'unavailable');
console.log(JSON.stringify({controls:14,status:'passed'}));
"""
    result = subprocess.run([node, "-e", code + checks], check=True, capture_output=True, text=True)
    assert json.loads(result.stdout)["status"] == "passed"


def test_banner_change_remains_visible_until_acknowledged():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node required to execute browser JavaScript")
    page = Path(__file__).resolve().parents[1] / "collectors/gamma_watch/index.html"
    code = re.search(r'<script id="gamma-regime-alert">(.*?)</script>', page.read_text(encoding="utf-8"), re.S)[1]
    setup = r"""
const assert=require('node:assert/strict');let clock=Date.parse('2026-10-02T16:00:00Z');Date.now=()=>clock;
const intervals=[];global.setInterval=fn=>intervals.push(fn);
const freshness={before:node=>freshness.banner=node};
global.document={createElement:tag=>({tag,style:{},children:[],setAttribute(){},append(...nodes){this.children.push(...nodes);}})};
const $=id=>freshness,et=x=>new Date(x).toISOString(),fmt=x=>String(x),ageText=x=>'delayed';
function sample(minutes,sign){return {gex:{as_of:new Date(clock-minutes*60000).toISOString(),received_at:new Date(clock-1000).toISOString(),net_gex_at_spot:sign==='long'?1e9:-1e9,spot:769,source:'ZeroGEX delayed',regime:sign,market_phase:'open',gamma_flip:766}};}
let state=sample(18,'short');
"""
    checks = r"""
const [heading,details,ack,sound]=freshness.banner.children;
assert.equal(heading.textContent,'MODELED NEGATIVE GAMMA');assert.equal(ack.hidden,true);
clock+=1000;state=sample(16,'long');intervals[0]();
assert.equal(heading.textContent,'GAMMA SWITCH: NEGATIVE → POSITIVE');assert.equal(ack.hidden,false);
assert.match(details.textContent,/delayed 16m/);assert.match(details.textContent,/direction unconfirmed/);
intervals[0]();assert.match(heading.textContent,/GAMMA SWITCH/);
ack.onclick();assert.equal(heading.textContent,'MODELED POSITIVE GAMMA');assert.equal(ack.hidden,true);
state.gex_error='failed';intervals[0]();assert.equal(heading.textContent,'GAMMA CONTEXT UNAVAILABLE');
state=sample(15,'short');intervals[0]();assert.equal(heading.textContent,'MODELED NEGATIVE GAMMA');
assert.equal(sound.disabled,undefined);console.log('passed');
"""
    result = subprocess.run([node, "-e", setup + code + checks], check=True, capture_output=True, text=True)
    assert result.stdout.strip() == "passed"
