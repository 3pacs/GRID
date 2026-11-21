// Server-side run of the shipped AstroGrid lens engines over one sky snapshot.
//
// The twelve lens engines live only in astrogrid_shared/prophecy.js; the
// browser runs them and so does scripts/astrogrid_celestial_features.mjs.
// This runner lets the scheduled celestial cycle (oracle/astrogrid_cycle.py)
// run the SAME code instead of a Python copy that would drift.
//
// Input (stdin):  {"snapshot": {...}, "lens_ids": [...], "mode": "chorus"}
// Output (stdout): {"engine_outputs": [...], "code_sha256": {path: sha256}}
//
// Pure function of its input: no network, no files written, no clock read
// when the snapshot carries a timestamp or date. The caller pins the
// timestamp so every run over the same snapshot gives the same rows.
//
// Run: node --experimental-default-type=module scripts/astrogrid_engine_outputs.mjs < in.json
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {dirname, join} from 'node:path';
import {computeEngineOutputs} from '../astrogrid_shared/prophecy.js';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const input = JSON.parse(readFileSync(0, 'utf8'));
const snapshot = input && typeof input.snapshot === 'object' ? input.snapshot : null;
if (!snapshot || !(snapshot.timestamp || snapshot.date)) {
    // Without a timestamp the engines would seed their phrasing from the
    // wall clock, and two runs over one sky would disagree.
    throw new Error('snapshot with a timestamp or date is required');
}
const lensIds = Array.isArray(input.lens_ids) ? input.lens_ids : [];
const mode = typeof input.mode === 'string' && input.mode ? input.mode : 'chorus';

const engineOutputs = computeEngineOutputs(snapshot, lensIds, mode);
const code = ['astrogrid_shared/prophecy.js', 'astrogrid_shared/snapshot.js'];
const codeSha256 = Object.fromEntries(code.map((path) => [
    path,
    createHash('sha256').update(readFileSync(join(root, path))).digest('hex'),
]));
process.stdout.write(JSON.stringify({engine_outputs: engineOutputs, code_sha256: codeSha256}));
