import {test} from 'node:test';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {mkdtempSync,readFileSync,writeFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';

test('actual engine feature capture is deterministic and includes every declared zodiac state', () => {
    const root = fileURLToPath(new URL('../../',import.meta.url));
    const temp = mkdtempSync(join(tmpdir(),'astrogrid-feature-test-'));
    try {
        // Synthetic Sun fixture checks feature contracts, not astronomical accuracy.
        const dates = ['2024-01-02','2024-07-02'];
        writeFileSync(join(temp,'dates.json'),JSON.stringify(dates));
        writeFileSync(join(temp,'sun.json'),JSON.stringify(Object.fromEntries(dates.map((d) => [d,{
            geocentric_longitude:280,zodiac_sign:'Capricorn',is_retrograde:false}]))));
        const run = (name) => {
            const result = spawnSync(process.execPath,['--experimental-default-type=module',
                'scripts/astrogrid_celestial_features.mjs',join(temp,'dates.json'),join(temp,name),join(temp,'sun.json')],
                {cwd:root,encoding:'utf8'});
            assert.equal(result.status,0,result.stderr);
            return JSON.parse(readFileSync(join(temp,name),'utf8'));
        };
        const one = run('one.json'), two = run('two.json');
        assert.deepEqual(one.records,two.records);
        for (const row of one.records) {
            assert.equal(Object.keys(row.engine).length,12);
            assert.equal(Object.keys(row.sky).length,245);
            for (const body of ['Sun','Moon','Mercury','Venus','Mars','Jupiter','Saturn','Uranus','Neptune','Pluto','Rahu','Ketu']) {
                const states = Object.entries(row.sky).filter(([key]) => key.startsWith(`${body}_zodiac_`));
                assert.equal(states.length,12);
                assert.equal(states.reduce((n,[,v]) => n+v,0),1);
            }
            assert.ok(Object.values(row.sky).every(Number.isFinite));
        }
        writeFileSync(join(temp,'sun.json'),'{}');
        const failed = spawnSync(process.execPath,['--experimental-default-type=module',
            'scripts/astrogrid_celestial_features.mjs',join(temp,'dates.json'),join(temp,'bad.json'),join(temp,'sun.json')],
            {cwd:root,encoding:'utf8'});
        assert.notEqual(failed.status,0);
    } finally {
        rmSync(temp,{recursive:true,force:true});
    }
});
