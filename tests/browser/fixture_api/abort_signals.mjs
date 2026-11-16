// Local acceptance-gate negative control. It uses the ordinary fixture runner
// and real product UI, with only the Signals transport failed by the browser.
import { main } from '../run_evidence.mjs';

await main({ requestFailureControl: url =>
    url.pathname === '/api/v1/options/signals' ? 'connectionfailed' : null });
