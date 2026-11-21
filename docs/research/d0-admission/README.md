# D0 admission signatures

These files feed the D0 admission verifier, `analysis/d0_admission.py` (contract §8, §9 G14).

- `allowed_signers`: the public keys trusted to admit a hypothesis. It ships with **no keys**. The reviewer and the operator each add their own key, under the principal `reviewer` or `operator`, in a reviewed commit. A key is never added on someone else's behalf.
- `<hypothesis>.<principal>.sig`: one SSH signature per hypothesis and principal.

A hypothesis is admitted only when all of these hold:
1. `VERIFIER_ACCEPTED` is `True` in `analysis/d0_admission.py`. It is flipped only after the reviewer accepts the verifier, in its own commit.
2. The hypothesis is one of the supported ones in `FROZEN_SPEC` (`analysis/d0_admission.py`, pinned in `FROZEN_SPEC_SHA256`). Its required inputs, floors, level, window, coverage scope and blocking gaps come from that frozen spec, never from the manifest, and the manifest's copies must agree with it.
3. The manifest carries everything the verifier lists for that hypothesis:
   - `required_inputs`: exactly the frozen ids, each one admitted;
   - `coverage.measured_n` and a typed coverage receipt (`d0-coverage-receipt/1`: hypothesis, frozen scope, unit, inputs, the same count; D1 adds per-horizon cells and the G15 closure hash);
   - `blocking_gaps`, including every frozen gap, each with a typed `closure` (`state: "CLOSED"`, `closed_at`, an `evidence` receipt of a `d0-gap-closure/1` file). A `status` string never closes a gap. G15's evidence also carries the typed price-only exception;
   - `admission.signed_at`;
   - `reviewer.signed` exactly `true`, with `manifest_sha256`.
4. Both principals have signed the hypothesis's statement.

## Signing one hypothesis

Signing comes last: set every manifest field first, because any later manifest edit invalidates the signatures. The reviewer fields `signed`, `signed_at` and `manifest_sha256` are excluded from the digest, so they can be recorded afterwards.

```bash
python -m analysis.d0_admission statement D2-H2 > /tmp/D2-H2.statement
ssh-keygen -Y sign -f ~/.ssh/<your-key> -n grid-d0-admission /tmp/D2-H2.statement
cp /tmp/D2-H2.statement.sig docs/research/d0-admission/D2-H2.<reviewer|operator>.sig
python -m analysis.d0_manifest   # must print OK
```

`python -m analysis.d0_admission digest` prints the manifest digest that the statement binds.
