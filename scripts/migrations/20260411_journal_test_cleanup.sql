-- Journal test cleanup
--
-- The original prevent_journal_delete() trigger unconditionally blocks
-- DELETE, which prevents tests from cleaning up after themselves. This
-- migration replaces it with a version that allows DELETE only when BOTH
-- of the following are true:
--
--   1. Session has set `app.journal_testing = 'on'` (via SET LOCAL so it
--      cannot leak outside the current transaction), AND
--   2. The row's `annotation = 'TEST_JOURNAL'` sentinel.
--
-- Production code never sets the GUC and never writes the TEST_JOURNAL
-- annotation, so production immutability is preserved. Tests can now
-- clean up their own rows without weakening the append-only contract.

BEGIN;

CREATE OR REPLACE FUNCTION prevent_journal_delete()
RETURNS TRIGGER AS $$
DECLARE
    testing_mode TEXT;
BEGIN
    -- current_setting(name, true) returns NULL when the GUC is unset rather
    -- than raising, so tests can opt in explicitly via SET LOCAL.
    testing_mode := current_setting('app.journal_testing', true);

    IF testing_mode = 'on' AND OLD.annotation = 'TEST_JOURNAL' THEN
        RETURN OLD;   -- permit DELETE of test-tagged rows in test sessions
    END IF;

    RAISE EXCEPTION 'decision_journal is append-only: DELETE is not permitted. '
                    'Row id=% cannot be deleted.', OLD.id;
END;
$$ LANGUAGE plpgsql;

COMMIT;
