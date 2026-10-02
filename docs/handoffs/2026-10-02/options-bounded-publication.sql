-- REVIEW PACKET ONLY. No Alembic registration and no automatic deploy apply.
-- Root must package/sequence this schema contract before the new writer runs.
-- Execute as one short DDL transaction: 15 trigger-contract DATA rows, no
-- legacy backfill, no legacy source/image rewrite. Include any migration stamp
-- in the aggregate budget. Fail closed; never replay an unknown COMMIT ACK.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE options_capture_batches RENAME TO options_capture_batches_all;
ALTER TABLE options_capture_batches_all ADD COLUMN requires_publication boolean NOT NULL DEFAULT false;

CREATE TABLE options_capture_publications (
    capture_batch_id text PRIMARY KEY REFERENCES options_capture_batches_all(capture_batch_id),
    published_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER options_capture_publications_no_row_mutation
    BEFORE UPDATE OR DELETE ON options_capture_publications
    FOR EACH ROW EXECUTE FUNCTION options_capture_append_only_guard();
CREATE TRIGGER options_capture_publications_no_truncate
    BEFORE TRUNCATE ON options_capture_publications
    FOR EACH STATEMENT EXECUTE FUNCTION options_capture_append_only_guard();

CREATE FUNCTION options_bounded_contract_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE h options_capture_batches_all;
BEGIN
    SELECT * INTO STRICT h FROM options_capture_batches_all
        WHERE capture_batch_id = NEW.capture_batch_id FOR UPDATE;
    IF h.requires_publication THEN
        IF current_setting('transaction_isolation') <> 'read committed' THEN
            RAISE EXCEPTION 'prepared options writes require READ COMMITTED';
        END IF;
        IF EXISTS (SELECT 1 FROM options_capture_publications
                   WHERE capture_batch_id = NEW.capture_batch_id) THEN
            RAISE EXCEPTION 'published options batch is sealed';
        END IF;
        IF NEW.capture_started_at <> h.capture_started_at
            OR NEW.capture_completed_at <> h.capture_completed_at
            OR NEW.provider_regular_market_at > h.capture_completed_at THEN
            RAISE EXCEPTION 'options contract provenance mismatch';
        END IF;
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER options_bounded_contract_guard BEFORE INSERT ON options_snapshots_all
    FOR EACH ROW EXECUTE FUNCTION options_bounded_contract_guard();

CREATE FUNCTION options_bounded_completion_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE h options_capture_batches_all; n bigint;
BEGIN
    SELECT * INTO STRICT h FROM options_capture_batches_all
        WHERE capture_batch_id = NEW.capture_batch_id FOR UPDATE;
    IF NOT h.requires_publication THEN
        RAISE EXCEPTION 'legacy atomic batch does not take a completion receipt';
    END IF;
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'options completion requires READ COMMITTED';
    END IF;
    SELECT count(*) INTO n FROM options_snapshots_all
        WHERE capture_batch_id = h.capture_batch_id;
    IF n <> h.row_count OR EXISTS (
        SELECT 1 FROM options_snapshots_all s WHERE s.capture_batch_id = h.capture_batch_id
          AND (s.capture_ordinal <> h.capture_ordinal OR s.ticker <> h.ticker
               OR s.snap_date <> h.snap_date OR s.capture_started_at <> h.capture_started_at
               OR s.capture_completed_at <> h.capture_completed_at
               OR s.provider_regular_market_at > h.capture_completed_at)) THEN
        RAISE EXCEPTION 'incomplete or mismatched options batch';
    END IF;
    NEW.published_at := clock_timestamp();
    RETURN NEW;
END
$$;
CREATE TRIGGER options_bounded_completion_guard BEFORE INSERT ON options_capture_publications
    FOR EACH ROW EXECUTE FUNCTION options_bounded_completion_guard();

-- Existing header insert shape remains automatically updatable. Old writers
-- omit requires_publication, get false, and still register atomically.
-- This compatibility DOES NOT make their unbounded transactions acceptable.
CREATE VIEW options_capture_batches AS
SELECT h.capture_batch_id, h.ticker, h.snap_date, h.capture_ordinal,
       h.capture_started_at, h.capture_completed_at, h.row_count, h.spot_price,
       h.capture_source, h.backfilled,
       CASE WHEN h.requires_publication THEN
           (SELECT p.published_at FROM options_capture_publications p
            WHERE p.capture_batch_id = h.capture_batch_id)
           ELSE h.registered_at END AS registered_at
FROM options_capture_batches_all h
WHERE NOT h.requires_publication OR EXISTS (
    SELECT 1 FROM options_capture_publications p WHERE p.capture_batch_id = h.capture_batch_id
);

CREATE OR REPLACE VIEW options_snapshots AS
SELECT s.id, s.ticker, s.snap_date, s.expiry, s.opt_type, s.strike,
       s.last_price, s.bid, s.ask, s.volume, s.open_interest,
       s.implied_vol, s.in_the_money, s.created_at,
       s.capture_batch_id, s.capture_ordinal, s.capture_started_at,
       s.capture_completed_at, s.provider_regular_market_at
FROM options_snapshots_all s
WHERE NOT EXISTS (
    SELECT 1 FROM options_capture_batches_all h
    WHERE h.capture_batch_id = s.capture_batch_id AND h.requires_publication
      AND NOT EXISTS (SELECT 1 FROM options_capture_publications p
                      WHERE p.capture_batch_id = h.capture_batch_id)
)
AND NOT EXISTS (
    SELECT 1 FROM options_capture_batches b
    WHERE b.ticker = s.ticker AND b.snap_date = s.snap_date
      AND b.capture_batch_id IS DISTINCT FROM s.capture_batch_id
      AND b.capture_ordinal >= COALESCE(s.capture_ordinal, 0)
);

-- Only the bounded writer opts in. This is not global writer/role hardening.
-- Known triggers have no DATA sidewrites. Unknown/changed trigger closures
-- fail before writes while ROW EXCLUSIVE locks prevent concurrent trigger DDL.
CREATE FUNCTION options_bounded_row_budget() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE n integer;
BEGIN
    IF current_setting('grid.options_bounded', true) = 'on' THEN
        n := COALESCE(NULLIF(current_setting('grid.options_rows', true), ''), '0')::integer + 1;
        IF n > 50 THEN RAISE EXCEPTION 'options DATA budget exceeded'; END IF;
        PERFORM set_config('grid.options_rows', n::text, true);
    END IF;
    RETURN NULL;
END
$$;
DO $$
DECLARE t text;
BEGIN
    FOREACH t IN ARRAY ARRAY['options_capture_batches_all','options_snapshots_all',
        'options_capture_publications','options_daily_signals','feature_registry',
        'resolved_series','source_catalog'] LOOP
        EXECUTE format('CREATE TRIGGER options_bounded_row_budget AFTER INSERT OR UPDATE OR DELETE ON %I
            FOR EACH ROW EXECUTE FUNCTION options_bounded_row_budget()', t);
    END LOOP;
END
$$;

CREATE TABLE options_bounded_trigger_contracts (
    table_name text NOT NULL, trigger_name text NOT NULL,
    function_hash text NOT NULL, trigger_hash text NOT NULL,
    PRIMARY KEY (table_name, trigger_name)
);
CREATE TRIGGER options_bounded_trigger_contracts_no_row_mutation
    BEFORE UPDATE OR DELETE ON options_bounded_trigger_contracts
    FOR EACH ROW EXECUTE FUNCTION options_capture_append_only_guard();
CREATE TRIGGER options_bounded_trigger_contracts_no_truncate
    BEFORE TRUNCATE ON options_bounded_trigger_contracts
    FOR EACH STATEMENT EXECUTE FUNCTION options_capture_append_only_guard();
-- Snapshot only the named, source-reviewed functions. An extra trigger is
-- deliberately absent from this whitelist and will fail closure validation.
INSERT INTO options_bounded_trigger_contracts
SELECT c.relname, t.tgname, md5(pg_get_functiondef(t.tgfoid)), md5(pg_get_triggerdef(t.oid))
FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_proc p ON p.oid = t.tgfoid
WHERE NOT t.tgisinternal AND c.oid = ANY(ARRAY[
    'options_capture_batches_all'::regclass,'options_snapshots_all'::regclass,
    'options_capture_publications'::regclass,'options_daily_signals'::regclass,
    'feature_registry'::regclass,'resolved_series'::regclass,'source_catalog'::regclass])
AND p.proname IN ('options_capture_append_only_guard', 'options_bounded_contract_guard',
                 'options_bounded_completion_guard', 'options_bounded_row_budget');

CREATE FUNCTION options_bounded_assert_trigger_closure() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
        LEFT JOIN options_bounded_trigger_contracts a
          ON a.table_name = c.relname AND a.trigger_name = t.tgname
        WHERE NOT t.tgisinternal AND c.oid = ANY(ARRAY[
            'options_capture_batches_all'::regclass,'options_snapshots_all'::regclass,
            'options_capture_publications'::regclass,'options_daily_signals'::regclass,
            'feature_registry'::regclass,'resolved_series'::regclass,'source_catalog'::regclass])
          AND (a.function_hash IS NULL OR a.function_hash <> md5(pg_get_functiondef(t.tgfoid))
               OR a.trigger_hash <> md5(pg_get_triggerdef(t.oid)) OR t.tgenabled <> 'O')
    ) OR EXISTS (
        SELECT 1 FROM options_bounded_trigger_contracts a
        WHERE NOT EXISTS (SELECT 1 FROM pg_trigger t
                          WHERE t.tgrelid = to_regclass(a.table_name)
                            AND t.tgname = a.trigger_name AND NOT t.tgisinternal)
    ) THEN
        RAISE EXCEPTION 'unreviewed options trigger closure';
    END IF;
END
$$;
SELECT options_bounded_assert_trigger_closure();
