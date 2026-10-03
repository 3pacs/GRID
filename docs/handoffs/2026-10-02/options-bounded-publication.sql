-- REVIEW PACKET ONLY: outside Alembic; no automatic deploy/application.
-- Use one explicit transaction (psql --single-transaction --set ON_ERROR_STOP=1).
-- Root supplies fresh reviewed exact row counts in grid.options_expected_counts.
-- COMMIT once; unknown ACK means STOP/inspect, never automatic replay.
-- 16 closure rows plus ONE version/schema stamp = 17 aggregate DATA rows.
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='5s';
SET LOCAL idle_in_transaction_session_timeout='5s';
SET LOCAL search_path=public,pg_catalog;
SET LOCAL grid.options_apply_started_at TO DEFAULT;
SELECT pg_catalog.set_config('grid.options_apply_started_at',pg_catalog.clock_timestamp()::text,true);
LOCK TABLE source_catalog,feature_registry,resolved_series,options_daily_signals,
    options_capture_batches,options_snapshots_all IN ACCESS EXCLUSIVE MODE;
CREATE FUNCTION pg_temp.options_catalog_image(relation_name text) RETURNS jsonb
LANGUAGE plpgsql AS $image$
DECLARE relation_oid oid; result jsonb;
BEGIN
    SELECT c.oid INTO relation_oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
      WHERE n.nspname='public' AND c.relname=relation_name;
    IF relation_oid IS NULL THEN RETURN pg_catalog.jsonb_build_object('exists',false); END IF;
    SELECT pg_catalog.jsonb_build_object('exists',true,'kind',c.relkind::text,'owner',r.rolname,
      'rls',c.relrowsecurity,'forced_rls',c.relforcerowsecurity,'acl',c.relacl::text)
      INTO result FROM pg_catalog.pg_class c JOIN pg_catalog.pg_roles r ON r.oid=c.relowner WHERE c.oid=relation_oid;
    RETURN result || pg_catalog.jsonb_build_object(
      'columns',COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(a.attnum,a.attname,
        pg_catalog.format_type(a.atttypid,a.atttypmod),a.attnotnull,a.attidentity::text,a.attgenerated::text,
        pg_catalog.pg_get_expr(d.adbin,d.adrelid)) ORDER BY a.attnum)
        FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
        WHERE a.attrelid=relation_oid AND a.attnum>0),'[]'::jsonb),
      'constraints',COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(conname,contype::text,
        CASE WHEN confrelid=0 THEN NULL ELSE confrelid::regclass::text END,
        condeferrable,condeferred,convalidated,pg_get_constraintdef(oid)) ORDER BY conname)
        FROM pg_catalog.pg_constraint WHERE conrelid=relation_oid OR confrelid=relation_oid),'[]'::jsonb),
      'indexes',COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(pg_catalog.pg_get_indexdef(indexrelid),
        indisvalid,indisready,indislive) ORDER BY pg_catalog.pg_get_indexdef(indexrelid))
        FROM pg_catalog.pg_index WHERE indrelid=relation_oid),'[]'::jsonb),
      'rules',COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(pg_catalog.pg_get_ruledef(oid)) ORDER BY pg_catalog.pg_get_ruledef(oid))
        FROM pg_catalog.pg_rewrite WHERE ev_class=relation_oid),'[]'::jsonb),
      'view_definition',CASE WHEN (SELECT relkind FROM pg_catalog.pg_class WHERE oid=relation_oid)='v'
        THEN pg_catalog.pg_get_viewdef(relation_oid,true) ELSE NULL END,
      'triggers',COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(
        pg_catalog.regexp_replace(t.tgname,'RI_ConstraintTrigger_([ac])_[0-9]+','RI_ConstraintTrigger_\1_OID','g'),
        t.tgisinternal,t.tgenabled::text,
        pg_catalog.regexp_replace(pg_catalog.pg_get_triggerdef(t.oid),'RI_ConstraintTrigger_([ac])_[0-9]+','RI_ConstraintTrigger_\1_OID','g'),
        n.nspname,p.proname,p.prosecdef,p.provolatile::text,r.rolname,p.proconfig,
        pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.pg_get_functiondef(p.oid),'UTF8')),'hex')) ORDER BY
        pg_catalog.regexp_replace(pg_catalog.pg_get_triggerdef(t.oid),'RI_ConstraintTrigger_([ac])_[0-9]+','RI_ConstraintTrigger_\1_OID','g'))
        FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
        JOIN pg_catalog.pg_roles r ON r.oid=p.proowner WHERE t.tgrelid=relation_oid),'[]'::jsonb));
END
$image$;
DO $preflight$
DECLARE expected jsonb := $catalog${"feature_registry":"d07fd24b00d7ec2e0bba4b5ca59d6d4016c20ae3d229eaf79c2b3f7795783a7f","options_capture_batches":"0284ea82536291f7aaa8be38db931ea53491407b08aee0e8573831af6dbb9814","options_daily_signals":"8a44795314d411d77a24ceeb51bff9ede67c14a7df4d872f2e417af6b578157d","options_snapshots":"492562d5482beed206210d24f1ea233142b20278dfa038a32c58c7558ac252f9","options_snapshots_all":"27261e782b5e225b60706b1ad06ea2bc282f4245de7a2a6665527a091bfb57a6","resolved_series":"f9f7f9b7896541a03af1d29ca00f68387d7e3fdf9c09558a6f88e4eb64109110","source_catalog":"2b3675de797154c3c316bb5eac1816ef7c7da02d330eae2e351abf57b06e2912"}$catalog$::jsonb;
    entry record; table_name text; counts jsonb; n bigint;
BEGIN
    IF current_user <> 'grid' OR current_schema() <> 'public'
       OR pg_catalog.current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'options initial role/schema/isolation identity drift'; END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_event_trigger WHERE evtenabled<>'D') THEN
        RAISE EXCEPTION 'unreviewed options event trigger'; END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_default_acl WHERE defaclrole=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=current_user)
        AND defaclobjtype IN ('f','r') AND defaclnamespace IN (0,'public'::regnamespace::oid)) THEN
        RAISE EXCEPTION 'unreviewed options default grants; preserve and review, never revoke'; END IF;
    IF pg_catalog.to_regclass('public.options_capture_batches_all') IS NOT NULL
       OR pg_catalog.to_regclass('public.options_capture_publications') IS NOT NULL
       OR pg_catalog.to_regclass('public.options_bounded_trigger_contracts') IS NOT NULL
       OR pg_catalog.to_regclass('public.options_bounded_schema_contract') IS NOT NULL THEN
        RAISE EXCEPTION 'options initial version is not absent; inspect, never retry'; END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
        WHERE n.nspname='public' AND p.proname IN ('options_bounded_contract_guard',
          'options_bounded_completion_guard','options_bounded_row_budget',
          'options_bounded_catalog_image','options_bounded_assert_trigger_closure')) THEN
        RAISE EXCEPTION 'options initial function version is not absent'; END IF;
    FOR entry IN SELECT * FROM pg_catalog.jsonb_each(expected) LOOP
        IF pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_temp.options_catalog_image(entry.key)::text,'UTF8')),'hex')
           IS DISTINCT FROM entry.value #>> '{}' THEN
            RAISE EXCEPTION 'options initial catalog drift: %',entry.key; END IF;
    END LOOP;
    IF 'public.feature_registry'::regclass::oid <> 16430 THEN RAISE EXCEPTION 'options original relation OID drift: feature_registry'; END IF;
    IF 'public.options_capture_batches'::regclass::oid <> 778337239 THEN RAISE EXCEPTION 'options original relation OID drift: options_capture_batches'; END IF;
    IF 'public.options_daily_signals'::regclass::oid <> 17005 THEN RAISE EXCEPTION 'options original relation OID drift: options_daily_signals'; END IF;
    IF 'public.options_snapshots'::regclass::oid <> 778337266 THEN RAISE EXCEPTION 'options original relation OID drift: options_snapshots'; END IF;
    IF 'public.options_snapshots_all'::regclass::oid <> 16991 THEN RAISE EXCEPTION 'options original relation OID drift: options_snapshots_all'; END IF;
    IF 'public.resolved_series'::regclass::oid <> 16451 THEN RAISE EXCEPTION 'options original relation OID drift: resolved_series'; END IF;
    IF 'public.source_catalog'::regclass::oid <> 16387 THEN RAISE EXCEPTION 'options original relation OID drift: source_catalog'; END IF;
    -- Function names are not authority; these reviewed original OIDs, body
    -- hashes and catalog images must all match before ALTER/INSERT.
    IF 'public.feature_registry_check_transformation_version()'::regprocedure::oid <> 5259572 OR pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.pg_get_functiondef('public.feature_registry_check_transformation_version()'::regprocedure),'UTF8')),'hex') <> '57671d10b24505bd6294a85e6ce801a2dd4f474281518d7e98c9d0402135d3bd' THEN RAISE EXCEPTION 'reviewed options guard identity drift'; END IF;
    IF 'public.options_capture_append_only_guard()'::regprocedure::oid <> 778337261 OR pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.pg_get_functiondef('public.options_capture_append_only_guard()'::regprocedure),'UTF8')),'hex') <> 'dfbf6c305268f9c74d8e740892ff78a73354b46dd1f570900c333ba811a5162f' THEN RAISE EXCEPTION 'reviewed options guard identity drift'; END IF;
    counts := NULLIF(pg_catalog.current_setting('grid.options_expected_counts',true),'')::jsonb;
    IF counts IS NULL OR (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(counts)) <> 6 THEN
        RAISE EXCEPTION 'six fresh exact initial row counts required'; END IF;
    FOREACH table_name IN ARRAY ARRAY['source_catalog','feature_registry','resolved_series',
        'options_daily_signals','options_capture_batches','options_snapshots_all'] LOOP
        EXECUTE pg_catalog.format('SELECT pg_catalog.count(*) FROM public.%I',table_name) INTO n;
        IF NOT counts ? table_name OR counts->>table_name <> n::text THEN
            RAISE EXCEPTION 'options initial DATA count drift: %',table_name; END IF;
    END LOOP;
END
$preflight$;
ALTER TABLE options_capture_batches RENAME TO options_capture_batches_all;
ALTER TABLE options_capture_batches_all ADD COLUMN requires_publication boolean NOT NULL DEFAULT false;

CREATE TABLE options_capture_publications (
    capture_batch_id text PRIMARY KEY REFERENCES options_capture_batches_all(capture_batch_id),
    published_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp()
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
    -- Preserve the exact original required-metadata CHECK and header FK error
    -- paths. They reject missing/unknown registrations after BEFORE triggers.
    IF NEW.capture_batch_id IS NULL THEN
        RETURN NEW;
    END IF;
    SELECT * INTO h FROM options_capture_batches_all
        WHERE capture_batch_id = NEW.capture_batch_id FOR UPDATE;
    IF NOT FOUND THEN
        RETURN NEW;
    END IF;
    IF h.requires_publication THEN
        IF pg_catalog.current_setting('transaction_isolation') <> 'read committed' THEN
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
    IF pg_catalog.current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'options completion requires READ COMMITTED';
    END IF;
    SELECT pg_catalog.count(*) INTO n FROM options_snapshots_all
        WHERE capture_batch_id = h.capture_batch_id;
    IF n <> h.row_count OR EXISTS (
        SELECT 1 FROM options_snapshots_all s WHERE s.capture_batch_id = h.capture_batch_id
          AND (s.capture_ordinal <> h.capture_ordinal OR s.ticker <> h.ticker
               OR s.snap_date <> h.snap_date OR s.capture_started_at <> h.capture_started_at
               OR s.capture_completed_at <> h.capture_completed_at
               OR s.provider_regular_market_at > h.capture_completed_at)) THEN
        RAISE EXCEPTION 'incomplete or mismatched options batch';
    END IF;
    NEW.published_at := pg_catalog.clock_timestamp();
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
    IF pg_catalog.current_setting('grid.options_bounded', true) = 'on' THEN
        n := COALESCE(NULLIF(pg_catalog.current_setting('grid.options_rows', true), ''), '0')::integer + 1;
        IF n > 50 THEN RAISE EXCEPTION 'options DATA budget exceeded'; END IF;
        PERFORM pg_catalog.set_config('grid.options_rows', n::text, true);
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
        EXECUTE pg_catalog.format('CREATE TRIGGER options_bounded_row_budget AFTER INSERT OR UPDATE OR DELETE ON %I
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
DO $reviewed_guards$ BEGIN
    IF 'public.feature_registry_check_transformation_version()'::regprocedure::oid <> 5259572 OR pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.pg_get_functiondef('public.feature_registry_check_transformation_version()'::regprocedure),'UTF8')),'hex') <> '57671d10b24505bd6294a85e6ce801a2dd4f474281518d7e98c9d0402135d3bd' THEN RAISE EXCEPTION 'reviewed options guard identity drift'; END IF;
    IF 'public.options_capture_append_only_guard()'::regprocedure::oid <> 778337261 OR pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.pg_get_functiondef('public.options_capture_append_only_guard()'::regprocedure),'UTF8')),'hex') <> 'dfbf6c305268f9c74d8e740892ff78a73354b46dd1f570900c333ba811a5162f' THEN RAISE EXCEPTION 'reviewed options guard identity drift'; END IF;
END $reviewed_guards$;

INSERT INTO options_bounded_trigger_contracts
SELECT c.relname, t.tgname, pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.pg_get_functiondef(t.tgfoid),'UTF8')),'hex'), pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.pg_get_triggerdef(t.oid),'UTF8')),'hex')
FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid JOIN pg_catalog.pg_proc p ON p.oid = t.tgfoid
WHERE NOT t.tgisinternal AND c.oid = ANY(ARRAY[
    'options_capture_batches_all'::regclass,'options_snapshots_all'::regclass,
    'options_capture_publications'::regclass,'options_daily_signals'::regclass,
    'feature_registry'::regclass,'resolved_series'::regclass,'source_catalog'::regclass])
AND p.proname IN ('options_capture_append_only_guard', 'options_bounded_contract_guard',
                 'options_bounded_completion_guard', 'options_bounded_row_budget',
                 'feature_registry_check_transformation_version');


-- The captured ACLs expose no service principal besides grid. grid remains
-- owner; these explicit grants are exactly the reader/writer operations needed.
-- No existing ACL is revoked or expanded to another principal.
GRANT SELECT, INSERT ON options_capture_batches TO grid;
GRANT SELECT, INSERT ON options_capture_batches_all,options_capture_publications TO grid;
GRANT SELECT ON options_bounded_trigger_contracts TO grid;

CREATE FUNCTION options_bounded_catalog_image(relation_name text) RETURNS jsonb
LANGUAGE plpgsql AS $image$
DECLARE relation_oid oid; result jsonb;
BEGIN
    SELECT c.oid INTO relation_oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
      WHERE n.nspname='public' AND c.relname=relation_name;
    IF relation_oid IS NULL THEN RETURN pg_catalog.jsonb_build_object('exists',false); END IF;
    SELECT pg_catalog.jsonb_build_object('exists',true,'kind',c.relkind::text,'owner',r.rolname,
      'rls',c.relrowsecurity,'forced_rls',c.relforcerowsecurity,'acl',c.relacl::text)
      INTO result FROM pg_catalog.pg_class c JOIN pg_catalog.pg_roles r ON r.oid=c.relowner WHERE c.oid=relation_oid;
    RETURN result || pg_catalog.jsonb_build_object(
      'oid',relation_oid,
      'default_identity',COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(d.oid,d.adnum,d.adbin::text) ORDER BY d.adnum)
        FROM pg_catalog.pg_attrdef d WHERE d.adrelid=relation_oid),'[]'::jsonb),
      'trigger_identity',COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(t.oid,t.tgname,t.tgfoid,p.proacl::text)
        ORDER BY t.tgname,t.oid) FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid WHERE t.tgrelid=relation_oid),'[]'::jsonb)
    ) || pg_catalog.jsonb_build_object(
      'columns',COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(a.attnum,a.attname,
        pg_catalog.format_type(a.atttypid,a.atttypmod),a.attnotnull,a.attidentity::text,a.attgenerated::text,
        pg_catalog.pg_get_expr(d.adbin,d.adrelid)) ORDER BY a.attnum)
        FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
        WHERE a.attrelid=relation_oid AND a.attnum>0),'[]'::jsonb),
      'constraints',COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(conname,contype::text,
        CASE WHEN confrelid=0 THEN NULL ELSE confrelid::regclass::text END,
        condeferrable,condeferred,convalidated,pg_get_constraintdef(oid)) ORDER BY conname)
        FROM pg_catalog.pg_constraint WHERE conrelid=relation_oid OR confrelid=relation_oid),'[]'::jsonb),
      'indexes',COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(pg_catalog.pg_get_indexdef(indexrelid),
        indisvalid,indisready,indislive) ORDER BY pg_catalog.pg_get_indexdef(indexrelid))
        FROM pg_catalog.pg_index WHERE indrelid=relation_oid),'[]'::jsonb),
      'rules',COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(pg_catalog.pg_get_ruledef(oid)) ORDER BY pg_catalog.pg_get_ruledef(oid))
        FROM pg_catalog.pg_rewrite WHERE ev_class=relation_oid),'[]'::jsonb),
      'view_definition',CASE WHEN (SELECT relkind FROM pg_catalog.pg_class WHERE oid=relation_oid)='v'
        THEN pg_catalog.pg_get_viewdef(relation_oid,true) ELSE NULL END,
      'triggers',COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(
        pg_catalog.regexp_replace(t.tgname,'RI_ConstraintTrigger_([ac])_[0-9]+','RI_ConstraintTrigger_\1_OID','g'),
        t.tgisinternal,t.tgenabled::text,
        pg_catalog.regexp_replace(pg_catalog.pg_get_triggerdef(t.oid),'RI_ConstraintTrigger_([ac])_[0-9]+','RI_ConstraintTrigger_\1_OID','g'),
        n.nspname,p.proname,p.prosecdef,p.provolatile::text,r.rolname,p.proconfig,
        pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.pg_get_functiondef(p.oid),'UTF8')),'hex')) ORDER BY
        pg_catalog.regexp_replace(pg_catalog.pg_get_triggerdef(t.oid),'RI_ConstraintTrigger_([ac])_[0-9]+','RI_ConstraintTrigger_\1_OID','g'))
        FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
        JOIN pg_catalog.pg_roles r ON r.oid=p.proowner WHERE t.tgrelid=relation_oid),'[]'::jsonb));
END
$image$;
CREATE TABLE options_bounded_schema_contract (
    version text PRIMARY KEY CHECK (version='options-production-shape-v1'),
    root_catalog_sha256 text NOT NULL CHECK (root_catalog_sha256='7ae5deaf4e7e54c8c3e74421a98b1cfcaee4d320ef8578fc221c5b177d8e3785'),
    images jsonb NOT NULL,
    function_identity jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp()
);
CREATE TRIGGER options_bounded_schema_contract_no_row_mutation
    BEFORE UPDATE OR DELETE ON options_bounded_schema_contract
    FOR EACH ROW EXECUTE FUNCTION options_capture_append_only_guard();
CREATE TRIGGER options_bounded_schema_contract_no_truncate
    BEFORE TRUNCATE ON options_bounded_schema_contract
    FOR EACH STATEMENT EXECUTE FUNCTION options_capture_append_only_guard();
GRANT SELECT ON options_bounded_schema_contract TO grid;

CREATE FUNCTION options_bounded_assert_trigger_closure() RETURNS void LANGUAGE plpgsql AS $closure$
DECLARE expected jsonb; expected_functions jsonb; actual_functions jsonb; entry record;
BEGIN
    IF pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.pg_get_functiondef('public.feature_registry_check_transformation_version()'::regprocedure),'UTF8')),'hex') <> '57671d10b24505bd6294a85e6ce801a2dd4f474281518d7e98c9d0402135d3bd' THEN RAISE EXCEPTION 'reviewed options guard identity drift'; END IF;
    IF pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.pg_get_functiondef('public.options_capture_append_only_guard()'::regprocedure),'UTF8')),'hex') <> 'dfbf6c305268f9c74d8e740892ff78a73354b46dd1f570900c333ba811a5162f' THEN RAISE EXCEPTION 'reviewed options guard identity drift'; END IF;
    IF current_schema()<>'public' OR current_user<>'grid'
       OR pg_catalog.current_setting('transaction_isolation')<>'read committed'
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_event_trigger WHERE evtenabled<>'D') THEN
        RAISE EXCEPTION 'unreviewed options schema/role/event closure'; END IF;
    IF (SELECT pg_catalog.count(*) FROM options_bounded_schema_contract) <> 1
       OR (SELECT pg_catalog.count(*) FROM options_bounded_trigger_contracts) <> 16 THEN
        RAISE EXCEPTION 'unreviewed options version/closure row count'; END IF;
    SELECT images,function_identity INTO STRICT expected,expected_functions FROM options_bounded_schema_contract
      WHERE version='options-production-shape-v1'
        AND root_catalog_sha256='7ae5deaf4e7e54c8c3e74421a98b1cfcaee4d320ef8578fc221c5b177d8e3785';
    SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(p.oid,p.proname,pg_catalog.pg_get_function_identity_arguments(p.oid),
        p.prosecdef,p.provolatile::text,pg_catalog.pg_get_userbyid(p.proowner),p.proconfig,p.proacl::text,
        pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.pg_get_functiondef(p.oid),'UTF8')),'hex')) ORDER BY p.proname,p.oid)
      INTO actual_functions FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
      WHERE n.nspname='public' AND p.proname IN ('options_capture_append_only_guard',
        'feature_registry_check_transformation_version','options_bounded_contract_guard',
        'options_bounded_completion_guard','options_bounded_row_budget',
        'options_bounded_catalog_image','options_bounded_assert_trigger_closure');
    IF actual_functions IS DISTINCT FROM expected_functions THEN
        RAISE EXCEPTION 'unreviewed options function OID/body/catalog/ACL identity'; END IF;
    FOR entry IN SELECT * FROM pg_catalog.jsonb_each(expected) LOOP
        IF options_bounded_catalog_image(entry.key) IS DISTINCT FROM entry.value THEN
            RAISE EXCEPTION 'unreviewed options catalog/trigger closure: %',entry.key; END IF;
    END LOOP;
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_class c ON c.oid=t.tgrelid
        LEFT JOIN options_bounded_trigger_contracts a ON a.table_name=c.relname AND a.trigger_name=t.tgname
        WHERE NOT t.tgisinternal AND c.oid=ANY(ARRAY[
          'options_capture_batches_all'::regclass,'options_snapshots_all'::regclass,
          'options_capture_publications'::regclass,'options_daily_signals'::regclass,
          'feature_registry'::regclass,'resolved_series'::regclass,'source_catalog'::regclass])
          AND (a.function_hash IS NULL OR a.function_hash<>pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.pg_get_functiondef(t.tgfoid),'UTF8')),'hex')
            OR a.trigger_hash<>pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.pg_get_triggerdef(t.oid),'UTF8')),'hex') OR t.tgenabled<>'O')
    ) OR EXISTS (
        SELECT 1 FROM options_bounded_trigger_contracts a WHERE NOT EXISTS (
          SELECT 1 FROM pg_catalog.pg_trigger t WHERE t.tgrelid=pg_catalog.to_regclass(a.table_name)
            AND t.tgname=a.trigger_name AND NOT t.tgisinternal)) THEN
        RAISE EXCEPTION 'unreviewed options trigger closure'; END IF;
END
$closure$;

INSERT INTO options_bounded_schema_contract(version,root_catalog_sha256,images,function_identity)
SELECT 'options-production-shape-v1','7ae5deaf4e7e54c8c3e74421a98b1cfcaee4d320ef8578fc221c5b177d8e3785',
    pg_catalog.jsonb_object_agg(name,options_bounded_catalog_image(name)),
    (SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(p.oid,p.proname,pg_catalog.pg_get_function_identity_arguments(p.oid),
        p.prosecdef,p.provolatile::text,pg_catalog.pg_get_userbyid(p.proowner),p.proconfig,p.proacl::text,
        pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.pg_get_functiondef(p.oid),'UTF8')),'hex')) ORDER BY p.proname,p.oid)
      FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
      WHERE n.nspname='public' AND p.proname IN ('options_capture_append_only_guard',
        'feature_registry_check_transformation_version','options_bounded_contract_guard',
        'options_bounded_completion_guard','options_bounded_row_budget',
        'options_bounded_catalog_image','options_bounded_assert_trigger_closure'))
FROM pg_catalog.unnest(ARRAY['source_catalog','feature_registry','resolved_series','options_daily_signals',
    'options_capture_batches_all','options_capture_batches','options_snapshots_all','options_snapshots',
    'options_capture_publications','options_bounded_trigger_contracts','options_bounded_schema_contract']) name;
SELECT options_bounded_assert_trigger_closure();
DO $verify$
DECLARE counts jsonb := pg_catalog.current_setting('grid.options_expected_counts')::jsonb; entry record; n bigint;
BEGIN
    FOR entry IN SELECT * FROM pg_catalog.jsonb_each(counts) LOOP
        EXECUTE pg_catalog.format('SELECT pg_catalog.count(*) FROM public.%I',
            CASE WHEN entry.key='options_capture_batches' THEN 'options_capture_batches_all' ELSE entry.key END) INTO n;
        IF entry.value::text <> n::text THEN RAISE EXCEPTION 'options DATA count changed'; END IF;
    END LOOP;
    IF pg_catalog.clock_timestamp()-pg_catalog.current_setting('grid.options_apply_started_at')::timestamptz > interval '15 seconds' THEN
        RAISE EXCEPTION 'options schema transaction deadline expired'; END IF;
END
$verify$;
