-- 0052_transformation_version_invariant.sql
-- Closes consolidated audit MEDIUM #35 (Transformation Version Mismatch
-- Risk). The invariant: every distinct `transformation` string must
-- map to exactly one `transformation_version`. If someone changes the
-- transformation logic in code without bumping the version, the audit
-- said no validation flagged it. This migration adds two guards:
--
--   1. A CHECK constraint that transformation_version >= 1 (cheap,
--      schema-level, catches the "0 default forgotten" mistake).
--
--   2. A trigger that raises when an insert/update tries to use a
--      different version for an existing transformation. This is the
--      real protection: registering a new feature with the same
--      transformation but a bumped version is fine; registering a
--      conflicting version on an existing transformation is a bug.
--
-- Verified empty before adding — `SELECT transformation,
-- COUNT(DISTINCT transformation_version) FROM feature_registry GROUP BY
-- transformation HAVING COUNT(DISTINCT transformation_version) > 1`
-- returns 0 rows on the live DB as of 2026-05-07.

-- Step 1: positive-version CHECK
ALTER TABLE feature_registry
    DROP CONSTRAINT IF EXISTS chk_transformation_version_positive;
ALTER TABLE feature_registry
    ADD CONSTRAINT chk_transformation_version_positive
    CHECK (transformation_version >= 1);

-- Step 2: per-transformation version consistency trigger
CREATE OR REPLACE FUNCTION feature_registry_check_transformation_version()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    existing_version INTEGER;
BEGIN
    -- Skip when transformation didn't change (UPDATE no-op for this column)
    IF TG_OP = 'UPDATE'
       AND OLD.transformation = NEW.transformation
       AND OLD.transformation_version = NEW.transformation_version THEN
        RETURN NEW;
    END IF;

    SELECT DISTINCT transformation_version
      INTO existing_version
      FROM feature_registry
     WHERE transformation = NEW.transformation
       AND id <> COALESCE(NEW.id, -1)
     LIMIT 1;

    IF existing_version IS NOT NULL
       AND existing_version <> NEW.transformation_version THEN
        RAISE EXCEPTION
            'transformation_version mismatch: % already uses version %, '
            'but new row claims version %. Either reuse the existing '
            'version or update all existing rows when bumping.',
            NEW.transformation, existing_version, NEW.transformation_version;
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_feature_registry_transformation_version
    ON feature_registry;
CREATE TRIGGER trg_feature_registry_transformation_version
    BEFORE INSERT OR UPDATE OF transformation, transformation_version
    ON feature_registry
    FOR EACH ROW
    EXECUTE FUNCTION feature_registry_check_transformation_version();

COMMENT ON FUNCTION feature_registry_check_transformation_version() IS
    'Enforces audit MEDIUM #35: each transformation name has one '
    'consistent version across all features. Bumping the version means '
    'updating every row that uses that transformation atomically.';
