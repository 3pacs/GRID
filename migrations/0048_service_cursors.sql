-- Durable cursors for bounded daemon workers.
--
-- The signal backlinker uses this to resume from the last processed
-- signal_data.id instead of rescanning history and holding long snapshots.

CREATE TABLE IF NOT EXISTS service_cursors (
    service_name TEXT NOT NULL,
    cursor_name TEXT NOT NULL,
    cursor_value BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (service_name, cursor_name)
);

GRANT ALL ON TABLE service_cursors TO grid;
