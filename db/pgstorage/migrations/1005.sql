-- In RC14 onwards, the ugstream drops the DB column
-- from sync.block table. However, our XLayer code
-- still references this value.

-- +migrate Down
ALTER TABLE sync.block DROP COLUMN IF EXISTS received_at;

-- +migrate Up
ALTER TABLE sync.block ADD COLUMN IF NOT EXISTS received_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();
