-- Step 1: Add new `id` column to blobs
ALTER TABLE blobs ADD COLUMN id INTEGER;

-- Step 2: Populate `id` with unique values (autoincrement simulation)
-- SQLite doesn't support autoincrement on existing columns directly
-- So we use a temp table to recreate blobs with new schema
CREATE TABLE blobs_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    digest TEXT UNIQUE NOT NULL,
    size INTEGER NOT NULL,
    location INTEGER NOT NULL,
    state INTEGER NOT NULL DEFAULT 0,
    created_by TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Insert data and let SQLite assign `id`s
INSERT INTO blobs_new (digest, size, location, state, created_by, created_at, updated_at)
SELECT digest, size, location, state, created_by, created_at, updated_at FROM blobs;

-- Create a mapping table from old digest to new id
CREATE TEMP TABLE blob_digest_to_id AS
SELECT digest, id FROM blobs_new;

-- Drop old blobs table and rename the new one
DROP TABLE blobs;
ALTER TABLE blobs_new RENAME TO blobs;

-- Step 3: Migrate blobs_repositories
ALTER TABLE blobs_repositories RENAME TO blobs_repositories_old;

CREATE TABLE blobs_repositories (
    blob_id INTEGER NOT NULL,
    repository_id INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (blob_id, repository_id),
    FOREIGN KEY(repository_id) REFERENCES repositories(id),
    FOREIGN KEY(blob_id) REFERENCES blobs(id)
);

INSERT INTO blobs_repositories (blob_id, repository_id, created_at, updated_at)
SELECT b.id, br.repository_id, br.created_at, br.updated_at
FROM blobs_repositories_old br
JOIN blob_digest_to_id b ON br.digest = b.digest;

DROP TABLE blobs_repositories_old;

-- Step 4: Migrate manifest_layers
ALTER TABLE manifest_layers RENAME TO manifest_layers_old;

CREATE TABLE manifest_layers (
    manifest_id INTEGER NOT NULL,
    blob_id INTEGER NOT NULL,
    PRIMARY KEY (manifest_id, blob_id),
    FOREIGN KEY(manifest_id) REFERENCES manifests(id) ON DELETE CASCADE,
    FOREIGN KEY(blob_id) REFERENCES blobs(id)
);

INSERT INTO manifest_layers (manifest_id, blob_id)
SELECT ml.manifest_id, b.id
FROM manifest_layers_old ml
JOIN blob_digest_to_id b ON ml.blob_digest = b.digest;

DROP TABLE manifest_layers_old;

-- Step 5: Drop temporary mapping table
DROP TABLE blob_digest_to_id;