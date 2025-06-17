-- Step 0: Disable foreign key enforcement during migration
PRAGMA foreign_keys = OFF;

-- Step 1: Create new blobs table with `id` as AUTOINCREMENT primary key
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

-- Step 2: Populate blobs_new
INSERT INTO blobs_new (digest, size, location, state, created_by, created_at, updated_at)
SELECT digest, size, location, state, created_by, created_at, updated_at FROM blobs;

-- Step 3: Create temporary mapping table from digest to new id
CREATE TEMP TABLE blob_digest_to_id AS
SELECT digest, id FROM blobs_new;

-- Step 4: Drop old blobs table and rename new one to blobs
DROP TABLE blobs;
ALTER TABLE blobs_new RENAME TO blobs;

-- Step 5: Migrate blobs_repositories

-- Rename old table
ALTER TABLE blobs_repositories RENAME TO blobs_repositories_old;

-- Create new table with blob_id foreign key
CREATE TABLE blobs_repositories (
    blob_id INTEGER NOT NULL,
    repository_id INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (blob_id, repository_id),
    FOREIGN KEY(repository_id) REFERENCES repositories(id),
    FOREIGN KEY(blob_id) REFERENCES blobs(id)
);

-- Migrate data using digest → id mapping
INSERT INTO blobs_repositories (blob_id, repository_id, created_at, updated_at)
SELECT b.id, br.repository_id, br.created_at, br.updated_at
FROM blobs_repositories_old br
JOIN blob_digest_to_id b ON br.digest = b.digest;

DROP TABLE blobs_repositories_old;

-- Step 6: Migrate manifest_layers

-- Rename old table
ALTER TABLE manifest_layers RENAME TO manifest_layers_old;

-- Create new table with blob_id foreign key
CREATE TABLE manifest_layers (
    manifest_id INTEGER NOT NULL,
    blob_id INTEGER NOT NULL,
    PRIMARY KEY (manifest_id, blob_id),
    FOREIGN KEY(manifest_id) REFERENCES manifests(id) ON DELETE CASCADE,
    FOREIGN KEY(blob_id) REFERENCES blobs(id)
);

-- Migrate data using digest → id mapping
INSERT INTO manifest_layers (manifest_id, blob_id)
SELECT ml.manifest_id, b.id
FROM manifest_layers_old ml
JOIN blob_digest_to_id b ON ml.blob_digest = b.digest;

DROP TABLE manifest_layers_old;

-- Step 7: Drop temporary mapping table
DROP TABLE blob_digest_to_id;

-- Step 8: Re-enable foreign key checks
PRAGMA foreign_keys = ON;