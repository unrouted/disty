
-- 1. Create new `manifests_new` table with global uniqueness on digest
CREATE TABLE manifests_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    digest TEXT UNIQUE NOT NULL,
    size INTEGER NOT NULL,
    media_type TEXT NOT NULL,
    location INTEGER NOT NULL,
    state INTEGER NOT NULL DEFAULT 0,
    created_by TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 2. Insert into `manifests_new`, merging duplicates with identical fields
-- (failing if digest is reused with different data)
INSERT INTO manifests_new (digest, size, media_type, location, state, created_by, created_at, updated_at)
SELECT DISTINCT digest, size, media_type, location, state, created_by, created_at, updated_at
FROM manifests;

-- 3. Create `manifest_digest_to_id` temp table to track new manifest IDs
CREATE TEMP TABLE manifest_digest_to_id AS
SELECT digest, id FROM manifests_new;

-- 4. Create `manifests_repositories` table
CREATE TABLE manifests_repositories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    manifest_id INTEGER NOT NULL,
    repository_id INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(manifest_id) REFERENCES manifests_new(id),
    FOREIGN KEY(repository_id) REFERENCES repositories(id),
    UNIQUE(manifest_id, repository_id)
);

-- 5. Populate `manifests_repositories` from old manifests table
INSERT INTO manifests_repositories (manifest_id, repository_id, created_at, updated_at)
SELECT mdt.id, m.repository_id, m.created_at, m.updated_at
FROM manifests m
JOIN manifest_digest_to_id mdt ON m.digest = mdt.digest;

-- 6. Create `tags_new` table with reference to `manifests_repositories`
CREATE TABLE tags_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    manifest_repository_id INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(manifest_repository_id) REFERENCES manifests_repositories(id) ON DELETE CASCADE,
    UNIQUE(manifest_repository_id, name)
);

-- 7. Create temp map from (manifest_id, repository_id) to new `manifests_repositories.id`
CREATE TEMP TABLE tag_manifest_map AS
SELECT mr.id AS manifest_repository_id, m.id AS manifest_id, mr.repository_id
FROM manifests m
JOIN manifest_digest_to_id mdt ON m.digest = mdt.digest
JOIN manifests_repositories mr ON mr.manifest_id = mdt.id AND mr.repository_id = m.repository_id;

-- 8. Copy data from `tags` into `tags_new`
INSERT INTO tags_new (id, name, manifest_repository_id, created_at, updated_at)
SELECT t.id, t.name, tm.manifest_repository_id, t.created_at, t.updated_at
FROM tags t
JOIN tag_manifest_map tm ON t.manifest_id = tm.manifest_id AND t.repository_id = tm.repository_id;

-- 9. Replace old tables
DROP TABLE tags;
ALTER TABLE tags_new RENAME TO tags;

DROP TABLE manifests;
ALTER TABLE manifests_new RENAME TO manifests;

-- 10. Clean up temp tables
DROP TABLE manifest_digest_to_id;
DROP TABLE tag_manifest_map;
