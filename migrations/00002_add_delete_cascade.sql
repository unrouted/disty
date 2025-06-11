-- Rename the old table
ALTER TABLE tags RENAME TO tags_old;

-- Recreate the `tags` table with ON DELETE CASCADE on manifest_id
CREATE TABLE tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    repository_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    manifest_id INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(repository_id) REFERENCES repositories(id),
    FOREIGN KEY(manifest_id) REFERENCES manifests(id) ON DELETE CASCADE,
    UNIQUE(repository_id, name)
);

-- Copy data from the old table to the new one
INSERT INTO tags (id, repository_id, name, manifest_id, created_at, updated_at)
SELECT id, repository_id, name, manifest_id, created_at, updated_at FROM tags_old;

-- Drop the old table
DROP TABLE tags_old;
