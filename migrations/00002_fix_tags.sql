ALTER TABLE tags RENAME TO tags_old;

CREATE TABLE tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    manifest_repository_id INTEGER NOT NULL,
    repository_id INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(manifest_repository_id) REFERENCES manifests_repositories(id) ON DELETE CASCADE,
    FOREIGN KEY(repository_id) REFERENCES repositories(id) ON DELETE CASCADE,
    UNIQUE(repository_id, name)
);

INSERT INTO tags (name, manifest_repository_id, repository_id, created_at, updated_at)
SELECT
    t.name,
    t.manifest_repository_id,
    mr.repository_id,
    t.created_at,
    t.updated_at
FROM tags_old t
JOIN manifests_repositories mr ON t.manifest_repository_id = mr.id
WHERE t.id IN (
    SELECT id FROM (
        SELECT
            t1.id,
            ROW_NUMBER() OVER (
                PARTITION BY mr1.repository_id, t1.name
                ORDER BY t1.created_at DESC
            ) AS row_num
        FROM tags_old t1
        JOIN manifests_repositories mr1 ON t1.manifest_repository_id = mr1.id
    ) WHERE row_num = 1
);

DROP TABLE tags_old;
