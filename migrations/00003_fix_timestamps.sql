-- repositories
CREATE TABLE repositories_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    created_at DATETIME NOT NULL
);
INSERT INTO repositories_new SELECT * FROM repositories;
DROP TABLE repositories;
ALTER TABLE repositories_new RENAME TO repositories;

-- blobs
CREATE TABLE blobs_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    digest TEXT UNIQUE NOT NULL,
    size INTEGER NOT NULL,
    location INTEGER NOT NULL,
    state INTEGER NOT NULL,
    created_by TEXT NOT NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL
);
INSERT INTO blobs_new SELECT * FROM blobs;
DROP TABLE blobs;
ALTER TABLE blobs_new RENAME TO blobs;

-- blobs_repositories
CREATE TABLE blobs_repositories_new (
    blob_id INTEGER NOT NULL,
    repository_id INTEGER NOT NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    PRIMARY KEY (blob_id, repository_id),
    FOREIGN KEY(repository_id) REFERENCES repositories(id),
    FOREIGN KEY(blob_id) REFERENCES blobs(id)
);
INSERT INTO blobs_repositories_new SELECT * FROM blobs_repositories;
DROP TABLE blobs_repositories;
ALTER TABLE blobs_repositories_new RENAME TO blobs_repositories;

-- manifests
CREATE TABLE manifests_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    digest TEXT UNIQUE NOT NULL,
    size INTEGER NOT NULL,
    media_type TEXT NOT NULL,
    location INTEGER NOT NULL,
    state INTEGER NOT NULL,
    created_by TEXT NOT NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    annotations TEXT NOT NULL DEFAULT '{}',
    artifact_type TEXT
);
INSERT INTO manifests_new SELECT * FROM manifests;
DROP TABLE manifests;
ALTER TABLE manifests_new RENAME TO manifests;

-- manifests_repositories
CREATE TABLE manifests_repositories_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    manifest_id INTEGER NOT NULL,
    repository_id INTEGER NOT NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    FOREIGN KEY(manifest_id) REFERENCES manifests(id),
    FOREIGN KEY(repository_id) REFERENCES repositories(id),
    UNIQUE(manifest_id, repository_id)
);
INSERT INTO manifests_repositories_new SELECT * FROM manifests_repositories;
DROP TABLE manifests_repositories;
ALTER TABLE manifests_repositories_new RENAME TO manifests_repositories;

-- tags
CREATE TABLE tags_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    manifest_repository_id INTEGER NOT NULL,
    repository_id INTEGER NOT NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    FOREIGN KEY(manifest_repository_id) REFERENCES manifests_repositories(id) ON DELETE CASCADE,
    FOREIGN KEY(repository_id) REFERENCES repositories(id) ON DELETE CASCADE,
    UNIQUE(repository_id, name)
);
INSERT INTO tags_new SELECT * FROM tags;
DROP TABLE tags;
ALTER TABLE tags_new RENAME TO tags;
