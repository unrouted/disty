CREATE TABLE repositories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE manifest_references (
    manifest_id INTEGER NOT NULL,
    child_id INTEGER NOT NULL,
    FOREIGN KEY(manifest_id) REFERENCES manifests(id) ON DELETE CASCADE,
    FOREIGN KEY(child_id) REFERENCES manifests(id) ON DELETE CASCADE,
    PRIMARY KEY(manifest_id, child_id)
);

CREATE TABLE blobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    digest TEXT UNIQUE NOT NULL,
    size INTEGER NOT NULL,
    location INTEGER NOT NULL,
    state INTEGER NOT NULL DEFAULT 0,
    created_by TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE blobs_repositories (
    blob_id INTEGER NOT NULL,
    repository_id INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (blob_id, repository_id),
    FOREIGN KEY(repository_id) REFERENCES repositories(id),
    FOREIGN KEY(blob_id) REFERENCES blobs(id)
);

CREATE TABLE manifest_layers (
    manifest_id INTEGER NOT NULL,
    blob_id INTEGER NOT NULL,
    PRIMARY KEY (manifest_id, blob_id),
    FOREIGN KEY(manifest_id) REFERENCES manifests(id) ON DELETE CASCADE,
    FOREIGN KEY(blob_id) REFERENCES blobs(id)
);

CREATE TABLE manifests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    digest TEXT UNIQUE NOT NULL,
    size INTEGER NOT NULL,
    media_type TEXT NOT NULL,
    location INTEGER NOT NULL,
    state INTEGER NOT NULL DEFAULT 0,
    created_by TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    annotations TEXT NOT NULL DEFAULT '{}', artifact_type TEXT
);

CREATE TABLE manifests_repositories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    manifest_id INTEGER NOT NULL,
    repository_id INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(manifest_id) REFERENCES "manifests"(id),
    FOREIGN KEY(repository_id) REFERENCES repositories(id),
    UNIQUE(manifest_id, repository_id)
);

CREATE TABLE manifest_subject (
    manifest_id INTEGER NOT NULL,
    subject_id INTEGER NOT NULL,
    FOREIGN KEY(manifest_id) REFERENCES manifests(id) ON DELETE CASCADE,
    FOREIGN KEY(subject_id) REFERENCES manifests(id) ON DELETE CASCADE,
    PRIMARY KEY(manifest_id, subject_id)
);

CREATE TABLE tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    manifest_repository_id INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(manifest_repository_id) REFERENCES manifests_repositories(id) ON DELETE CASCADE,
    UNIQUE(manifest_repository_id, name)
);
