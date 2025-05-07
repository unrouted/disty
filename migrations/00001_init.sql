-- Repositories (e.g., "library/ubuntu")
CREATE TABLE repositories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Blobs: image layers or configs, associated with a repository
CREATE TABLE blobs (
    digest TEXT PRIMARY KEY,  -- e.g., sha256:...
    repository_id INTEGER NOT NULL,
    size INTEGER NOT NULL,
    media_type TEXT NOT NULL,
    location INTEGER NOT NULL,  -- Bitset of where content exists in the cluster
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,  -- Timestamp when the record was last updated
    deleted_at DATETIME,  -- Soft delete time, null if not deleted
    FOREIGN KEY(repository_id) REFERENCES repositories(id)
);

-- Manifests: associated with a repository, includes size, media type, location, and update timestamp
CREATE TABLE manifests (
    digest TEXT PRIMARY KEY,
    repository_id INTEGER NOT NULL,
    size INTEGER NOT NULL,
    media_type TEXT NOT NULL,
    location INTEGER NOT NULL,  -- Bitset of where content exists in the cluster
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,  -- Timestamp when the record was last updated
    deleted_at DATETIME,  -- Soft delete time, null if not deleted
    FOREIGN KEY(repository_id) REFERENCES repositories(id)
);

-- Manifest layers: many-to-many between manifests and blobs, capturing the order of layers
CREATE TABLE manifest_layers (
    manifest_digest TEXT NOT NULL,
    blob_digest TEXT NOT NULL,
    position INTEGER NOT NULL,  -- Order of the layers
    PRIMARY KEY (manifest_digest, blob_digest),
    FOREIGN KEY(manifest_digest) REFERENCES manifests(digest),
    FOREIGN KEY(blob_digest) REFERENCES blobs(digest)
);

-- Tags: point to a specific manifest, supports soft delete, and includes update timestamp
CREATE TABLE tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    repository_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    manifest_digest TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,  -- Timestamp when the record was last updated
    deleted_at DATETIME,  -- Soft delete time, null if not deleted
    FOREIGN KEY(repository_id) REFERENCES repositories(id),
    FOREIGN KEY(manifest_digest) REFERENCES manifests(digest),
    UNIQUE(repository_id, name)
);
