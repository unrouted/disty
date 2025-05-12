-- Repositories (e.g., "library/ubuntu")
CREATE TABLE repositories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Blobs: content-addressable image layers or configs
CREATE TABLE blobs (
    digest TEXT PRIMARY KEY,
    size INTEGER NOT NULL,
    media_type TEXT NOT NULL,
    location INTEGER NOT NULL,  -- Bitset indicating which hosts store this blob
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    deleted_at DATETIME  -- Null if not deleted (used for garbage collection)
);

-- Association of blobs with repositories
CREATE TABLE blobs_repositories (
    digest TEXT NOT NULL,
    repository_id INTEGER NOT NULL,
    PRIMARY KEY (digest, repository_id),
    FOREIGN KEY(repository_id) REFERENCES repositories(id),
    FOREIGN KEY(digest) REFERENCES blobs(digest)
);

-- Manifests: represent image manifests, associated with a repository
CREATE TABLE manifests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    repository_id INTEGER NOT NULL,
    digest TEXT NOT NULL,
    size INTEGER NOT NULL,
    media_type TEXT NOT NULL,
    location INTEGER NOT NULL,  -- Bitset indicating which hosts store this manifest
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    deleted_at DATETIME,  -- Null if not deleted
    FOREIGN KEY(repository_id) REFERENCES repositories(id),
    UNIQUE(repository_id, digest)
);

-- Layers that make up a manifest, in order
CREATE TABLE manifest_layers (
    manifest_id INTEGER NOT NULL,
    blob_digest TEXT NOT NULL,
    position INTEGER NOT NULL,  -- Layer order in the manifest
    PRIMARY KEY (manifest_id, blob_digest),
    FOREIGN KEY(manifest_id) REFERENCES manifests(id),
    FOREIGN KEY(blob_digest) REFERENCES blobs(digest)
);

-- Tags: named references to manifests, unique per repository
CREATE TABLE tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    repository_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    manifest_id INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    deleted_at DATETIME,  -- Null if not deleted
    FOREIGN KEY(repository_id) REFERENCES repositories(id),
    FOREIGN KEY(manifest_id) REFERENCES manifests(id),
    UNIQUE(repository_id, name)
);