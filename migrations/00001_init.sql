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
    location INTEGER NOT NULL,  -- Bitset indicating which hosts store this blob
    created_by TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Association of blobs with repositories
CREATE TABLE blobs_repositories (
    digest TEXT NOT NULL,
    repository_id INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
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
    created_by TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(repository_id) REFERENCES repositories(id),
    UNIQUE(repository_id, digest)
);

-- Layers that make up a manifest, in order
CREATE TABLE manifest_layers (
    manifest_id INTEGER NOT NULL,
    blob_digest TEXT NOT NULL,
    PRIMARY KEY (manifest_id, blob_digest),
    FOREIGN KEY(manifest_id) REFERENCES manifests(id) ON DELETE CASCADE,
    FOREIGN KEY(blob_digest) REFERENCES blobs(digest)
);

CREATE TABLE manifest_references (
    manifest_id INTEGER NOT NULL,
    child_id INTEGER NOT NULL,
    FOREIGN KEY(manifest_id) REFERENCES manifests(id) ON DELETE CASCADE,
    FOREIGN KEY(child_id) REFERENCES manifests(id) ON DELETE CASCADE,
    PRIMARY KEY(manifest_id, child_id)
);

-- Tags: named references to manifests, unique per repository
CREATE TABLE tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    repository_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    manifest_id INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(repository_id) REFERENCES repositories(id),
    FOREIGN KEY(manifest_id) REFERENCES manifests(id),
    UNIQUE(repository_id, name)
);
