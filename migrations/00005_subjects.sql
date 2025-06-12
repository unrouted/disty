CREATE TABLE manifest_subject (
    manifest_id INTEGER NOT NULL,
    subject_id INTEGER NOT NULL,
    FOREIGN KEY(manifest_id) REFERENCES manifests(id) ON DELETE CASCADE,
    FOREIGN KEY(subject_id) REFERENCES manifests(id) ON DELETE CASCADE,
    PRIMARY KEY(manifest_id, subject_id)
);
