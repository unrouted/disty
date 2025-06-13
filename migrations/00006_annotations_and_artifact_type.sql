ALTER TABLE manifests
ADD COLUMN annotations TEXT NOT NULL DEFAULT '{}';

ALTER TABLE manifests
ADD COLUMN artifact_type TEXT;