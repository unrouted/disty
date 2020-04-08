from .actions import RegistryActions


class Reducer:
    def __init__(self, log):
        self.log = log

    def dispatch_entries(self, entries):
        for entry in entries:
            self.dispatch(entry)


class RegistryState(Reducer):
    def __init__(self):
        self.state = {}
        self.manifests = {}
        self.blobs = {}

    def is_blob_available(self, repository, hash):
        if hash not in self.blobs:
            return False

        if repository not in self.blobs[hash]:
            return False

        return True

    def is_manifest_available(self, repository, hash):
        if hash not in self.manifests:
            return False

        if repository not in self.manifests[hash]:
            return False

        return True

    def get_tag(self, repository, tag):
        return self.state.get(repository, {})[tag]

    def dispatch(self, entry):
        if entry["type"] == RegistryActions.HASH_TAGGED:
            repository = self.state.setdefault(entry["repository"], {})
            repository[entry["tag"]] = entry["hash"]

        elif entry["type"] == RegistryActions.BLOB_MOUNTED:
            blob = self.blobs.setdefault(entry["hash"], set())
            blob.add(entry["repository"])

        elif entry["type"] == RegistryActions.BLOB_DELETED:
            blob = self.blobs.setdefault(entry["hash"], set())
            blob.discard(entry["repository"])

        elif entry["type"] == RegistryActions.MANIFEST_MOUNTED:
            manifest = self.manifests.setdefault(entry["hash"], set())
            manifest.add(entry["repository"])

        elif entry["type"] == RegistryActions.MANIFEST_DELETED:
            manifest = self.manifests.setdefault(entry["hash"], set())
            manifest.discard(entry["repository"])
