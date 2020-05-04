import logging

from networkx import DiGraph

from .actions import RegistryActions

logger = logging.getLogger(__name__)

ATTR_CONTENT_TYPE = "content_type"
ATTR_SIZE = "size"
ATTR_DEPENDENCIES = "dependencies"
ATTR_HASH = "hash"
ATTR_REPOSITORY = "repository"


class Reducer:
    def __init__(self, log):
        self.log = log

    def dispatch_entries(self, entries):
        for term, entry in entries:
            if "type" in entry:
                self.dispatch(entry)


class RegistryState(Reducer):
    def __init__(self):
        self.state = {}
        self.manifests = {}
        self.tags_for_hash = {}

        self.graph = DiGraph()

    def __getitem__(self, key):
        return self.graph.nodes[key]

    def is_blob_available(self, repository, hash):
        if hash not in self.graph.nodes:
            return False

        if repository not in self.graph.nodes[hash]["repositories"]:
            return False

        return True

    def is_manifest_available(self, repository, hash):
        if hash not in self.graph.nodes:
            return False

        manifest = self.graph.nodes[hash]

        if "content_type" not in manifest:
            return False

        if repository not in self.graph.nodes[hash]["repositories"]:
            return False

        return True

    def get_tags(self, repository):
        return list(self.state[repository].keys())

    def get_tag(self, repository, tag):
        logger.debug("%s %s %s", self.state, repository, tag)
        return self.state.get(repository, {})[tag]

    def dispatch(self, entry):
        logger.critical("Applying %s", entry)

        if entry["type"] == RegistryActions.HASH_TAGGED:
            repository = self.state.setdefault(entry["repository"], {})
            repository[entry["tag"]] = entry["hash"]

            tags_for_hash = self.tags_for_hash.setdefault(entry["hash"], set())
            tags_for_hash.add((entry["repository"], entry["tag"]))

        elif entry["type"] == RegistryActions.BLOB_MOUNTED:
            if entry["hash"] not in self.graph.nodes:
                self.graph.add_node(entry["hash"])

            self.graph.nodes[entry["hash"]].setdefault("repositories", set()).add(
                entry["repository"]
            )

        elif entry["type"] == RegistryActions.BLOB_UNMOUNTED:
            self.graph.nodes[entry["hash"]]["repositories"].discard(entry["repository"])

        elif entry["type"] == RegistryActions.BLOB_INFO:
            for dependency in entry["dependencies"]:
                self.graph.add_edge(entry["hash"], dependency)
            self.graph.nodes[entry["hash"]]["content_type"] = entry["content_type"]

        elif entry["type"] == RegistryActions.BLOB_STAT:
            self.graph.nodes[entry["hash"]]["size"] = entry["size"]

        elif entry["type"] == RegistryActions.MANIFEST_MOUNTED:
            if entry["hash"] not in self.graph.nodes:
                self.graph.add_node(entry["hash"])

            self.graph.nodes[entry["hash"]].setdefault("repositories", set()).add(
                entry["repository"]
            )

        elif entry["type"] == RegistryActions.MANIFEST_UNMOUNTED:
            self.graph.nodes[entry["hash"]]["repositories"].discard(entry["repository"])

            # Legacy
            for repository, tag in set(self.tags_for_hash.get(entry["hash"], [])):
                if repository != entry["repository"]:
                    continue
                self.state.get(repository, {}).pop(tag, None)

        elif entry["type"] == RegistryActions.MANIFEST_INFO:
            for dependency in entry["dependencies"]:
                self.graph.add_edge(entry["hash"], dependency)
            self.graph.nodes[entry["hash"]]["content_type"] = entry["content_type"]

        elif entry["type"] == RegistryActions.MANIFEST_INFO:
            self.graph.nodes[entry["hash"]]["size"] = entry["size"]
