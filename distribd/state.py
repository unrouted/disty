import logging

from networkx import DiGraph

from .actions import RegistryActions

logger = logging.getLogger(__name__)

ATTR_CONTENT_TYPE = "content_type"
ATTR_SIZE = "size"
ATTR_DEPENDENCIES = "dependencies"
ATTR_HASH = "hash"
ATTR_REPOSITORY = "repository"
ATTR_TAG = "tag"
ATTR_REPOSITORIES = "repositories"
ATTR_TYPE = "type"

TYPE_MANIFEST = "manifest"
TYPE_BLOB = "blob"
TYPE_TAG = "tag"


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

        blob = self.graph.nodes[hash]

        if blob[ATTR_TYPE] != TYPE_BLOB:
            return False

        if repository not in self.graph.nodes[hash][ATTR_REPOSITORIES]:
            return False

        return True

    def is_manifest_available(self, repository, hash):
        if hash not in self.graph.nodes:
            return False

        manifest = self.graph.nodes[hash]

        if manifest[ATTR_TYPE] != TYPE_MANIFEST:
            return False

        if ATTR_CONTENT_TYPE not in manifest:
            return False

        if repository not in self.graph.nodes[hash][ATTR_REPOSITORIES]:
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
            repository = self.state.setdefault(entry[ATTR_REPOSITORY], {})
            repository[entry[ATTR_TAG]] = entry[ATTR_HASH]

            tags_for_hash = self.tags_for_hash.setdefault(entry[ATTR_HASH], set())
            tags_for_hash.add((entry[ATTR_REPOSITORY], entry[ATTR_TAG]))

        elif entry["type"] == RegistryActions.BLOB_MOUNTED:
            if entry[ATTR_HASH] not in self.graph.nodes:
                self.graph.add_node(entry[ATTR_HASH], **{ATTR_TYPE: TYPE_BLOB})

            self.graph.nodes[entry[ATTR_HASH]].setdefault(ATTR_REPOSITORIES, set()).add(
                entry[ATTR_REPOSITORY]
            )

        elif entry["type"] == RegistryActions.BLOB_UNMOUNTED:
            self.graph.nodes[entry[ATTR_HASH]][ATTR_REPOSITORIES].discard(
                entry[ATTR_REPOSITORY]
            )

        elif entry["type"] == RegistryActions.BLOB_INFO:
            for dependency in entry[ATTR_DEPENDENCIES]:
                self.graph.add_edge(entry[ATTR_HASH], dependency)
            self.graph.nodes[entry[ATTR_HASH]][ATTR_CONTENT_TYPE] = entry[
                ATTR_CONTENT_TYPE
            ]

        elif entry["type"] == RegistryActions.BLOB_STAT:
            self.graph.nodes[entry[ATTR_HASH]][ATTR_SIZE] = entry[ATTR_SIZE]

        elif entry["type"] == RegistryActions.MANIFEST_MOUNTED:
            if entry[ATTR_HASH] not in self.graph.nodes:
                self.graph.add_node(entry[ATTR_HASH], **{ATTR_TYPE: TYPE_MANIFEST})

            self.graph.nodes[entry[ATTR_HASH]].setdefault(ATTR_REPOSITORIES, set()).add(
                entry[ATTR_REPOSITORY]
            )

        elif entry["type"] == RegistryActions.MANIFEST_UNMOUNTED:
            self.graph.nodes[entry[ATTR_HASH]][ATTR_REPOSITORIES].discard(
                entry[ATTR_REPOSITORY]
            )

            # Legacy
            for repository, tag in set(self.tags_for_hash.get(entry[ATTR_HASH], [])):
                if repository != entry[ATTR_REPOSITORY]:
                    continue
                self.state.get(repository, {}).pop(tag, None)

        elif entry["type"] == RegistryActions.MANIFEST_INFO:
            for dependency in entry[ATTR_DEPENDENCIES]:
                self.graph.add_edge(entry[ATTR_HASH], dependency)
            self.graph.nodes[entry[ATTR_HASH]][ATTR_CONTENT_TYPE] = entry[
                ATTR_CONTENT_TYPE
            ]

        elif entry["type"] == RegistryActions.MANIFEST_INFO:
            self.graph.nodes[entry[ATTR_HASH]][ATTR_SIZE] = entry[ATTR_SIZE]
