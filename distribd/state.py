import logging

from networkx import DiGraph, subgraph_view

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
        def _filter(node):
            print(node)
            n = self.graph.nodes[node]
            print(n)
            if n[ATTR_TYPE] != TYPE_TAG:
                print("NOT TAG")
                return False
            if n[ATTR_REPOSITORY] != repository:
                print("NOT REPO")
                return False
            return True

        tags = subgraph_view(self.graph, _filter)
        return [self.graph[tag]["tag"] for tag in tags]

    def get_tag(self, repository, tag):
        key = f"tag:{repository}:{tag}"
        if not key in self.graph:
            raise KeyError()
        return next(self.graph.neighbors(key))

    def dispatch(self, entry):
        logger.critical("Applying %s", entry)

        if entry["type"] == RegistryActions.HASH_TAGGED:
            tag = entry[ATTR_TAG]
            repository = entry[ATTR_REPOSITORY]
            key = f"tag:{repository}:{tag}"

            if key in self.graph.nodes:
                self.graph.remove_node(key)

            self.graph.add_node(key, **{
                ATTR_TAG: tag,
                ATTR_REPOSITORY: repository,
                ATTR_TYPE: TYPE_TAG,
            })
            self.graph.add_edge(key, entry[ATTR_HASH])

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
            manifest = self.graph.nodes[entry[ATTR_HASH]]
            manifest[ATTR_REPOSITORIES].discard(
                entry[ATTR_REPOSITORY]
            )

            for tag in list(self.graph.predecessors(entry[ATTR_HASH])):
                if self.graph.nodes[tag][ATTR_REPOSITORY] == entry[ATTR_REPOSITORY]:
                    self.graph.remove_node(tag)

        elif entry["type"] == RegistryActions.MANIFEST_INFO:
            for dependency in entry[ATTR_DEPENDENCIES]:
                self.graph.add_edge(entry[ATTR_HASH], dependency)
            self.graph.nodes[entry[ATTR_HASH]][ATTR_CONTENT_TYPE] = entry[
                ATTR_CONTENT_TYPE
            ]

        elif entry["type"] == RegistryActions.MANIFEST_INFO:
            self.graph.nodes[entry[ATTR_HASH]][ATTR_SIZE] = entry[ATTR_SIZE]
