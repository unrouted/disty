import logging

import iso8601
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
ATTR_LOCATIONS = "locations"
ATTR_LOCATION = "location"
ATTR_TIMESTAMP = "timestamp"
ATTR_CREATED = "created"
ATTR_UPDATED = "updated"

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

    def get_blob(self, repository, hash):
        blob = self.graph.nodes[hash]

        if blob[ATTR_TYPE] != TYPE_BLOB:
            raise KeyError("No such hash")

        if repository not in self.graph.nodes[hash][ATTR_REPOSITORIES]:
            raise KeyError("No such hash")

        return blob

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

    def get_manifest(self, repository, hash):
        manifest = self.graph.nodes[hash]

        if manifest[ATTR_TYPE] != TYPE_MANIFEST:
            raise KeyError("No such hash")

        if repository not in self.graph.nodes[hash][ATTR_REPOSITORIES]:
            raise KeyError("No such hash")

        manifest = dict(manifest)
        manifest["dependencies"] = []

        return manifest

    def get_orphaned_objects(self):
        """
        Returns all nodes that have no incoming edges and are not a tag.

        These nodes should be deleted by a mirror, and when this has
        happened they should report that with a BLOB_UNMOUNTED or MANIFEST_UNMOUNTED.
        """
        orphaned = set()
        for v in self.graph:
            if self.graph.nodes[v][ATTR_TYPE] == TYPE_TAG:
                continue
            if len(self.graph.in_edges(v)) == 0:
                orphaned.add(v)
        return orphaned

    def get_orphaned_blobs(self):
        """
        Returns all nodes that have no incoming edges and are a blob.
        """
        orphaned = set()
        for v in self.graph:
            if self.graph.nodes[v][ATTR_TYPE] != TYPE_BLOB:
                continue
            if len(self.graph.in_edges(v)) == 0:
                orphaned.add(v)
        return [self.graph.nodes[v] for v in orphaned]

    def get_orphaned_manifests(self):
        """
        Returns all nodes that have no incoming edges and are a manifest.
        """
        orphaned = set()
        for v in self.graph:
            if self.graph.nodes[v][ATTR_TYPE] != TYPE_MANIFEST:
                continue
            if len(self.graph.in_edges(v)) == 0:
                orphaned.add(v)
        return [self.graph.nodes[v] for v in orphaned]

    def get_tags(self, repository):
        logger.critical("get_tags: START: %s", repository)

        def _filter(node):
            n = self.graph.nodes[node]
            if n[ATTR_TYPE] != TYPE_TAG:
                return False
            if n[ATTR_REPOSITORY] != repository:
                return False
            return True

        tags = subgraph_view(self.graph, _filter)
        resolved_tags = [tags.nodes[tag][ATTR_TAG] for tag in tags.nodes]
        if len(resolved_tags) == 0:
            logger.critical("get_tags: FIN: %s: NO TAGS", repository)
            raise KeyError()

        logger.critical("get_tags: FIN: %s: TAGS: %s", repository, resolved_tags)

        return resolved_tags

    def get_tag(self, repository, tag):
        key = f"tag:{repository}:{tag}"
        if key not in self.graph:
            raise KeyError()
        return next(self.graph.neighbors(key))

    def dispatch(self, entry):
        logger.critical("Applying %s", entry)

        timestamp_str = entry.get(ATTR_TIMESTAMP, "2022-04-29T17:03:04.285771+00:00")
        timestamp = iso8601.parse_date(timestamp_str)

        if entry["type"] == RegistryActions.HASH_TAGGED:
            tag = entry[ATTR_TAG]
            repository = entry[ATTR_REPOSITORY]
            key = f"tag:{repository}:{tag}"

            created = timestamp
            if key in self.graph.nodes:
                created = self.graph.nodes[key][ATTR_CREATED]
                self.graph.remove_node(key)

            self.graph.add_node(
                key,
                **{
                    ATTR_TAG: tag,
                    ATTR_REPOSITORY: repository,
                    ATTR_TYPE: TYPE_TAG,
                    ATTR_CREATED: created,
                    ATTR_UPDATED: timestamp,
                },
            )
            self.graph.add_edge(key, entry[ATTR_HASH])

        elif entry["type"] == RegistryActions.BLOB_MOUNTED:
            if entry[ATTR_HASH] not in self.graph.nodes:
                self.graph.add_node(
                    entry[ATTR_HASH],
                    **{
                        ATTR_TYPE: TYPE_BLOB,
                        ATTR_CREATED: timestamp,
                        ATTR_REPOSITORIES: set(),
                        ATTR_LOCATIONS: set(),
                    },
                )

            node = self.graph.nodes[entry[ATTR_HASH]]
            node[ATTR_REPOSITORIES].add(entry[ATTR_REPOSITORY])
            node[ATTR_UPDATED] = timestamp

        elif entry["type"] == RegistryActions.BLOB_UNMOUNTED:
            node = self.graph.nodes[entry[ATTR_HASH]]
            node[ATTR_REPOSITORIES].discard(entry[ATTR_REPOSITORY])
            node[ATTR_UPDATED] = timestamp

        elif entry["type"] == RegistryActions.BLOB_INFO:
            node = self.graph.nodes[entry[ATTR_HASH]]
            for dependency in entry[ATTR_DEPENDENCIES]:
                self.graph.add_edge(entry[ATTR_HASH], dependency)
            node[ATTR_CONTENT_TYPE] = entry[ATTR_CONTENT_TYPE]
            node[ATTR_UPDATED] = timestamp

        elif entry["type"] == RegistryActions.BLOB_STAT:
            node = self.graph.nodes[entry[ATTR_HASH]]
            node[ATTR_SIZE] = entry[ATTR_SIZE]
            node[ATTR_UPDATED] = timestamp

        elif entry["type"] == RegistryActions.BLOB_STORED:
            node = self.graph.nodes[entry[ATTR_HASH]]
            node[ATTR_UPDATED] = timestamp
            node[ATTR_LOCATIONS].add(entry[ATTR_LOCATION])

        elif entry["type"] == RegistryActions.BLOB_UNSTORED:
            if entry[ATTR_HASH] not in self.graph.nodes:
                return
            node = self.graph.nodes[entry[ATTR_HASH]]
            node[ATTR_LOCATIONS].discard(entry[ATTR_LOCATION])
            node[ATTR_UPDATED] = timestamp
            if len(node[ATTR_LOCATIONS]) == 0:
                self.graph.remove_node(entry[ATTR_HASH])

        elif entry["type"] == RegistryActions.MANIFEST_MOUNTED:
            if entry[ATTR_HASH] not in self.graph.nodes:
                self.graph.add_node(
                    entry[ATTR_HASH],
                    **{
                        ATTR_TYPE: TYPE_MANIFEST,
                        ATTR_CREATED: timestamp,
                        ATTR_REPOSITORIES: set(),
                        ATTR_LOCATIONS: set(),
                    },
                )

            node = self.graph.nodes[entry[ATTR_HASH]]
            node[ATTR_REPOSITORIES].add(entry[ATTR_REPOSITORY])
            node[ATTR_UPDATED] = timestamp

        elif entry["type"] == RegistryActions.MANIFEST_UNMOUNTED:
            manifest = self.graph.nodes[entry[ATTR_HASH]]
            manifest[ATTR_REPOSITORIES].discard(entry[ATTR_REPOSITORY])
            manifest[ATTR_UPDATED] = timestamp

            for tag in list(self.graph.predecessors(entry[ATTR_HASH])):
                if self.graph.nodes[tag][ATTR_REPOSITORY] == entry[ATTR_REPOSITORY]:
                    self.graph.remove_node(tag)

        elif entry["type"] == RegistryActions.MANIFEST_INFO:
            node = self.graph.nodes[entry[ATTR_HASH]]
            for dependency in entry[ATTR_DEPENDENCIES]:
                self.graph.add_edge(entry[ATTR_HASH], dependency)
            node[ATTR_CONTENT_TYPE] = entry[ATTR_CONTENT_TYPE]
            node[ATTR_UPDATED] = timestamp

        elif entry["type"] == RegistryActions.MANIFEST_STAT:
            node = self.graph.nodes[entry[ATTR_HASH]]
            node[ATTR_SIZE] = entry[ATTR_SIZE]
            node[ATTR_UPDATED] = timestamp

        elif entry["type"] == RegistryActions.MANIFEST_STORED:
            node = self.graph.nodes[entry[ATTR_HASH]]
            node[ATTR_LOCATIONS].add(entry[ATTR_LOCATION])
            node[ATTR_UPDATED] = timestamp

        elif entry["type"] == RegistryActions.MANIFEST_UNSTORED:
            if entry[ATTR_HASH] not in self.graph.nodes:
                return
            node = self.graph.nodes[entry[ATTR_HASH]]
            node[ATTR_LOCATIONS].discard(entry[ATTR_LOCATION])
            node[ATTR_UPDATED] = timestamp
            if len(node[ATTR_LOCATIONS]) == 0:
                self.graph.remove_node(entry[ATTR_HASH])
