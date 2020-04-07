# distribd

A simple replicated docker registry.

We use raft to replicate metadata about the registry:

* Hashes of blobs and manifests and where they are stored in the cluster
* Hashes of blobs and manifests and the repository that owned them. This is used so that you can't access blobs unless they belong to a repository you have access to.
* Tag/Hash pairs, by repository. This is for looking up all tags within a repository and for finding the hash to retrieve from a tag.

Each member of the cluster tries to maintain a full copy of all blobs and manifests. When it sees a new hash it tries to retrieve it from the server that announced it. When a node has acquired a copy of the blob it records in the cluster that it has a copy of that blob.
