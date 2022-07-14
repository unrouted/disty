# distribd

[![codecov](https://codecov.io/gh/distribd/distribd/branch/main/graph/badge.svg?token=8AIAEN68AO)](https://codecov.io/gh/distribd/distribd)

A simple clustered replicated container image registry. `distribd` is as simple and easy to deploy as [distribution](https://github.com/docker/distribution/) running in local file system mode, but nodes form a cluster and replicate images amongst themselves. A cluster of nodes can tolerate outages as long as there is a quorum.

:exclamation: `distribd` is currently still in Beta. Bug fixes welcome!. :exclamation:

## Is it for me?

There are a very specific set of circumstances where you might like a container image registry like this:

* You need to self-host, you can't use a cloud image registry and you can't use your hosts image registry (or it doesn't offer one)
* You need to run everything in-cluster (dedicated Quay or Harbor machines means less machines for Kubernetes)
* You need some level of fault tolerance

distribd is straightforward to deploy on a Kubernetes cluster and doesn't need you to supply fault tolerant storage or database layers.

## How does it work?

Artifacts are stored on local disk like with distribution. We use raft to replicate a metadata journal about the registry contents between cluster members. The jorunal contains:

* Hashes of blobs and manifests and where they are stored in the cluster
* Hashes of blobs and manifests and the repository that owned them. This is used so that you can't access blobs unless they belong to a repository you have access to.
* Tag/Hash pairs, by repository. This is for looking up all tags within a repository and for finding the hash to retrieve from a tag.

Each member of the cluster tries to maintain a full copy of all blobs and manifests. When it sees a new hash in the raft log it tries to retrieve it from the server that announced it. When a node has acquired a copy of the blob it records in the cluster that it has a copy of that blob.

Objects that are no longer reachable from a tag are automatically garbage collected and deleted from all cluster nodes.

## Setting up token auth

This works much like distribution. For more information about the basic flow see [here](https://docs.docker.com/registry/spec/auth/token/).

First of all you need to generate some keys:

```bash
openssl ecparam -genkey -name prime256v1 -noout -out token.key
openssl ec -in token.key -pubout -out token.pub
openssl req -x509 -new -key token.key -out token.crt -subj "/CN=unused"
```

If no token is present in a request, distribd redirects a client to a token server to get a token. The distribd registry then uses `token.pub` to verify tokens produced by a token server.
