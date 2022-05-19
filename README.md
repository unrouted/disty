# distribd

[![codecov](https://codecov.io/gh/Jc2k/distribd/branch/main/graph/badge.svg?token=8AIAEN68AO)](https://codecov.io/gh/Jc2k/distribd)

A simple replicated docker registry cluster. `distribd` is like a [distribution](https://github.com/docker/distribution/) instance running in local file system mode, but nodes can form a cluster and replicate images amongst themselves. A cluster of nodes can tolerate outages as long as there is a quorum.

:exclamation: `distribd` is currently still in Alpha. In particular, we just moved from Python to Rust. Stay away unless you want to help fix bugs. :exclamation:

## Is it for me?

There is no denying it, distribd is niche. You probably shouldn't use it.

* If you are OK using a private repository on a public registry then you probably should.
* If you are on the cloud then you should probably use their registry.
* If you are self-hosting and have the resources (both hardware and manpower) to run a full registry like [Quay](https://www.openshift.com/products/quay) or [Harbor](https://goharbor.io/), you probably should.
* If you already have a self-hosted GitLab and have modest requirements around access control from your kubernetes install, you probably should its container reigsry.
* If you have a fault tolerant object store like MinIO or file store then you should just configure [distribution](https://github.com/docker/distribution/) to use it.

On the other hand, if you have a small to medium self-hosted cluster, and want to run a simple fault tolerant minimal registry then distribd may be for you.

## How does it work?

Artifacts are stored on local disk like with distribution. Then it uses raft to replicate metadata about the registry contents:

* Hashes of blobs and manifests and where they are stored in the cluster
* Hashes of blobs and manifests and the repository that owned them. This is used so that you can't access blobs unless they belong to a repository you have access to.
* Tag/Hash pairs, by repository. This is for looking up all tags within a repository and for finding the hash to retrieve from a tag.

Each member of the cluster tries to maintain a full copy of all blobs and manifests. When it sees a new hash in the raft log it tries to retrieve it from the server that announced it. When a node has acquired a copy of the blob it records in the cluster that it has a copy of that blob.

## Setting up token auth

This works much like distribution. For more information about the basic flow see [here](https://docs.docker.com/registry/spec/auth/token/).

First of all you need to generate some keys:

```bash
openssl ecparam -genkey -name prime256v1 -noout -out token.key
openssl ec -in token.key -pubout -out token.pub
openssl req -x509 -new -key token.key -out token.crt -subj "/CN=unused"
```

If no token is present in a request, distribd redirects a client to a token server to get a token. The distribd registry then uses `token.pub` to verify tokens produced by a token server.
