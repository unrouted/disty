# distribd

A simple replicated docker registry.

We use raft to replicate metadata about the registry:

* Hashes of blobs and manifests and where they are stored in the cluster
* Hashes of blobs and manifests and the repository that owned them. This is used so that you can't access blobs unless they belong to a repository you have access to.
* Tag/Hash pairs, by repository. This is for looking up all tags within a repository and for finding the hash to retrieve from a tag.

Each member of the cluster tries to maintain a full copy of all blobs and manifests. When it sees a new hash it tries to retrieve it from the server that announced it. When a node has acquired a copy of the blob it records in the cluster that it has a copy of that blob.

## Setting up token auth

This works much like distribution. For more information about the basic flow see [here](https://docs.docker.com/registry/spec/auth/token/).

First of all you need to generate some keys:

```bash
openssl ecparam -genkey -name prime256v1 -noout -out token.key
openssl ec -in token.key -pubout -out token.pub
openssl req -x509 -new -key token.key -out token.crt -subj "/CN=unused"
```

If no token is present in a request, distribd redirects a client to a token server to get a token. The distribd registry then uses `token.pub` to verify tokens produced by a token server.
