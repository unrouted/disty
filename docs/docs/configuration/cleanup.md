## Automatic tag expiry

Content is only garbaged collected when there are no references to it any more. By default, no tags are expired so content is only garbaged collected when a tag is either updated to point at a different manifest or if something is explicitly deleted.

You can expire old tags with cleanup rules:

```yaml
cleanup:
- type: tag
  repository: { ends: /cache }
  tag: { starts: sha256- }
  older_than: 30
```

## Other garbage collection

The following garbage collection operations happen after tag expiry. They are currently not configurable.

* Manifests that are not referenced by tags (or other manifests) will be deleted from disk on all nodes and then dropped from the database. There is a grace period of a day.
* If a blob is in a repository but not referenced by a manifest in that repository, its unmounted from that repository. There is a grace period of a day.
* A blob that cannot be reached from any repository is removed from disk on all nodes and then dropped from the database.