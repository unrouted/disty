import json

from .exceptions import ManifestInvalid
from .utils.dispatch import Dispatcher

analyzers = Dispatcher()


@analyzers.register("application/vnd.docker.distribution.manifest.v1+json")
@analyzers.register("application/vnd.docker.distribution.manifest.v1+prettyjws")
def distribution_manifest_v1(manifest):
    """ application/vnd.docker.distribution.manifest.v1+json: schema1 (existing manifest format) """
    dependencies = set()
    for layer in manifest["fsLayers"]:
        dependencies.add(layer["blobSum"])

    return dependencies


@analyzers.register("application/vnd.docker.distribution.manifest.v2+json")
def distribution_manifest_v2(manifest):
    """
    application/vnd.docker.distribution.manifest.v2+json: New image manifest format (schemaVersion = 2)

    https://github.com/docker/distribution/blob/master/docs/spec/manifest-v2-2.md#image-manifest-field-descriptions
    """
    dependencies = set()

    if "config" not in manifest:
        raise ManifestInvalid(reason="no_config")

    if "digest" not in manifest["config"]:
        raise ManifestInvalid(reason="no_config_digest")

    if "layers" not in manifest:
        raise ManifestInvalid(reason="no_layers")

    dependencies.add(manifest["config"["digest"]])

    for layer in manifest["layers"]:
        if "digest" not in layer:
            raise ManifestInvalid(reason="layer_missing_digest")
        dependencies.add(manifest["digest"])

    return dependencies


@analyzers.register("application/vnd.docker.distribution.manifest.list.v2+json")
def distribution_manifest_v2_list(manifest):
    """
    application/vnd.docker.distribution.manifest.list.v2+json: Manifest list, aka “fat manifest”

    https://github.com/docker/distribution/blob/master/docs/spec/manifest-v2-2.md#manifest-list
    """

    dependencies = set()
    for layer in manifest["manifests"]:
        dependencies.add(layer["digest"])

    return dependencies


@analyzers.register("application/vnd.docker.container.image.v1+json")
def manifest_v1_json(manifest):
    """
    application/vnd.docker.container.image.v1+json: Container config JSON

    https://github.com/moby/moby/blob/master/image/spec/v1.md#image-json-description
    """
    return []


@analyzers.register("application/vnd.docker.distribution.manifest.v1+json")
def plugin_json(manifest):
    """
    application/vnd.docker.plugin.v1+json: Plugin config JSON

    https://docs.docker.com/engine/extend/config/
    """
    return []


def analyze(content_type, manifest):
    try:
        parsed = json.loads(manifest)
    except json.decoder.JSONDecodeError:
        raise ManifestInvalid()

    if content_type not in analyzers:
        return []

    return analyzers[content_type](parsed)
