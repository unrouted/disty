import json
import logging
import os
import pathlib

from jsonschema import ValidationError, validate

from .exceptions import ManifestInvalid
from .utils.dispatch import Dispatcher

logger = logging.getLogger(__name__)

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
    dependencies.add(manifest["config"]["digest"])

    for layer in manifest["layers"]:
        dependencies.add(layer["digest"])

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
    raise ManifestInvalid(reason="legacy_format_not_supported")


@analyzers.register("application/vnd.docker.plugin.v1+json")
def plugin_json(manifest):
    """
    application/vnd.docker.plugin.v1+json: Plugin config JSON

    https://docs.docker.com/engine/extend/config/
    """
    return []


def load_schemas():
    schemas = {}

    schema_path = pathlib.Path(__file__).parent / "schemas"
    for path in os.listdir(schema_path):
        sub_type = path.rsplit(".", 1)[0]
        content_type = f"application/{sub_type}"

        logger.debug("Loading schema %s", path)
        with open(schema_path / path, "r") as fp:
            schemas[content_type] = json.load(fp)

    return schemas


def analyze(content_type, manifest):
    try:
        parsed = json.loads(manifest)
    except json.decoder.JSONDecodeError:
        raise ManifestInvalid()

    if content_type not in analyzers:
        return []

    if content_type not in schemas:
        raise ManifestInvalid(reason="no_schema_for_this_type")

    schema = schemas[content_type]

    try:
        validate(instance=parsed, schema=schema)
    except ValidationError:
        raise ManifestInvalid(reason="schema_check_fail")

    return analyzers[content_type](parsed)


schemas = load_schemas()
