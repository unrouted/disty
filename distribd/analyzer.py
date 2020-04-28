import json
import logging
import os
import pathlib

from aiofile import AIOFile
from jsonschema import ValidationError, validate

from .actions import RegistryActions
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
        dependencies.add(("application/octet-stream", layer["blobSum"]))

    return dependencies


@analyzers.register("application/vnd.docker.distribution.manifest.v2+json")
def distribution_manifest_v2(manifest):
    """
    application/vnd.docker.distribution.manifest.v2+json: New image manifest format (schemaVersion = 2)

    https://github.com/docker/distribution/blob/master/docs/spec/manifest-v2-2.md#image-manifest-field-descriptions
    """
    dependencies = set()
    dependencies.add((manifest["config"]["mediaType"], manifest["config"]["digest"]))

    for layer in manifest["layers"]:
        dependencies.add((layer["mediaType"], layer["digest"]))

    return dependencies


@analyzers.register("application/vnd.docker.distribution.manifest.list.v2+json")
def distribution_manifest_v2_list(manifest):
    """
    application/vnd.docker.distribution.manifest.list.v2+json: Manifest list, aka “fat manifest”

    https://github.com/docker/distribution/blob/master/docs/spec/manifest-v2-2.md#manifest-list
    """

    dependencies = set()
    for layer in manifest["manifests"]:
        dependencies.add((layer["mediaType"], layer["digest"]))

    return dependencies


@analyzers.register("application/vnd.docker.container.image.v1+json")
def manifest_v1_json(manifest):
    """
    application/vnd.docker.container.image.v1+json: Container config JSON

    https://github.com/moby/moby/blob/master/image/spec/v1.md#image-json-description
    """
    dependencies = set()

    for layer in manifest["rootfs"]["diff_ids"]:
        dependencies.add(("application/octet-stream", layer))

    return dependencies


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
    logger.debug(f"MANIFEST: {content_type} start")
    try:
        parsed = json.loads(manifest)
    except json.decoder.JSONDecodeError:
        logger.debug(f"MANIFEST: invalid json")
        raise ManifestInvalid()

    if content_type not in analyzers:
        logger.debug(f"MANIFEST: no analyzer for {content_type}")
        return []

    if content_type not in schemas:
        logger.debug(f"MANIFEST: no schema for {content_type}")
        raise ManifestInvalid(reason="no_schema_for_this_type")

    schema = schemas[content_type]

    try:
        validate(instance=parsed, schema=schema)
    except ValidationError:
        logger.debug(f"MANIFEST: schema check fail")
        raise ManifestInvalid(reason="schema_check_fail")

    logger.debug(f"MANIFEST: invoking dep extractor")

    return analyzers[content_type](parsed)


async def recursive_analyze(mirrorer, repository, content_type, manifest):
    analysis = []

    logger.debug("Processing: %r", manifest)

    dependencies = set(analyze(content_type, manifest))
    seen = set()

    direct_deps = list({digest for (content_type, digest) in dependencies})

    while dependencies:
        content_type, digest = dependencies.pop()

        if digest not in mirrorer.blob_repos:
            logger.debug(f"MANIFEST: {digest} missing")
            raise ManifestInvalid(reason=f"{digest} missing")

        if repository not in mirrorer.blob_repos[digest]:
            logger.debug(f"MANIFEST: {digest} missing from repo")
            raise ManifestInvalid(reason=f"{digest} missing")

        path = await mirrorer.wait_for_blob(digest)

        info = {
            "type": RegistryActions.BLOB_INFO,
            "hash": digest,
            "dependencies": [],
            "content_type": content_type,
        }

        analysis.append(info)

        if content_type not in schemas:
            continue

        async with AIOFile(path, "r") as afp:
            manifest = await afp.read()

        try:
            results = analyze(content_type, manifest)
        except ManifestInvalid:
            logger.debug(f"MANIFEST: {digest} invalid")
            raise ManifestInvalid(reason=f"{digest} invalid")

        for content_type, digest in results:
            info["dependencies"].append(digest)

            if digest in seen:
                continue

            dependencies.add((content_type, digest))

    return direct_deps, analysis


schemas = load_schemas()
