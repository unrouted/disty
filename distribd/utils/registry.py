def get_blob_path(images_directory, digest):
    _, hash = digest.split(":", 1)
    return images_directory / "blobs" / hash[0:2] / hash[2:4] / hash[4:6] / hash[6:]


def get_manifest_path(images_directory, digest):
    _, hash = digest.split(":", 1)
    return images_directory / "manifests" / hash[0:2] / hash[2:4] / hash[4:6] / hash[6:]
