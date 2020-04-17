from distribd import analyzer


def test_distribution_manifest_v1():
    content_type = "application/vnd.docker.distribution.manifest.v1+json"
    with open("tests/fixtures/vnd.docker.distribution.manifest.v1+json.1") as fp:
        manifest = fp.read()

    assert analyzer.analyze(content_type, manifest) == {
        "sha256:cc8567d70002e957612902a8e985ea129d831ebe04057d88fb644857caa45d11",
        "sha256:5f70bf18a086007016e948b04aed3b82103a36bea41755b6cddfaf10ace3c6ef",
    }


def test_distribution_manifest_list():
    content_type = "application/vnd.docker.distribution.manifest.list.v2+json"
    with open("tests/fixtures/vnd.docker.distribution.manifest.list.v2+json.1") as fp:
        manifest = fp.read()

    assert analyzer.analyze(content_type, manifest) == {
        "sha256:cc8567d70002e957612902a8e985ea129d831ebe04057d88fb644857caa45d11",
        "sha256:5f70bf18a086007016e948b04aed3b82103a36bea41755b6cddfaf10ace3c6ef",
    }


def test_distribution_manifest_v2():
    content_type = "application/vnd.docker.distribution.manifest.v2+json"
    with open("tests/fixtures/vnd.docker.distribution.manifest.v2+json.1") as fp:
        manifest = fp.read()

    assert analyzer.analyze(content_type, manifest) == {
        "sha256:3c3a4604a545cdc127456d94e421cd355bca5b528f4a9c1905b15da2eb4a4c6b",
        "sha256:b5b2b2c507a0944348e0303114d8d93aaaa081732b86451d9bce1f432a537bc7",
        "sha256:e692418e4cbaf90ca69d05a66403747baa33ee08806650b51fab815ad7fc331f",
        "sha256:ec4b8955958665577945c89419d1af06b5f7636b4ac3da7f12184802ad867736",
    }
