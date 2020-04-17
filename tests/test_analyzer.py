import json

from distribd import analyzer


def test_manifest_list():
    with open("tests/fixtures/hello_world.manifest_list") as fp:
        manifest = json.load(fp)

    assert analyzer.distribution_manifest_v2_list(manifest) == {
        'sha256:cc8567d70002e957612902a8e985ea129d831ebe04057d88fb644857caa45d11',
        'sha256:5f70bf18a086007016e948b04aed3b82103a36bea41755b6cddfaf10ace3c6ef',
    }
