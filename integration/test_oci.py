from docker.client import DockerClient
from pytest_docker_tools import fetch

conformance = fetch(
    repository="ghcr.io/opencontainers/distribution-spec/conformance:v1.0.0",
)

skopeo = fetch(
    repository="quay.io/skopeo/stable:latest",
)


def test_oci(
    request, cluster, conformance, temporary_network, docker_client: DockerClient
):
    container = docker_client.containers.run(
        conformance.id,
        network=temporary_network.id,
        hostname="conformance",
        environment={
            "OCI_ROOT_URL": "http://node1:8000",
            "OCI_NAMESPACE": "example3/example4",
            "OCI_USERNAME": "admin",
            "OCI_PASSWORD": "badmin",
            "OCI_TEST_PULL": "1",
            "OCI_TEST_PUSH": "1",
            "OCI_TEST_CONTENT_DISCOVERY": "1",
            "OCI_TEST_CONTENT_MANAGEMENT": "1",
            "OCI_HIDE_SKIPPED_WORKFLOWS": "0",
            "OCI_DEBUG": "1",
            "OCI_DELETE_MANIFEST_BEFORE_BLOBS": "0",
        },
        detach=True,
    )
    request.addfinalizer(container.remove)

    for line in container.logs(stream=True):
        print(line.decode("utf-8"))

    assert container.wait()["StatusCode"] == 0


def test_skopeo(
    request, cluster, skopeo, temporary_network, docker_client: DockerClient
):
    container = docker_client.containers.run(
        skopeo.id,
        network=temporary_network.id,
        hostname="skopeo",
        command=[
            "copy",
            "--dest-tls-verify=false",
            "--dest-creds",
            "admin:badmin",
            "docker://alpine:3.15",
            "docker://node1:8000/alpine:3.15",
        ],
        detach=True,
    )
    request.addfinalizer(container.remove)

    for line in container.logs(stream=True):
        print(line.decode("utf-8"))

    assert container.wait()["StatusCode"] == 0


def test_mirrors(
    request, cluster, skopeo, temporary_network, docker_client: DockerClient
):
    container = docker_client.containers.run(
        skopeo.id,
        network=temporary_network.id,
        hostname="skopeo",
        command=[
            "copy",
            "--dest-tls-verify=false",
            "--dest-creds",
            "admin:badmin",
            "docker://alpine:3.15",
            "docker://node1:8000/alpine:3.15",
        ],
        detach=True,
    )
    request.addfinalizer(container.remove)

    for line in container.logs(stream=True):
        print(line.decode("utf-8"))

    assert container.wait()["StatusCode"] == 0

    container = docker_client.containers.run(
        skopeo.id,
        network=temporary_network.id,
        hostname="skopeo",
        command=[
            "copy",
            "--src-tls-verify=false",
            "--src-creds",
            "admin:badmin",
            "--dest-tls-verify=false",
            "--dest-creds",
            "admin:badmin",
            "docker://node2:8000/alpine:3.15",
            "docker://node3:8000/alpine-copy1:3.15",
        ],
        detach=True,
    )
    request.addfinalizer(container.remove)

    for line in container.logs(stream=True):
        print(line.decode("utf-8"))

    assert container.wait()["StatusCode"] == 0