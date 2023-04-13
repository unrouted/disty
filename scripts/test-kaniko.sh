#! /bin/sh

docker run \
    -v $(pwd):/workspace \
    gcr.io/kaniko-project/executor:v1.3.0 \
    --reproducible \
    --insecure \
    --dockerfile /workspace/Dockerfile \
    --destination "10.192.170.146:9080/distribd/distribd:dev" \
    --context dir:///workspace/

