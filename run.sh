#! /bin/bash
set -e

pushd rust
cargo build
popd
cp rust/target/debug/libdistribd_rust.dylib distribd/distribd_rust.so
poetry run python -m distribd --name default
