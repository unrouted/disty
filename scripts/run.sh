#! /bin/bash
set -e
maturin develop -E test,integration
python -m distribd --name node1
