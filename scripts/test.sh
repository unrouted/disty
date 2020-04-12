#! /bin/sh
set -e

python -m black tests distribd
python -m isort -rc tests distribd
python -m black tests distribd --check --diff
python -m flake8 tests distribd
python -m pytest --cov=. tests
python -m coverage html
