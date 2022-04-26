import logging
import pathlib

import pytest


@pytest.fixture
def fixtures_path():
    return pathlib.Path(__file__).parent / "fixtures"


@pytest.fixture(autouse=True)
def configure_logging(caplog):
    caplog.set_level(logging.DEBUG)
