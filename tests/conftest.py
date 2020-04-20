import logging

import aiohttp
import pytest


@pytest.fixture
async def client_session(loop):
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=0.1)
    ) as session:
        yield session


@pytest.fixture(autouse=True)
def configure_logging(caplog):
    caplog.set_level(logging.DEBUG)
