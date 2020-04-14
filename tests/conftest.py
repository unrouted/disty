import aiohttp
import pytest


@pytest.fixture
async def client_session(loop):
    async with aiohttp.ClientSession() as session:
        yield session
