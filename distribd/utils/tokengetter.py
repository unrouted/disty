import aiohttp
from yarl import URL


class TokenGetter:
    def __init__(
        self, session: aiohttp.ClientSession, realm, service, username, password
    ):
        self.session = session

        self.realm = realm
        self.service = service
        self.username = username
        self.password = password

        self.url = URL(realm).update_query(service)
        self.auth = aiohttp.BasicAuth(self.username, self.password)

    async def get_token(self, repository, actions):
        actions_str = ",".join(actions)
        scope = f"repository:{repository}:{actions_str}"
        url = self.url.update_query(scope=scope)

        async with self.session.get(url, auth=self.auth) as resp:
            if resp.status != 200:
                raise RuntimeError("Unable to get authentication token")
            payload = await resp.json()
            return payload["token"]
