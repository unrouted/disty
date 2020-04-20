import logging

from jwt import decode
from jwt.exceptions import InvalidTokenError

from . import exceptions

logger = logging.getLogger(__name__)


def authenticate(request, repository=None, actions=None):
    token_server = request.app["token_server"]

    if not token_server.get("enabled", False):
        return True

    realm = token_server["realm"]
    service = token_server["service"]
    public_key = token_server["public_key"]

    if "Authorization" not in request.headers:
        raise exceptions.Unauthorized(realm, service, repository, actions)

    auth_header = request.headers["Authorization"]
    if not auth_header.startswith("Bearer "):
        raise exceptions.Unauthorized(realm, service, repository, actions)

    bearer_token = auth_header.split(" ", 1)[1]

    try:
        decoded = decode(bearer_token, public_key, algorithms="ES256", audience=service)
    except InvalidTokenError as e:
        logger.warning("Request denied due to invalid token: %s", str(e))
        raise exceptions.Denied()

    for access in decoded["access"]:
        if access.get("type") != "repository":
            continue
        if access.get("name") != repository:
            continue
        if not set(actions).issubset(set(access.get("actions", []))):
            continue

        return True

    raise exceptions.Denied()
