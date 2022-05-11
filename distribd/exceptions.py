import json

from aiohttp import web


class JSONExceptionMixin:
    def __init__(self, **kwargs):
        error = {
            "code": self.code,
            "message": self.message,
        }

        if kwargs:
            error["detail"] = kwargs

        super().__init__(
            headers={"Content-Type": "application/json"},
            text=json.dumps({"errors": [error]}),
        )


class LeaderUnavailable(JSONExceptionMixin, web.HTTPServiceUnavailable):

    """Cannot safely perform an operation because this node does not have leader."""

    code = "LEADER_UNAVAILABLE"
    message = "There is no cluster leader so cannot perform this operation"
