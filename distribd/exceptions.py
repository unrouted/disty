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


class BlobUnknown(JSONExceptionMixin, web.HTTPNotFound):

    """This error MAY be returned when a blob is unknown to the registry in a specified repository. This can be returned with a standard get or if a manifest references an unknown layer during upload."""

    code = "BLOB_UNKNOWN"
    message = "blob unknown to registry"


class BlobUploadInvalid(JSONExceptionMixin, web.HTTPBadRequest):

    """The blob upload encountered an error and can no longer proceed."""

    code = "BLOB_UPLOAD_INVALID"
    message = "blob upload invalid"


class DigestInvalid(JSONExceptionMixin, web.HTTPBadRequest):

    """ When a blob is uploaded, the registry will check that the content matches the digest provided by the client. The error MAY include a detail structure with the key "digest", including the invalid digest string. This error MAY also be returned when a manifest includes an invalid layer digest. """

    code = "DIGEST_INVALID"
    message = "provided digest did not match uploaded content"


class ManifestBlobUnknown(JSONExceptionMixin, web.HTTPNotFound):

    """This error MAY be returned when a manifest blob is unknown to the registry."""

    code = "MANIFEST_BLOB_UNKNOWN"
    message = "blob unknown to registry"


class ManifestInvalid(JSONExceptionMixin, web.HTTPBadRequest):

    """During upload, manifests undergo several checks ensuring validity. If those checks fail, this error MAY be returned, unless a more specific error is included. The detail will contain information the failed validation."""

    code = "MANIFEST_INVALID"
    message = "manifest invalid"


class ManifestUnknown(JSONExceptionMixin, web.HTTPNotFound):

    """This error is returned when the manifest, identified by name and tag is unknown to the repository."""

    code = "MANIFEST_UNKNOWN"
    message = "manifest unknown"


class ManifestUnverified(JSONExceptionMixin, web.HTTPBadRequest):

    """During manifest upload, if the manifest fails signature verification, this error will be returned."""

    code = "MANIFEST_UNVERIFIED"
    message = "manifest failed signature verification"


class NameInvalid(JSONExceptionMixin, web.HTTPNotFound):

    """Invalid repository name encountered either during manifest validation or any API operation."""

    code = "NAME_INVALID"
    message = "invalid repository name"


class NameUnknown(JSONExceptionMixin, web.HTTPNotFound):

    """This is returned if the name used during an operation is unknown to the registry."""

    code = "NAME_UNKNOWN"
    message = "repository name not known to registry"


class SizeInvalid(JSONExceptionMixin, web.HTTPBadRequest):

    """When a layer is uploaded, the provided size will be checked against the uploaded content. If they do not match, this error will be returned."""

    code = "SIZE_INVALID"
    message = "provided length did not match content length"


class TagInvalid(JSONExceptionMixin, web.HTTPBadRequest):

    """During a manifest upload, if the tag in the manifest does not match the uri tag, this error will be returned."""

    code = "TAG_INVALID"
    message = "manifest tag did not match URI"


class Unauthorized(JSONExceptionMixin, web.HTTPUnauthorized):

    """The access controller was unable to authenticate the client. Often this will be accompanied by a Www-Authenticate HTTP response header indicating how to authenticate."""

    code = "UNAUTHORIZED"
    message = "authentication required"


class Denied(JSONExceptionMixin, web.HTTPForbidden):

    """The access controller denied access for the operation on a resource."""

    code = "DENIED"
    message = "requested access to the resource is denied"


class Unsupported(JSONExceptionMixin, web.HTTPMethodNotAllowed):

    """The operation was unsupported due to a missing implementation or invalid set of parameters."""

    code = "UNSUPPORTED"
    message = "The operation is unsupported."


class LeaderUnavailable(JSONExceptionMixin, web.HTTPServiceUnavailable):

    """Cannot safely perform an operation because this node does not have leader."""

    code = "LEADER_UNAVAILABLE"
    message = "There is no cluster leader so cannot perform this operation"
