/*


@routes.get("/v2/{repository:[^{}]+}/tags/list")
async def list_images_in_repository(request):
    registry_state = request.app["registry_state"]
    repository = request.match_info["repository"]

    request.app["token_checker"].authenticate(request, repository, ["pull"])

    try:
        tags = registry_state.get_tags(repository)
    except KeyError:
        raise exceptions.NameUnknown(repository=repository)

    tags.sort()

    include_link = False

    last = request.query.get("last", None)
    if last:
        start = tags.index(last)
        tags = tags[start:]

    n = request.query.get("n", None)
    if n is not None:
        n = int(n)
        if n < len(tags):
            include_link = True

        tags = tags[:n]

    headers = {}

    if include_link:
        url = URL(f"/v2/{repository}/tags/list")
        if n is not None:
            url = url.update_query({"n": str(n)})
        url = url.update_query({"last": tags[-1]})
        headers["Link"] = f'{url}; rel="next"'

    return web.json_response(
        {"name": repository, "tags": tags}, headers=headers, dumps=ujson.dumps
    )
    
*/