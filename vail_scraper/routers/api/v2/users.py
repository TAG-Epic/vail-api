from aiohttp import web

from ....models.meilisearch import SearchIndex
from ....errors import APIErrorCode
from .... import app_keys
from ....utils.cors import api_cors

router = web.RouteTableDef()


@router.get("/api/v2/users/search")
@api_cors
async def search_user(request: web.Request) -> web.StreamResponse:
    meilisearch = request.app[app_keys.MEILISEARCH]

    if "name" not in request.query:
        return web.json_response(
            {
                "detail": "missing param name",
                "code": APIErrorCode.MISSING_QUERY_PARAMETER,
                "parameter": "query",
            }
        )

    name = request.query["name"]

    results = await meilisearch.search(SearchIndex.USERS, name)

    data = {"items": results.hits}

    return web.json_response(data)


@router.get("/api/v2/users/{id}")
@api_cors
async def get_user(request: web.Request) -> web.StreamResponse:
    database = request.app[app_keys.DATABASE]

    result = await database.execute(
        "select name from users where id = ? limit 1", [request.match_info["id"]]
    )
    row = await result.fetchone()
    if row is None:
        return web.json_response(
            {
                "detail": "user not found/not scraped yet",
                "code": APIErrorCode.USER_NOT_FOUND,
            },
            status=404,
        )
    return web.json_response({"id": request.match_info["id"], "name": row["name"]})
