from aiohttp import web

from .... import app_keys
from ....utils.cors import api_cors

router = web.RouteTableDef()


@router.get("/api/v2/game/user-count")
@api_cors
async def get_user_count(request: web.Request) -> web.StreamResponse:
    db = request.app[app_keys.DATABASE]

    result = await db.execute("select count(*) from users")
    row = await result.fetchone()
    assert row is not None, "missing result from count(*) query"
    
    return web.json_response({"count": row[0]})
