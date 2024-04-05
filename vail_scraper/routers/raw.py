from aiohttp import web

from .. import app_keys

router = web.RouteTableDef()

@router.get("/db.sqlite")
async def download_db(request: web.Request) -> web.StreamResponse:
    config = request.app[app_keys.CONFIG]
    database_lock = request.app[app_keys.DATABASE_LOCK]
    
    if config.database_url != ":memory:":
        async with database_lock.exclusive():
            response = web.FileResponse(config.database_url)
            await response.prepare(request)
            return response
    
    return web.json_response({"detail": "in-memory db cannot be shared"}, status=500)
