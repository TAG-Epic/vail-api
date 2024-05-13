from aiohttp import web
from slowstack.asynchronous.times_per import TimesPerRateLimiter

from ..utils.cors import api_cors
from ..utils.rate_limit import rate_limit_http
from .. import app_keys

router = web.RouteTableDef()


@router.get("/db.sqlite")
@api_cors
@rate_limit_http(lambda: TimesPerRateLimiter(6, 60))
async def download_db(request: web.Request) -> web.StreamResponse:
    config = request.app[app_keys.CONFIG]
    database_lock = request.app[app_keys.DATABASE_LOCK]

    if config.database.sqlite_url != ":memory:":
        async with database_lock.exclusive():
            response = web.FileResponse(config.database.sqlite_url)
            await response.prepare(request)
            return response

    return web.json_response({"detail": "in-memory db cannot be shared"}, status=500)
