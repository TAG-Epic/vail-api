from datetime import datetime
from aiohttp import web

from vail_scraper.errors import APIErrorCode

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

@router.get("/api/v2/game/user-count/timeseries")
@api_cors
async def get_user_count_time_series(request: web.Request) -> web.StreamResponse:
    db = request.app[app_keys.QUEST_DB_POSTGRES]
    
    try:
        before_timestamp = datetime.fromtimestamp(float(request.query.getone("before", "0")))
    except ValueError as error:
        return web.json_response({"code": APIErrorCode.QUERY_PARAMETER_INVALID, "detail": f"failed to parse the before parameter: {error}", "field": "before"}, status=400)
    
    try:
        after_timestamp = datetime.fromtimestamp(float(request.query.getone("after")))
    except KeyError:
        after_timestamp = datetime.utcnow()
    except ValueError as error:
        return web.json_response({"code": APIErrorCode.QUERY_PARAMETER_INVALID, "detail": f"failed to parse the after parameter: {error}", "field": "after"}, status=400)
    
    try:
        limit = int(request.query.getone("limit"))
    except KeyError:
        limit = 100
    except ValueError as error:
        return web.json_response({"code": APIErrorCode.QUERY_PARAMETER_INVALID, "detail": f"failed to parse the limit parameter: {error}", "field": "limit"}, status=400)

    if limit <= 0:
        return web.json_response({"code": APIErrorCode.QUERY_PARAMETER_INVALID, "detail": "the limit parameter must be more than 0", "field": "limit"}, status=400)
    if limit > 100:
        return web.json_response({"code": APIErrorCode.QUERY_PARAMETER_INVALID, "detail": "the limit parameter must not be more than 100", "field": "limit"}, status=400)

    rows = await db.fetch("select timestamp, count from user_count where timestamp between $1 and $2 order by timestamp desc limit $3", after_timestamp, before_timestamp, limit)

    items = []

    for row in rows:
        items.append({"timestamp": str(row[0].timestamp()), "count": row[1]})

    print(items)

    return web.json_response({"items": items})
    
