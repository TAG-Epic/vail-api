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

    raw_before_timestamp = request.query.getone("before")
    raw_after_timestamp = request.query.getone("after")

    if raw_before_timestamp is not None and raw_after_timestamp is not None:
        return web.json_response({"code": APIErrorCode.MUTUALLY_EXCLUSIVE_QUERY_PARAMETERS, "detail": "you can only pass before or after, not both"}, status=400)
    
    # Limit
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

    if raw_before_timestamp is not None:
        try:
            before_timestamp = datetime.fromtimestamp(float(raw_before_timestamp))
        except ValueError as error:
            return web.json_response({"code": APIErrorCode.QUERY_PARAMETER_INVALID, "detail": f"failed to parse the before parameter: {error}", "field": "before"}, status=400)

        rows = await db.fetch("select timestamp, count from user_count where timestamp < $1 order by timestamp desc limit $2", before_timestamp, limit)
    elif raw_after_timestamp is not None:
        try:
            after_timestamp = datetime.fromtimestamp(float(raw_after_timestamp))
        except ValueError as error:
            return web.json_response({"code": APIErrorCode.QUERY_PARAMETER_INVALID, "detail": f"failed to parse the after parameter: {error}", "field": "before"}, status=400)

        rows = await db.fetch("select timestamp, count from user_count where timestamp > $1 order by timestamp asc limit $2", after_timestamp, limit)
    else:
        rows = await db.fetch("select timestamp, count from user_count order by timestamp desc limit $1", limit)
    

    items = []

    for row in rows:
        items.append({"timestamp": str(row[0].timestamp()), "count": row[1]})

    return web.json_response({"items": items})
    
