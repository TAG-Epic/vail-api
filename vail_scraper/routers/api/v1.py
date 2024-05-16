import time

from aiohttp import web
from slowstack.asynchronous.times_per import TimesPerRateLimiter

from ...models import AccelByteStatCode
from ...errors import APIErrorCode
from ... import app_keys
from ...utils.rate_limit import rate_limit_http
from ...utils.cors import api_cors

router = web.RouteTableDef()


@router.get("/api/v1/users/search")
@api_cors
async def search_user(request: web.Request) -> web.StreamResponse:
    database = request.app[app_keys.DATABASE]

    if "name" not in request.query:
        return web.json_response(
            {
                "detail": "missing param name",
                "code": APIErrorCode.MISSING_QUERY_PARAMETER,
                "parameter": "query",
            }
        )

    name = request.query["name"]
    escaped_name = name.replace("%", "\\%").replace("_", "\\_")

    result = await database.execute(
        """
        select id, name
        from users where
        name like ? or
        name like ? or
        name like ? or
        name = ?
        limit 50
        """,
        [f"%{escaped_name}%", f"%{escaped_name}", f"{escaped_name}%", name],
    )
    rows = await result.fetchall()

    data = {"items": []}

    for row in rows:
        data["items"].append({"id": row["id"], "name": row["name"]})

    return web.json_response(data)


@router.get("/api/v1/users/{id}")
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


@router.get("/api/v1/users/{user_id}/stats")
@api_cors
@rate_limit_http(lambda: TimesPerRateLimiter(5, 5))
async def get_stats_for_user(request: web.Request) -> web.StreamResponse:
    vail_client = request.app[app_keys.ACCEL_BYTE_CLIENT]
    database = request.app[app_keys.DATABASE]
    database_lock = request.app[app_keys.DATABASE_LOCK]

    user_id = request.match_info["user_id"]

    user_stats = await vail_client.get_user_stats(user_id)
    if user_stats is None:
        return web.json_response(
            {"detail": "user not found", "code": APIErrorCode.USER_NOT_FOUND}
        )

    # Store to DB
    scraped_at = time.time()
    async with database_lock.shared():
        await database.executemany(
            "insert or replace into stats (code, user_id, value, updated_at) values (?, ?, ?, ?)",
            [
                (stat_code, user_id, value, scraped_at)
                for stat_code, value in user_stats.items()
            ],
        )

    # Removed stat codes
    result = await database.execute(
        "select code from stats where user_id = ?", [user_id]
    )
    removed_stat_codes = []
    for row in await result.fetchall():
        stat_code = row[0]
        if stat_code not in user_stats.keys():
            removed_stat_codes.append(stat_code)

    await database.executemany(
        "delete from stats where user_id = ? and code = ?",
        [(user_id, removed_stat_code) for removed_stat_code in removed_stat_codes],
    )
    await database.commit()

    updated_at = time.time()

    return web.json_response(
        {
            "general": {
                "stats": {
                    "matches": {
                        "won": user_stats.get(AccelByteStatCode.GAMES_WON, 0),
                        "lost": user_stats.get(AccelByteStatCode.GAMES_LOST, 0),
                        "draws": user_stats.get(AccelByteStatCode.GAMES_DRAWN, 0),
                        "abandoned": user_stats.get(
                            AccelByteStatCode.GAMES_ABANDONED, 0
                        ),
                    },
                    "kills": user_stats.get(AccelByteStatCode.KILLS, 0),
                    "deaths": user_stats.get(AccelByteStatCode.DEATHS, 0),
                    "game_hours": float(
                        user_stats.get(AccelByteStatCode.GAME_SECONDS, 0)
                    )
                    / 60
                    / 60,
                },
                "meta": {"updated_at": updated_at},
            },
            "cto": {
                "steals": {
                    "count": user_stats.get(AccelByteStatCode.GAMEMODE_CTO_STEALS, 0),
                    "updated_at": updated_at,
                },
                "recovers": {
                    "count": user_stats.get(AccelByteStatCode.GAMEMODE_CTO_RECOVERS, 0),
                    "updated_at": updated_at,
                },
            },
        }
    )
