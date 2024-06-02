import time

from aiohttp import web
from slowstack.asynchronous.times_per import TimesPerRateLimiter

from ....models.accelbyte import AccelByteStatCode
from ....errors import APIErrorCode
from .... import app_keys
from ....utils.rate_limit import rate_limit_http
from ....utils.cors import api_cors


router = web.RouteTableDef()


@router.get("/api/v1/users/{user_id}/stats")
@api_cors
@rate_limit_http(lambda: TimesPerRateLimiter(5, 5))
async def get_stats_for_user(request: web.Request) -> web.StreamResponse:
    vail_client = request.app[app_keys.ACCEL_BYTE_CLIENT]

    user_id = request.match_info["user_id"]

    user_stats = await vail_client.get_user_stats(user_id)
    if user_stats is None:
        return web.json_response(
            {"detail": "user not found", "code": APIErrorCode.USER_NOT_FOUND}
        )

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
