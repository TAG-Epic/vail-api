from aiohttp import web

from .. import app_keys

router = web.RouteTableDef()

@router.get("/api/v1/user/{id}/stats")
async def get_stats_for_user(request: web.Request) -> web.StreamResponse:
    database = request.app[app_keys.DATABASE]

    result = await database.execute(
        """
        select
        users.id, users.name,
        general_stats.won, general_stats.lost, general_stats.draws, general_stats.draws, general_stats.abandoned, general_stats.kills, general_stats.deaths, general_stats.game_hours, general_stats.last_scraped_at as general_stats_last_scraped_at,
        cto_recover_stats.recovers as cto_recovers, cto_recover_stats.last_scraped_at as cto_recover_stats_last_scraped_at,
        cto_steal_stats.steals as cto_steals, cto_steal_stats.last_scraped_at as cto_steal_stats_last_scraped_at
        from users
        left join general_stats on users.id = general_stats.id
        left join cto_recover_stats on users.id = cto_recover_stats.id
        left join cto_steal_stats on users.id = cto_steal_stats.id
        left join xp_stats on users.id = xp_stats.id
        limit 1
        """
    )
    row = await result.fetchone()
    if row is None:
        return web.json_response({"detail": "user not found/not scraped yet"}, status=404)

    data = {}

    if row["general_stats_last_scraped_at"] is not None:
        data["general"] = {
            "stats": {
                "matches": {
                    "won": row["won"],
                    "lost": row["lost"],
                    "draws": row["draws"],
                    "abandoned": row["abandoned"]
                },
                "kills": row["kills"],
                "deaths": row["deaths"],
                "game_hours": row["game_hours"],
            },
            "meta": {
                "updated_at": row["general_stats_last_scraped_at"]
            }
        }
    else:
        data["general"] = None

    data["cto"] = {}

    if row["cto_recover_stats_last_scraped_at"] is not None:
        data["cto"]["recovers"] = {
            "count": row["cto_recovers"],
            "meta": {
                "updated_at": row["cto_recover_stats_last_scraped_at"]
            }
        }
    else:
        data["cto"]["recovers"] = None

    if row["cto_steal_stats_last_scraped_at"] is not None:
        data["cto"]["steals"] = {
            "count": row["cto_steals"],
            "meta": {
                "updated_at": row["cto_steal_stats_last_scraped_at"]
            }
        }
    else:
        data["cto"]["steals"] = None

    return web.json_response(data)
    
