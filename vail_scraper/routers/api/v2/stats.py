from logging import getLogger
import time
from typing import Any
from datetime import datetime
import typing
import asyncio

from aiohttp import web
from slowstack.asynchronous.times_per import TimesPerRateLimiter

from ....models.accelbyte import AccelByteStatCode
from ....errors import APIErrorCode
from .... import app_keys
from ....enums import RequestPriority
from ....utils.rate_limit import rate_limit_http
from ....utils.cors import api_cors

router = web.RouteTableDef()
_logger = getLogger(__name__)

def format_user_stats(user_stats: dict[str, float]):
    weapons: dict[str, Any] = {
        "kanto": {
            "kills": {
                "total": int(user_stats.get(AccelByteStatCode.WEAPON_KANTO_KILLS, 0)),
                "headshot_kills": int(
                    user_stats.get(AccelByteStatCode.WEAPON_KANTO_HEADSHOT_KILLS, 0)
                ),
            }
        }
    }
    generic_weapon_types: set[str] = set()

    for stat_code in AccelByteStatCode:
        stat_code = str(stat_code)
        if not stat_code.startswith("weapon-"):
            continue
        weapon_type = stat_code.split("-")[1]
        generic_weapon_types.add(weapon_type)
    _logger.debug("weapon types: %s", generic_weapon_types)
    generic_weapon_types.remove("kanto")  # Special

    for weapon_type in generic_weapon_types:
        prefix = f"weapon-{weapon_type}"
        weapons[weapon_type] = {
            "kills": {
                "total": int(user_stats.get(f"{prefix}-kills", 0)),
                "headshot_kills": int(user_stats.get(f"{prefix}-headshot-kills", 0)),
            },
            "shots": {
                "fired": int(user_stats.get(f"{prefix}-shots-fired", 0)),
                "hits": {
                    "leg": int(user_stats.get(f"{prefix}-shots-hit-leg", 0)),
                    "arm": int(user_stats.get(f"{prefix}-shots-hit-arm", 0)),
                    "body": int(user_stats.get(f"{prefix}-shots-hit-body", 0)),
                    "head": int(user_stats.get(f"{prefix}-shots-hit-head", 0)),
                },
            },
        }

    # Generate map stats
    map_types: set[str] = set()
    for stat_code in AccelByteStatCode:
        stat_code = str(stat_code)
        if not stat_code.startswith("map-"):
            continue
        map_name = stat_code.split("-")[1]
        map_types.add(map_name)
    _logger.debug("map types: %s", map_types)

    maps: dict[str, Any] = {}
    for map_type in map_types:
        prefix = f"map-{map_type}"
        maps[map_type] = {
            "match_results": {
                "wins": int(user_stats.get(f"{prefix}-games-won", 0)),
                "losses": int(user_stats.get(f"{prefix}-games-lost", 0)),
                "draws": int(user_stats.get(f"{prefix}-games-drawn", 0)),
                "abandons": int(user_stats.get(f"{prefix}-games-abandoned", 0)),
            }
        }

    return {
        "maps": maps,
        "weapons": weapons,
        "gamemodes": {
            "artifact": {
                "time_played_seconds": int(
                    user_stats.get(AccelByteStatCode.GAMEMODE_ART_GAME_SECONDS, 0)
                ),
                "kills_and_deaths": {
                    "kills": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_ART_KILLS, 0)
                    ),
                    "aces": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_ART_ACES, 0)
                    ),
                    "assists": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_ART_ASSISTS, 0)
                    ),
                    "deaths": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_ART_DEATHS, 0)
                    ),
                },
                "scanner": {
                    "planted": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_ART_PLANTS, 0)
                    ),
                    "disabled": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_ART_DISABLES, 0)
                    ),
                },
                "match_results": {
                    "wins": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_ART_GAMES_WON, 0)
                    ),
                    "losses": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_ART_GAMES_LOST, 0)
                    ),
                    "abandons": int(
                        user_stats.get(
                            AccelByteStatCode.GAMEMODE_ART_GAMES_ABANDONED, 0
                        )
                    ),
                },
                "round_results": {
                    "pistol_round": {
                        "wins": int(
                            user_stats.get(
                                AccelByteStatCode.GAMEMODE_ART_PISTOL_ROUND_WINS, 0
                            )
                        ),
                        "losses": int(
                            user_stats.get(
                                AccelByteStatCode.GAMEMODE_ART_PISTOL_ROUND_LOSSES,
                                0,
                            )
                        ),
                    },
                    "reyab": {
                        "wins": int(
                            user_stats.get(
                                AccelByteStatCode.GAMEMODE_ART_REYAB_ROUND_WINS, 0
                            )
                        ),
                        "losses": int(
                            user_stats.get(
                                AccelByteStatCode.GAMEMODE_ART_REYAB_ROUND_LOSSES, 0
                            )
                        ),
                    },
                },
            },
            "capture_the_orb": {
                "time_played_seconds": int(
                    user_stats.get(AccelByteStatCode.GAMEMODE_CTO_GAME_SECONDS, 0)
                ),
                "kills_and_deaths": {
                    "kills": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_CTO_KILLS, 0)
                    ),
                    "kills_on_orb_carrier": int(
                        user_stats.get(
                            AccelByteStatCode.GAMEMODE_CTO_CARRIER_KILLS, 0
                        )
                    ),
                    "kills_as_orb_carrier": int(
                        user_stats.get(
                            AccelByteStatCode.GAMEMODE_CTO_AS_CARRIER_KILLS, 0
                        )
                    ),
                    "assists": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_CTO_ASSISTS, 0)
                    ),
                    "deaths": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_CTO_DEATHS, 0)
                    ),
                },
                "orb": {
                    "steals": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_CTO_STEALS, 0)
                    ),
                    "recovers": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_CTO_RECOVERS, 0)
                    ),
                    "captures": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_CTO_CAPTURES, 0)
                    ),
                },
                "match_results": {
                    "wins": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_CTO_GAMES_WON, 0)
                    ),
                    "losses": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_CTO_GAMES_LOST, 0)
                    ),
                    "draws": int(
                        user_stats.get(
                            AccelByteStatCode.GAMEMODE_CTO_GAMES_DRAWN, 0
                        )
                    ),
                    "abandons": int(
                        user_stats.get(
                            AccelByteStatCode.GAMEMODE_CTO_GAMES_ABANDONED, 0
                        )
                    ),
                },
            },
            "team_deathmatch": {
                "time_played_seconds": int(
                    user_stats.get(AccelByteStatCode.GAMEMODE_TDM_GAME_SECONDS, 0)
                ),
                "kills_and_deaths": {
                    "kills": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_TDM_KILLS, 0)
                    ),
                    "assists": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_TDM_ASSISTS, 0)
                    ),
                    "deaths": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_TDM_DEATHS, 0)
                    ),
                },
                "match_results": {
                    "wins": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_TDM_GAMES_WON, 0)
                    ),
                    "losses": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_TDM_GAMES_LOST, 0)
                    ),
                    "draws": int(
                        user_stats.get(
                            AccelByteStatCode.GAMEMODE_TDM_GAMES_DRAWN, 0
                        )
                    ),
                    "abandons": int(
                        user_stats.get(
                            AccelByteStatCode.GAMEMODE_TDM_GAMES_ABANDONED, 0
                        )
                    ),
                },
            },
            "scoutzknivez": {
                "time_played_seconds": int(
                    user_stats.get(AccelByteStatCode.GAMEMODE_SKZ_GAME_SECONDS, 0)
                ),
                "kills_and_deaths": {
                    "kills": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_SKZ_KILLS, 0)
                    ),
                    "assists": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_SKZ_ASSISTS, 0)
                    ),
                    "deaths": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_SKZ_DEATHS, 0)
                    ),
                },
                "match_results": {
                    "wins": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_SKZ_GAMES_WON, 0)
                    ),
                    "losses": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_SKZ_GAMES_LOST, 0)
                    ),
                    "draws": int(
                        user_stats.get(
                            AccelByteStatCode.GAMEMODE_SKZ_GAMES_DRAWN, 0
                        )
                    ),
                    "abandons": int(
                        user_stats.get(
                            AccelByteStatCode.GAMEMODE_SKZ_GAMES_ABANDONED, 0
                        )
                    ),
                },
            },
            "hardpoint": {
                "time_played_seconds": int(
                    user_stats.get(AccelByteStatCode.GAMEMODE_HP_GAME_SECONDS, 0)
                ),
                "kills_and_deaths": {
                    "kills": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_HP_KILLS, 0)
                    ),
                    "offensive_kills": int(
                        user_stats.get(
                            AccelByteStatCode.GAMEMODE_HP_OFFENSIVE_KILLS, 0
                        )
                    ),
                    "defensive_kills": int(
                        user_stats.get(
                            AccelByteStatCode.GAMEMODE_HP_DEFENSIVE_KILLS, 0
                        )
                    ),
                    "assists": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_HP_ASSISTS, 0)
                    ),
                    "deaths": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_HP_DEATHS, 0)
                    ),
                },
                "point": {
                    "first_captures": int(
                        user_stats.get(
                            AccelByteStatCode.GAMEMODE_HP_INITIAL_CAPTURES, 0
                        )
                    )
                },
                "match_results": {
                    "wins": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_HP_GAMES_WON, 0)
                    ),
                    "losses": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_HP_GAMES_LOST, 0)
                    ),
                    "draws": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_HP_GAMES_DRAWN, 0)
                    ),
                    "abandons": int(
                        user_stats.get(
                            AccelByteStatCode.GAMEMODE_HP_GAMES_ABANDONED, 0
                        )
                    ),
                },
            },
            "free_for_all": {
                "time_played_seconds": int(
                    user_stats.get(AccelByteStatCode.GAMEMODE_FFA_GAME_SECONDS, 0)
                ),
                "kills_and_deaths": {
                    "kills": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_FFA_KILLS, 0)
                    ),
                    "assists": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_FFA_ASSISTS, 0)
                    ),
                    "deaths": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_FFA_DEATHS, 0)
                    ),
                },
                "match_results": {
                    "wins": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_FFA_GAMES_WON, 0)
                    ),
                    "losses": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_FFA_GAMES_LOST, 0)
                    ),
                    "abandons": int(
                        user_stats.get(
                            AccelByteStatCode.GAMEMODE_FFA_GAMES_ABANDONED, 0
                        )
                    ),
                },
            },
            "gun_game": {
                "time_played_seconds": int(
                    user_stats.get(AccelByteStatCode.GAMEMODE_GG_GAME_SECONDS, 0)
                ),
                "kills_and_deaths": {
                    "kills": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_GG_KILLS, 0)
                    ),
                    "assists": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_GG_ASSISTS, 0)
                    ),
                    "deaths": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_GG_DEATHS, 0)
                    ),
                },
                "match_results": {
                    "wins": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_GG_GAMES_WON, 0)
                    ),
                    "losses": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_GG_GAMES_LOST, 0)
                    ),
                    "abandons": int(
                        user_stats.get(
                            AccelByteStatCode.GAMEMODE_GG_GAMES_ABANDONED, 0
                        )
                    ),
                },
            },
            "one_in_the_chamber": {
                "time_played_seconds": int(
                    user_stats.get(AccelByteStatCode.GAMEMODE_OTC_GAME_SECONDS, 0)
                ),
                "kills_and_deaths": {
                    "kills": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_OTC_KILLS, 0)
                    ),
                    "deaths": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_OTC_DEATHS, 0)
                    ),
                },
                "match_results": {
                    "wins": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_OTC_GAMES_WON, 0)
                    ),
                    "losses": int(
                        user_stats.get(AccelByteStatCode.GAMEMODE_OTC_GAMES_LOST, 0)
                    ),
                    "abandons": int(
                        user_stats.get(
                            AccelByteStatCode.GAMEMODE_OTC_GAMES_ABANDONED, 0
                        )
                    ),
                },
            },
        },
        "general": {
            "time_played_seconds": int(
                user_stats.get(AccelByteStatCode.GAME_SECONDS, 0)
            ),
            "kills_and_deaths": {
                "kills": int(user_stats.get(AccelByteStatCode.KILLS, 0)),
                "assists": int(user_stats.get(AccelByteStatCode.ASSISTS, 0)),
                "deaths": int(user_stats.get(AccelByteStatCode.DEATHS, 0)),

                "bursts": {
                    "2": int(user_stats.get(AccelByteStatCode.KILLSTREAKS_DOUBLE, 0)),
                    "3": int(user_stats.get(AccelByteStatCode.KILLSTREAKS_TRIPLE, 0)),
                    "5": int(user_stats.get(AccelByteStatCode.KILLSTREAKS_SPREE, 0))
                }
            },
            "match_results": {
                "wins": int(user_stats.get(AccelByteStatCode.GAMES_WON, 0)),
                "losses": int(user_stats.get(AccelByteStatCode.GAMES_LOST, 0)),
                "draws": int(user_stats.get(AccelByteStatCode.GAMES_DRAWN, 0)),
                "abandons": int(
                    user_stats.get(AccelByteStatCode.GAMES_ABANDONED, 0)
                ),
            },
            "prestige": int(user_stats.get(AccelByteStatCode.PRESTIGE, 0)),
            "xp": {
                "total": {
                    "value": int(user_stats.get(AccelByteStatCode.SCORE, 0))
                },
                "current_prestige": {
                    "value": int(user_stats.get(AccelByteStatCode.XP, 0)) 
                }
            }
        },
    }


@router.get("/api/v2/users/{user_id}/stats")
@api_cors
@rate_limit_http(lambda: TimesPerRateLimiter(5, 5))
async def get_stats_for_user(request: web.Request) -> web.StreamResponse:
    vail_client = request.app[app_keys.ACCEL_BYTE_CLIENT]

    user_id = request.match_info["user_id"]

    user_stats = await vail_client.get_user_stats(
        user_id, priority=RequestPriority.HIGH
    )
    if user_stats is None:
        return web.json_response(
            {"detail": "user not found", "code": APIErrorCode.USER_NOT_FOUND},
            status=410,
        )

    # Generate stats
    return web.json_response(format_user_stats(user_stats))

@router.get("/api/v2/users/{user_id}/stats/timeseries")
@api_cors
async def get_timeseries_stats_for_user(request: web.Request) -> web.StreamResponse:
    database = request.app[app_keys.DATABASE]
    quest_db = request.app[app_keys.QUEST_DB_POSTGRES]


    user_id = request.match_info["user_id"]

    # Check if user exists
    result = await database.execute("select count(*) from users where id = ?", [user_id])
    row = await result.fetchone()
    assert row is not None
    if row[0] == 0:
        return web.json_response({"code": APIErrorCode.USER_NOT_FOUND, "detail": "user not found/not scraped yet."})


    raw_before_timestamp = request.query.getone("before", None)
    raw_after_timestamp = request.query.getone("after", None)

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

        rows = await quest_db.fetch("select timestamp from user_stats where user_id = $1 and code = $2 and timestamp < $3 order by timestamp desc limit $4", user_id, "game-seconds", before_timestamp, limit)
    elif raw_after_timestamp is not None:
        try:
            after_timestamp = datetime.fromtimestamp(float(raw_after_timestamp))
        except ValueError as error:
            return web.json_response({"code": APIErrorCode.QUERY_PARAMETER_INVALID, "detail": f"failed to parse the after parameter: {error}", "field": "before"}, status=400)

        rows = await quest_db.fetch("select timestamp from user_stats where user_id = $1 and code = $2 and timestamp > $3 order by timestamp asc limit $4", user_id, "game-seconds", after_timestamp, limit)
        rows = reversed(rows)
    else:
        rows = await quest_db.fetch("select timestamp from user_stats where user_id = $1 and code=$2 order by timestamp desc limit $3", user_id, "game-seconds", limit)

    timestamps = set([row[0] for row in rows]) # just in case

    items = await asyncio.gather(*[get_stat_snapshot(request, user_id, timestamp) for timestamp in timestamps])

    return web.json_response({"items": items})

async def get_stat_snapshot(request: web.Request, user_id: str, timestamp: datetime):
    quest_db = request.app[app_keys.QUEST_DB_POSTGRES]

    stats = {}
    rows = await quest_db.fetch("select code, value from user_stats where user_id = $1 and timestamp = $2", user_id, timestamp)

    for row in rows:
        stats[row[0]] = row[1]
    
    
    formatted = format_user_stats(stats)
    formatted["timestamp"] = str(timestamp.timestamp())
    return formatted
