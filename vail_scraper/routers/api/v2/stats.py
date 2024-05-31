from logging import getLogger
import time
from typing import Any
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


@router.get("/api/v2/users/{user_id}/stats")
@api_cors
@rate_limit_http(lambda: TimesPerRateLimiter(5, 5))
async def get_stats_for_user_v2(request: web.Request) -> web.StreamResponse:
    vail_client = request.app[app_keys.ACCEL_BYTE_CLIENT]
    database = request.app[app_keys.DATABASE]
    database_lock = request.app[app_keys.DATABASE_LOCK]

    user_id = request.match_info["user_id"]

    user_stats = await vail_client.get_user_stats(
        user_id, priority=RequestPriority.HIGH
    )
    if user_stats is None:
        return web.json_response(
            {"detail": "user not found", "code": APIErrorCode.USER_NOT_FOUND},
            status=410,
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

    # Generate weapon stats
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

    return web.json_response(
        {
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
                "prestige": int(user_stats.get(AccelByteStatCode.PRESTIGE, 0))
            },
        }
    )
