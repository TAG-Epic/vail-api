from aiohttp import web
from slowstack.asynchronous.times_per import TimesPerRateLimiter

from ..utils.rate_limit import rate_limit_http
from .. import app_keys

router = web.RouteTableDef()


@router.get("/metrics")
@rate_limit_http(lambda: TimesPerRateLimiter(1, 10))
async def get_metrics(request: web.Request) -> web.Response:
    lines = []
    database = request.app[app_keys.DATABASE]
    scraper = request.app[app_keys.SCRAPER]

    # Scrape info
    result = await database.execute("select count(*) from users")
    row = await result.fetchone()
    assert row is not None
    total_users = row[0]
    lines.append(f"scraper_users_found {total_users}")

    lines.append(f"scraper_users_outdated {scraper.users_failed_scrape}")

    lines.append(f"scraper_last_scrape_duration {scraper.last_scrape_duration}")

    # General stats
    result = await database.execute(
        """
        select
            users.id, name, won, lost, draws, abandoned, kills, assists, deaths, game_hours
            from general_stats
            join users on users.id = general_stats.id
    """
    )
    rows = await result.fetchall()

    for row in rows:
        lines.append(
            f'stats_wins{{id="{escape_prometheus(row[0])}", name="{escape_prometheus(row[1])}"}} {row[2]}'
        )
        lines.append(
            f'stats_losses{{id="{escape_prometheus(row[0])}", name="{escape_prometheus(row[1])}"}} {row[3]}'
        )
        lines.append(
            f'stats_draws{{id="{escape_prometheus(row[0])}", name="{escape_prometheus(row[1])}"}} {row[4]}'
        )
        lines.append(
            f'stats_abandoned{{id="{escape_prometheus(row[0])}", name="{escape_prometheus(row[1])}"}} {row[5]}'
        )
        lines.append(
            f'stats_kills{{id="{escape_prometheus(row[0])}", name="{escape_prometheus(row[1])}"}} {row[6]}'
        )
        lines.append(
            f'stats_assists{{id="{escape_prometheus(row[0])}", name="{escape_prometheus(row[1])}"}} {row[7]}'
        )
        lines.append(
            f'stats_deaths{{id="{escape_prometheus(row[0])}", name="{escape_prometheus(row[1])}"}} {row[8]}'
        )
        lines.append(
            f'stats_game_hours{{id="{escape_prometheus(row[0])}", name="{escape_prometheus(row[1])}"}} {row[9]}'
        )

    result = await database.execute(
        """
        select users.id, name, xp from xp_stats join users on users.id = xp_stats.id
        """
    )
    rows = await result.fetchall()
    for row in rows:
        lines.append(
            f'stats_xp{{id="{escape_prometheus(row[0])}", name="{escape_prometheus(row[1])}"}} {row[2]}'
        )

    result = await database.execute(
        """
        select users.id, name, steals from cto_steal_stats join users on users.id = cto_steal_stats.id
        """
    )
    rows = await result.fetchall()
    for row in rows:
        lines.append(
            f'stats_cto_steals{{id="{escape_prometheus(row[0])}", name="{escape_prometheus(row[1])}"}} {row[2]}'
        )

    result = await database.execute(
        """
        select users.id, name, recovers from cto_recover_stats join users on users.id = cto_recover_stats.id
        """
    )
    rows = await result.fetchall()
    for row in rows:
        lines.append(
            f'stats_cto_recovers{{id="{escape_prometheus(row[0])}", name="{escape_prometheus(row[1])}"}} {row[2]}'
        )
    return web.Response(text="\n".join(lines))


def escape_prometheus(text: str) -> str:
    return text.replace("\\", "\\\\").replace('"', '\\"')
