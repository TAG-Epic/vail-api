import asyncio
import logging

import aiosqlite
from aiohttp import web

from .asynk import ExclusiveLock
from .config import load_config
from .database.migration_manager import do_migrations
from . import app_keys
from .scraper import VailScraper
from .prometheus import escape_prometheus

logging.basicConfig(level=logging.DEBUG)
_logger = logging.getLogger(__name__)

routes = web.RouteTableDef()
app = web.Application()


@routes.get("/db.sqlite")
async def download_db(request: web.Request) -> web.StreamResponse:
    config = app[app_keys.CONFIG]
    database_lock = app[app_keys.DATABASE_LOCK]
    
    if config.database_url != ":memory:":
        async with database_lock.exclusive():
            response = web.FileResponse(config.database_url)
            await response.prepare(request)
            return response
    
    return web.json_response({"detail": "in-memory db cannot be shared"}, status=500)

@routes.get("/metrics")
async def get_metrics(request: web.Request) -> web.Response:
    del request
    lines = []
    database = app[app_keys.DATABASE]
    scraper = app[app_keys.SCRAPER]

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
        lines.append(f'stats_xp{{id="{escape_prometheus(row[0])}", name="{escape_prometheus(row[1])}"}}')

    result = await database.execute(
        """
        select users.id, name, steals from cto_steal_stats join users on users.id = cto_steal_stats.id
        """
    )
    rows = await result.fetchall()
    for row in rows:
        lines.append(f'stats_cto_steals{{id="{escape_prometheus(row[0])}", name="{escape_prometheus(row[1])}"}} {row[2]}')

    result = await database.execute(
        """
        select users.id, name, recovers from cto_recover_stats join users on users.id = cto_recover_stats.id
        """
    )
    rows = await result.fetchall()
    for row in rows:
        lines.append(f'stats_cto_recovers{{id="{escape_prometheus(row[0])}", name="{escape_prometheus(row[1])}"}} {row[2]}')
    return web.Response(text="\n".join(lines))


app.add_routes(routes)


async def main() -> None:
    config = load_config()
    database_lock = ExclusiveLock()

    database = await aiosqlite.connect(config.database_url)
    _logger.info("doing migrations")
    await do_migrations(database)
    
    app[app_keys.CONFIG] = config
    app[app_keys.DATABASE] = database
    app[app_keys.SCRAPER] = VailScraper(database, database_lock, config)
    app[app_keys.DATABASE_LOCK] = database_lock
    asyncio.create_task(app[app_keys.SCRAPER].run())
    await web._run_app(app, host="0.0.0.0", port=8000)


asyncio.run(main())
