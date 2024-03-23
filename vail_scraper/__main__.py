import asyncio
import logging
from urllib.parse import quote

import aiosqlite
from aiohttp import web

from .config import load_config
from .database.migration_manager import do_migrations
from . import app_keys
from .scraper import VailScraper

logging.basicConfig(level=logging.DEBUG)
_logger = logging.getLogger(__name__)

routes = web.RouteTableDef()
app = web.Application()


@routes.get("/metrics")
async def get_metrics(request: web.Request) -> web.Response:
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

    # Wins
    result = await database.execute(
        """
        select
            users.id, name, won, lost, draws, abandoned, kills, assists, deaths, points, game_hours
            from stats
            join users on users.id = stats.id
    """
    )
    rows = await result.fetchall()

    for row in rows:
        lines.append(
            f'stats_wins{{id="{quote(row[0])}", name="{quote(row[1])}"}} {row[2]}'
        )
        lines.append(
            f'stats_losses{{id="{quote(row[0])}", name="{quote(row[1])}"}} {row[3]}'
        )
        lines.append(
            f'stats_draws{{id="{quote(row[0])}", name="{quote(row[1])}"}} {row[4]}'
        )
        lines.append(
            f'stats_abandoned{{id="{quote(row[0])}", name="{quote(row[1])}"}} {row[5]}'
        )
        lines.append(
            f'stats_kills{{id="{quote(row[0])}", name="{quote(row[1])}"}} {row[6]}'
        )
        lines.append(
            f'stats_assists{{id="{quote(row[0])}", name="{quote(row[1])}"}} {row[7]}'
        )
        lines.append(
            f'stats_deaths{{id="{quote(row[0])}", name="{quote(row[1])}"}} {row[8]}'
        )
        lines.append(
            f'stats_points{{id="{quote(row[0])}", name="{quote(row[1])}"}} {row[9]}'
        )
        lines.append(
            f'stats_game_hours{{id="{quote(row[0])}", name="{quote(row[1])}"}} {row[10]}'
        )
    return web.Response(text="\n".join(lines))


app.add_routes(routes)


async def main() -> None:
    config = load_config()

    database = await aiosqlite.connect(config.database_url)
    _logger.info("doing migrations")
    await do_migrations(database)

    app[app_keys.DATABASE] = database
    app[app_keys.SCRAPER] = VailScraper(database, config)
    asyncio.create_task(app[app_keys.SCRAPER].run())
    await web._run_app(app, host="0.0.0.0", port=8000)


asyncio.run(main())
