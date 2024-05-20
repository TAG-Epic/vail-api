import asyncio
import logging

import aiosqlite
from aiohttp import web

from .client.accelbyte import AccelByteClient
from .client.epic_games import EpicGamesClient
from .database.quest import QuestDBWrapper
from .utils.exclusive_lock import ExclusiveLock
from .config import load_config
from .database.migration_manager import do_migrations
from . import app_keys
from .scraper import VailScraper
from .routers.raw import router as raw_router
from .routers.api.v1 import router as api_v1_router
from .routers.api.v2 import router as api_v2_router

logging.basicConfig(level=logging.DEBUG)
_logger = logging.getLogger(__name__)

app = web.Application()

# Register routers
app.add_routes(raw_router)
app.add_routes(api_v1_router)
app.add_routes(api_v2_router)


async def main() -> None:
    config = load_config()
    database_lock = ExclusiveLock()

    database = await aiosqlite.connect(config.database.sqlite_url)
    database.row_factory = aiosqlite.Row
    _logger.info("doing migrations")
    await do_migrations(database)

    app[app_keys.CONFIG] = config
    app[app_keys.DATABASE] = database
    app[app_keys.QUEST_DB] = QuestDBWrapper(config.database.quest_url)
    app[app_keys.ACCEL_BYTE_CLIENT] = AccelByteClient(config)
    app[app_keys.EPIC_GAMES_CLIENT] = EpicGamesClient(config)
    app[app_keys.SCRAPER] = VailScraper(
        database,
        database_lock,
        app[app_keys.QUEST_DB],
        app[app_keys.ACCEL_BYTE_CLIENT],
        app[app_keys.EPIC_GAMES_CLIENT],
        config,
    )
    app[app_keys.DATABASE_LOCK] = database_lock

    if config.enabled:
        asyncio.create_task(app[app_keys.SCRAPER].run())

    _logger.info("starting listening")
    await web._run_app(app, host="0.0.0.0", port=8000)


asyncio.run(main())
