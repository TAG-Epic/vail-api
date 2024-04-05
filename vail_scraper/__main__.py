import asyncio
import logging

import aiosqlite
from aiohttp import web

from .asynk import ExclusiveLock
from .config import load_config
from .database.migration_manager import do_migrations
from . import app_keys
from .scraper import VailScraper

from .routers.raw import router as raw_router
from .routers.prometheus import router as prometheus_router
from .routers.api import router as api_router

logging.basicConfig(level=logging.DEBUG)
_logger = logging.getLogger(__name__)

app = web.Application()

# Register routers
app.add_routes(raw_router)
app.add_routes(prometheus_router)
app.add_routes(api_router)

async def main() -> None:
    config = load_config()
    database_lock = ExclusiveLock()

    database = await aiosqlite.connect(config.database_url)
    database.row_factory = aiosqlite.Row
    _logger.info("doing migrations")
    await do_migrations(database)
    
    app[app_keys.CONFIG] = config
    app[app_keys.DATABASE] = database
    app[app_keys.SCRAPER] = VailScraper(database, database_lock, config)
    app[app_keys.DATABASE_LOCK] = database_lock
    asyncio.create_task(app[app_keys.SCRAPER].run())
    await web._run_app(app, host="0.0.0.0", port=8000)


asyncio.run(main())
