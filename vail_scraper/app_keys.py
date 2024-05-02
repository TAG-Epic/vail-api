import aiosqlite
from aiohttp.web import AppKey

from vail_scraper.client import VailClient

from .utils.exclusive_lock import ExclusiveLock
from .config import ScraperConfig
from .scraper import VailScraper

CONFIG: AppKey[ScraperConfig] = AppKey("config", ScraperConfig)
DATABASE: AppKey[aiosqlite.Connection] = AppKey("database", aiosqlite.Connection)
SCRAPER: AppKey[VailScraper] = AppKey("scraper", VailScraper)
DATABASE_LOCK: AppKey[ExclusiveLock] = AppKey("database_lock", ExclusiveLock)
VAIL_CLIENT: AppKey[VailClient] = AppKey("vail_client", VailClient)
