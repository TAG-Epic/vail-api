import aiosqlite
from aiohttp.web import AppKey

from .config import Config
from .scraper import VailScraper

CONFIG: AppKey[Config] = AppKey("config", Config)
DATABASE: AppKey[aiosqlite.Connection] = AppKey("database", aiosqlite.Connection)
SCRAPER: AppKey[VailScraper] = AppKey("scraper", VailScraper)
