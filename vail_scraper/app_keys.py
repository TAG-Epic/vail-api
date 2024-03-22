import aiosqlite
from aiohttp.web import AppKey

from .scraper import VailScraper

DATABASE: AppKey[aiosqlite.Connection] = AppKey("database", aiosqlite.Connection)
SCRAPER: AppKey[VailScraper] = AppKey("scraper", VailScraper)
