import aiosqlite
from aiohttp.web import AppKey
import asyncpg

from .client.accelbyte import AccelByteClient
from .client.epic_games import EpicGamesClient
from .database.quest import QuestDBWrapper
from .utils.exclusive_lock import ExclusiveLock
from .config import ScraperConfig
from .scraper import VailScraper

CONFIG: AppKey[ScraperConfig] = AppKey("config", ScraperConfig)
DATABASE: AppKey[aiosqlite.Connection] = AppKey("database", aiosqlite.Connection)
SCRAPER: AppKey[VailScraper] = AppKey("scraper", VailScraper)
DATABASE_LOCK: AppKey[ExclusiveLock] = AppKey("database_lock", ExclusiveLock)
ACCEL_BYTE_CLIENT: AppKey[AccelByteClient] = AppKey(
    "accel_byte_client", AccelByteClient
)
EPIC_GAMES_CLIENT: AppKey[EpicGamesClient] = AppKey(
    "epic_games_client", EpicGamesClient
)
QUEST_DB: AppKey[QuestDBWrapper] = AppKey("quest_db", QuestDBWrapper)
QUEST_DB_POSTGRES: AppKey[asyncpg.Pool] = AppKey("quest_db_postgres", asyncpg.Pool)
