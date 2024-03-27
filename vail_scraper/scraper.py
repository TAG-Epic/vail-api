from logging import getLogger
import typing
import aiosqlite
import time
from aiohttp import ClientSession
from nextcore.common.times_per import TimesPer

from vail_scraper.asynk import ExclusiveLock

from .config import Config
from .errors import NoContentPageBug
from .models import ScoreLeaderboardPage, ScoreLeaderboardPlayer

_logger = getLogger(__name__)


class VailScraper:
    def __init__(self, database: aiosqlite.Connection, database_lock: ExclusiveLock, config: Config) -> None:
        self._session: ClientSession = ClientSession(
            headers={"User-Agent": config.user_agent, "Bot": "true"},
            base_url="https://aexlab.com",
        )
        self._rate_limiter: TimesPer = TimesPer(
            config.rate_limiter.times, config.rate_limiter.per
        )
        self._database: aiosqlite.Connection = database
        self._database_lock: ExclusiveLock = database_lock
        self.last_scrape_at: float = time.time()
        self.users_failed_scrape: int = 0
        self.last_scrape_duration: float = 0

    async def run(self) -> None:
        while True:
            started_scraping_at = time.time()
            await self._scrape_xp_stats()
            await self._scrape_cto_steals()
            await self._scrape_cto_recovers()
            
            # Scraping stats
            ended_scraping_at = time.time()
            self.last_scrape_duration = ended_scraping_at - started_scraping_at

            # Check how many users are outdated
            outdated_user_ids: set[str] = set()


            # General stats
            result = await self._database.execute("select id from general_stats where last_scraped_at < ?", [started_scraping_at])
            rows = await result.fetchall()
            outdated_user_ids.update([row[0] for row in rows])
            
            # XP Stats
            result = await self._database.execute("select id from xp_stats where last_scraped_at < ?", [started_scraping_at])
            rows = await result.fetchall()
            outdated_user_ids.update([row[0] for row in rows])
            
            # CTO steals
            result = await self._database.execute("select id from cto_steal_stats where last_scraped_at < ?", [started_scraping_at])
            rows = await result.fetchall()
            outdated_user_ids.update([row[0] for row in rows])

            # CTO recovers
            result = await self._database.execute("select id from cto_recover_stats where last_scraped_at < ?", [started_scraping_at])
            rows = await result.fetchall()
            outdated_user_ids.update([row[0] for row in rows])
            
            self.users_failed_scrape = len(outdated_user_ids)




    async def _scrape_xp_stats(self) -> None:
        page_id = 0
        while True:
            try:
                page = await self.get_leaderboard_page(page_id)
            except NoContentPageBug:
                _logger.error("no content page bug!")
                page_id += 1
                continue
            except:
                _logger.exception("failed to fetch page")
                continue
            _logger.debug("scraped %s users (page %s)", len(page), page_id)

            if len(page) == 0:
                break

            paged_scraped_at = time.time()
            async with self._database_lock.shared():
                await self._database.executemany(
                    "insert or replace into users (id, name) values (?, ?)",
                    [(user.user_id, user.display_name) for user in page],
                )
                await self._database.executemany(
                    "insert or replace into general_stats (id, won, lost, draws, abandoned, kills, assists, deaths, game_hours, last_scraped_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (
                            user.user_id,
                            user.stats.won,
                            user.stats.lost,
                            user.stats.draws,
                            user.stats.abandoned,
                            user.stats.kills,
                            user.stats.assists,
                            user.stats.deaths,
                            user.stats.game_hours,
                            paged_scraped_at
                        )
                        for user in page
                    ],
                )
                await self._database.executemany(
                    "insert or replace into xp_stats (id, xp, last_scraped_at) values (?, ?, ?)",
                    [
                        (
                            user.user_id,
                            user.stats.point,
                            paged_scraped_at
                        )
                        for user in page
                    ]
                )
                await self._database.commit()

            page_id += 1

    async def _scrape_cto_steals(self) -> None:
        page_id = 0
        while True:
            try:
                page = await self.get_leaderboard_page(page_id, point="cto-steals")
            except NoContentPageBug:
                _logger.error("no content page bug!")
                page_id += 1
                continue
            except:
                _logger.exception("failed to fetch page")
                continue
            _logger.debug("scraped %s users (page %s)", len(page), page_id)

            if len(page) == 0:
                break

            paged_scraped_at = time.time()
            async with self._database_lock.shared():
                await self._database.executemany(
                    "insert or replace into users (id, name) values (?, ?)",
                    [(user.user_id, user.display_name) for user in page],
                )
                await self._database.executemany(
                    "insert or replace into general_stats (id, won, lost, draws, abandoned, kills, assists, deaths, game_hours, last_scraped_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (
                            user.user_id,
                            user.stats.won,
                            user.stats.lost,
                            user.stats.draws,
                            user.stats.abandoned,
                            user.stats.kills,
                            user.stats.assists,
                            user.stats.deaths,
                            user.stats.game_hours,
                            paged_scraped_at
                        )
                        for user in page
                    ],
                )
                await self._database.executemany(
                    "insert or replace into cto_steal_stats (id, steals, last_scraped_at) values (?, ?, ?)",
                    [
                        (
                            user.user_id,
                            user.stats.point,
                            paged_scraped_at
                        )
                        for user in page
                    ]
                )
                await self._database.commit()

            page_id += 1

    async def _scrape_cto_recovers(self) -> None:
        page_id = 0
        while True:
            try:
                page = await self.get_leaderboard_page(page_id, point="cto-recovers")
            except NoContentPageBug:
                _logger.error("no content page bug!")
                page_id += 1
                continue
            except:
                _logger.exception("failed to fetch page")
                continue
            _logger.debug("scraped %s users (page %s)", len(page), page_id)

            if len(page) == 0:
                break

            paged_scraped_at = time.time()
            async with self._database_lock.shared():
                await self._database.executemany(
                    "insert or replace into users (id, name) values (?, ?)",
                    [(user.user_id, user.display_name) for user in page],
                )
                await self._database.executemany(
                    "insert or replace into general_stats (id, won, lost, draws, abandoned, kills, assists, deaths, game_hours, last_scraped_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (
                            user.user_id,
                            user.stats.won,
                            user.stats.lost,
                            user.stats.draws,
                            user.stats.abandoned,
                            user.stats.kills,
                            user.stats.assists,
                            user.stats.deaths,
                            user.stats.game_hours,
                            paged_scraped_at
                        )
                        for user in page
                    ],
                )
                await self._database.executemany(
                    "insert or replace into cto_recover_stats (id, recovers, last_scraped_at) values (?, ?, ?)",
                    [
                        (
                            user.user_id,
                            user.stats.point,
                            paged_scraped_at
                        )
                        for user in page
                    ]
                )
                await self._database.commit()

            page_id += 1




    async def get_leaderboard_page(
            self, page_id: int, *, page_size: int = 100, point: typing.Literal["score", "wins", "kills", "cto-steals", "game-seconds", "cto-recovers"] = "score"
    ) -> list[ScoreLeaderboardPlayer]:
        error: Exception | None = None
        for retry_attempt in range(5):
            async with self._rate_limiter.acquire():
                _logger.debug("fetching page %s", page_id)
                try:
                    response = await self._session.get(
                        "/api/leaderboard",
                        params={
                            "leaderboardCode": point,
                            "page": page_id + 1,
                            "pageLimit": page_size,
                        },
                    )
                except Exception as error:
                    _logger.exception("failed to fetch page")
                    error = error
                    continue
            if response.status == 204:
                _logger.warn("204 page!")
                raise NoContentPageBug()
            if not response.ok:
                _logger.error("failed to parse result: %s", await response.text())
                response.raise_for_status()
            data = await response.text()
            page = ScoreLeaderboardPage.validate_json(data)
            return page
        raise typing.cast(Exception, error)
