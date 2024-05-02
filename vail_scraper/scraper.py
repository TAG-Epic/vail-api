import asyncio
from logging import getLogger
import aiosqlite
import time
from aiohttp import ClientSession
from slowstack.asynchronous.times_per import TimesPerRateLimiter

from .models import AccelByteStatCode, AexlabStatCode
from .client import VailClient
from .utils.circuit_breaker import CircuitBreaker
from .utils.exclusive_lock import ExclusiveLock
from .config import ScraperConfig
from .errors import NoContentPageBug

_logger = getLogger(__name__)


class VailScraper:
    def __init__(
        self,
        database: aiosqlite.Connection,
        database_lock: ExclusiveLock,
        vail_client: VailClient,
        config: ScraperConfig,
    ) -> None:
        self._session: ClientSession = ClientSession(
            headers={"User-Agent": config.user_agent, "Bot": "true"},
            base_url="https://aexlab.com",
        )
        self._rate_limiter: TimesPerRateLimiter = TimesPerRateLimiter(
            config.rate_limiter.times, config.rate_limiter.per
        )
        self.circuit_breaker: CircuitBreaker = CircuitBreaker(5, 10)
        self._database: aiosqlite.Connection = database
        self._database_lock: ExclusiveLock = database_lock
        self._vail_client: VailClient = vail_client
        self._config: ScraperConfig = config

    async def run(self) -> None:
        tasks: list[asyncio.Task[None]] = []
        if not self._config.bans.aexlab:
            tasks.append(asyncio.create_task(self._scrape_aexlab_xp_stats()))
            tasks.append(asyncio.create_task(self._scrape_aexlab_cto_steal_stats()))
            tasks.append(asyncio.create_task(self._scrape_aexlab_cto_recover_stats()))
        elif not self._config.bans.accelbyte:
            tasks.append(asyncio.create_task(self._scrape_accelbyte_stats()))

        await asyncio.gather(*tasks)

    async def _scrape_aexlab_xp_stats(self) -> None:
        page_id = 0
        while True:
            try:
                page = await self._vail_client.get_aexlab_leaderboard_page(
                    AexlabStatCode.SCORE, page_id=page_id
                )
            except NoContentPageBug:
                _logger.error("no content page bug on aexlab xp!")
                page_id += 1
                continue
            except:
                _logger.exception("failed to fetch page")
                continue
            _logger.debug("scraped %s users (page %s)", len(page), page_id)

            if len(page) == 0:
                page_id = 0
                _logger.debug("scanned through all pages, starting over again")
                continue

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
                            paged_scraped_at,
                        )
                        for user in page
                    ],
                )
                await self._database.executemany(
                    "insert or replace into xp_stats (id, xp, last_scraped_at) values (?, ?, ?)",
                    [
                        (user.user_id, user.stats.point, paged_scraped_at)
                        for user in page
                    ],
                )
                await self._database.commit()

            page_id += 1

    async def _scrape_aexlab_cto_steal_stats(self) -> None:
        page_id = 0
        while True:
            try:
                page = await self._vail_client.get_aexlab_leaderboard_page(
                    AexlabStatCode.CTO_STEALS, page_id=page_id
                )
            except NoContentPageBug:
                _logger.error("no content page bug on aexlab cto steal!")
                page_id += 1
                continue
            except:
                _logger.exception("failed to fetch page")
                continue
            _logger.debug("scraped %s users (page %s)", len(page), page_id)

            if len(page) == 0:
                page_id = 0
                _logger.debug("scanned through all pages, starting over again")
                continue

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
                            paged_scraped_at,
                        )
                        for user in page
                    ],
                )
                await self._database.executemany(
                    "insert or replace into cto_steal_stats (id, steals, last_scraped_at) values (?, ?, ?)",
                    [
                        (user.user_id, user.stats.point, paged_scraped_at)
                        for user in page
                    ],
                )
                await self._database.commit()

            page_id += 1

    async def _scrape_aexlab_cto_recover_stats(self) -> None:
        page_id = 0
        while True:
            try:
                page = await self._vail_client.get_aexlab_leaderboard_page(
                    AexlabStatCode.CTO_RECOVERS, page_id=page_id
                )
            except NoContentPageBug:
                _logger.error("no content page bug on aexlab cto recover!")
                page_id += 1
                continue
            except:
                _logger.exception("failed to fetch page")
                continue
            _logger.debug("scraped %s users (page %s)", len(page), page_id)

            if len(page) == 0:
                page_id = 0
                _logger.debug("scanned through all pages, starting over again")
                continue

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
                            paged_scraped_at,
                        )
                        for user in page
                    ],
                )
                await self._database.executemany(
                    "insert or replace into cto_recover_stats (id, recovers, last_scraped_at) values (?, ?, ?)",
                    [
                        (user.user_id, user.stats.point, paged_scraped_at)
                        for user in page
                    ],
                )
                await self._database.commit()

            page_id += 1

    async def _scrape_accelbyte_stats(self) -> None:
        page_id = 0
        while True:
            try:
                page = await self._vail_client.get_accelbyte_leaderboard_page(
                    AccelByteStatCode.SCORE, page_id=page_id
                )
            except NoContentPageBug:
                _logger.error("no content page bug on accelbyte!")
                page_id += 1
                continue
            except:
                _logger.exception("failed to fetch page")
                continue
            _logger.debug("scraped %s users (page %s)", len(page), page_id)

            if len(page) == 0:
                page_id = 0
                _logger.debug("scanned through all pages, starting over again")
                continue

            for leaderboard_stat in page:
                user_info = await self._vail_client.get_accelbyte_user_info(
                    leaderboard_stat.user_id
                )
                user_stats = await self._vail_client.get_accelbyte_user_stats(
                    leaderboard_stat.user_id
                )
                scraped_at = time.time()
                async with self._database_lock.shared():
                    await self._database.execute(
                        "insert or replace into users (id, name) values (?, ?)",
                        [leaderboard_stat.user_id, user_info.display_name],
                    )
                    await self._database.execute(
                        "insert or replace into general_stats (id, won, lost, draws, abandoned, kills, assists, deaths, game_hours, last_scraped_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        [
                            leaderboard_stat.user_id,
                            user_stats.get(AccelByteStatCode.GAMES_WON, 0),
                            user_stats.get(AccelByteStatCode.GAMES_LOST, 0),
                            user_stats.get(AccelByteStatCode.GAMES_DRAWN, 0),
                            user_stats.get(AccelByteStatCode.GAMES_ABANDONED, 0),
                            user_stats.get(AccelByteStatCode.KILLS, 0),
                            user_stats.get(AccelByteStatCode.ASSISTS, 0),
                            user_stats.get(AccelByteStatCode.DEATHS, 0),
                            user_stats.get(AccelByteStatCode.GAME_SECONDS, 0),  # TODO: Wrong name?
                            scraped_at,
                        ],
                    )
                    await self._database.execute(
                        "insert or replace into cto_steal_stats (id, steals, last_scraped_at) values (?, ?, ?)",
                        [
                            leaderboard_stat.user_id,
                            user_stats.get(AccelByteStatCode.GAMEMODE_CTO_STEALS, 0),
                            scraped_at,
                        ],
                    )
                    await self._database.execute(
                        "insert or replace into cto_recover_stats (id, recovers, last_scraped_at) values (?, ?, ?)",
                        [
                            leaderboard_stat.user_id,
                            user_stats.get(AccelByteStatCode.GAMEMODE_CTO_RECOVERS, 0),
                            scraped_at,
                        ],
                    )
                    await self._database.execute(
                        "insert or replace into xp_stats (id, xp, last_scraped_at) values (?, ?, ?)",
                        [
                            leaderboard_stat.user_id,
                            user_stats.get(AccelByteStatCode.SCORE, 0),
                            scraped_at,
                        ],
                    )
                    await self._database.commit()

            page_id += 1
