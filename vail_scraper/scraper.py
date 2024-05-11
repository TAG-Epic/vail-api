import asyncio
from logging import getLogger
import aiosqlite
import time
from aiohttp import ClientSession
from slowstack.asynchronous.times_per import TimesPerRateLimiter

from .models import AccelBytePlayerInfo, AccelByteStatCode, AexlabStatCode
from .client import VailClient
from .utils.circuit_breaker import CircuitBreaker
from .utils.exclusive_lock import ExclusiveLock
from .config import ScraperConfig
from .errors import ExternalServiceError, NoContentPageBug

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

            page_scraped_at = time.time()
            async with self._database_lock.shared():
                await self._database.executemany(
                    "insert or replace into users (id, name) values (?, ?)",
                    [(user.user_id, user.display_name) for user in page],
                )
                rows: list[tuple[str, str, int, float]] = []
                for user in page:
                    stats: dict[AccelByteStatCode, int] = {
                        AccelByteStatCode.GAMES_WON: user.stats.won,
                        AccelByteStatCode.GAMES_LOST: user.stats.lost,
                        AccelByteStatCode.GAMES_DRAWN: user.stats.draws,
                        AccelByteStatCode.GAMES_ABANDONED: user.stats.abandoned,
                        AccelByteStatCode.KILLS: user.stats.kills,
                        AccelByteStatCode.ASSISTS: user.stats.assists,
                        AccelByteStatCode.DEATHS: user.stats.deaths,
                        AccelByteStatCode.GAMEMODE_CTO_STEALS: user.point
                    }

                    for stat_code, value in stats.items():
                        rows.append((user.user_id, stat_code, value, page_scraped_at))
                await self._database.executemany("insert or replace into stats (user_id, code, value, updated_at) values (?, ?, ?, ?)", rows)
                await self._database.commit()

            page_id += 1

    async def _scrape_aexlab_cto_steal_stats(self) -> None:
        page_id = 0
        while True:
            try:
                page = await self._vail_client.get_aexlab_leaderboard_page(
                    AexlabStatCode.CTO_RECOVERS, page_id=page_id
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

            page_scraped_at = time.time()
            async with self._database_lock.shared():
                await self._database.executemany(
                    "insert or replace into users (id, name) values (?, ?)",
                    [(user.user_id, user.display_name) for user in page],
                )
                rows: list[tuple[str, str, int, float]] = []
                for user in page:
                    stats: dict[AccelByteStatCode, int] = {
                        AccelByteStatCode.GAMES_WON: user.stats.won,
                        AccelByteStatCode.GAMES_LOST: user.stats.lost,
                        AccelByteStatCode.GAMES_DRAWN: user.stats.draws,
                        AccelByteStatCode.GAMES_ABANDONED: user.stats.abandoned,
                        AccelByteStatCode.KILLS: user.stats.kills,
                        AccelByteStatCode.ASSISTS: user.stats.assists,
                        AccelByteStatCode.DEATHS: user.stats.deaths,
                        AccelByteStatCode.GAMEMODE_CTO_STEALS: user.point
                    }

                    for stat_code, value in stats.items():
                        rows.append((user.user_id, stat_code, value, page_scraped_at))
                await self._database.executemany("insert or replace into stats (user_id, code, value, updated_at) values (?, ?, ?, ?)", rows)
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

            page_scraped_at = time.time()
            async with self._database_lock.shared():
                await self._database.executemany(
                    "insert or replace into users (id, name) values (?, ?)",
                    [(user.user_id, user.display_name) for user in page],
                )
                rows: list[tuple[str, str, int, float]] = []
                for user in page:
                    stats: dict[AccelByteStatCode, int] = {
                        AccelByteStatCode.GAMES_WON: user.stats.won,
                        AccelByteStatCode.GAMES_LOST: user.stats.lost,
                        AccelByteStatCode.GAMES_DRAWN: user.stats.draws,
                        AccelByteStatCode.GAMES_ABANDONED: user.stats.abandoned,
                        AccelByteStatCode.KILLS: user.stats.kills,
                        AccelByteStatCode.ASSISTS: user.stats.assists,
                        AccelByteStatCode.DEATHS: user.stats.deaths,
                        AccelByteStatCode.GAMEMODE_CTO_RECOVERS: user.point
                    }

                    for stat_code, value in stats.items():
                        rows.append((user.user_id, stat_code, value, page_scraped_at))
                await self._database.executemany("insert or replace into stats (user_id, code, value, updated_at) values (?, ?, ?, ?)", rows)
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
                try:
                    user_info = await self._retry_get_player_info(
                        leaderboard_stat.user_id
                    )
                except ExternalServiceError as error:
                    if error.status == 500:
                        # Bugged user, not possible to view!
                        _logger.warn("skipped bugged user %s", leaderboard_stat.user_id)
                        continue
                    raise
                assert (
                    user_info is not None
                ), "user info missing even though it was on the leaderboard"
                try:
                    user_stats = await self._vail_client.get_accelbyte_user_stats(
                        leaderboard_stat.user_id
                    )
                except ExternalServiceError as error:
                    _logger.error("failed to fetch the stats of %s", leaderboard_stat.user_id)
                    continue
                assert (
                    user_stats is not None
                ), "user stats missing even though it was on the leaderboard"
                scraped_at = time.time()
                async with self._database_lock.shared():
                    await self._database.execute(
                        "insert or replace into users (id, name) values (?, ?)",
                        [leaderboard_stat.user_id, user_info.display_name],
                    )
                    await self._database.executemany("insert or replace into stats (code, user_id, value, updated_at) values (?, ?, ?, ?)", [(stat_code, leaderboard_stat.user_id, value, scraped_at) for stat_code, value in user_stats.items()])
                        
                    # Removed stat codes
                    result = await self._database.execute("select code from stats where user_id = ?", [leaderboard_stat.user_id])
                    removed_stat_codes = []
                    for row in await result.fetchall():
                        stat_code = row[0]
                        if stat_code not in user_stats.keys():
                            removed_stat_codes.append(stat_code)

                    await self._database.executemany("delete from stats where user_id = ? and code = ?", [(leaderboard_stat.user_id, removed_stat_code) for removed_stat_code in removed_stat_codes])

                    await self._database.commit()

            page_id += 1
    async def _retry_get_player_info(self, user_id: str) -> AccelBytePlayerInfo | None:
        for i in range(3):
            try:
                return await self._vail_client.get_accelbyte_user_info(
                    user_id
                )
            except ExternalServiceError as error:
                if error.status == 500:
                    continue
                raise
        else:
            raise error # type: ignore
