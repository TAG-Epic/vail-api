from logging import getLogger
import typing
import aiosqlite
import time
from aiohttp import ClientSession
from nextcore.common.times_per import TimesPer

from .config import Config
from .errors import NoContentPageBug
from .models import ScoreLeaderboardPage, ScoreLeaderboardPlayer

_logger = getLogger(__name__)


class VailScraper:
    def __init__(self, database: aiosqlite.Connection, config: Config) -> None:
        self._session: ClientSession = ClientSession(
            headers={"User-Agent": config.user_agent, "Bot": "true"},
            base_url="https://aexlab.com",
        )
        self._rate_limiter: TimesPer = TimesPer(
            config.rate_limiter.times, config.rate_limiter.per
        )
        self._database: aiosqlite.Connection = database
        self.started_scraping_at: float = time.time()
        self.last_scrape_at: float = time.time()
        self.users_failed_scrape: int = 0
        self.last_scrape_duration: float = 0

    async def run(self) -> None:
        while True:
            await self._tick()

    async def _tick(self) -> None:
        self.started_scraping_at = time.time()

        page_id = 0
        while True:
            try:
                page = await self.get_page(page_id)
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
            await self._database.executemany(
                "insert or replace into users (id, name, last_scraped) values (?, ?, ?)",
                [(user.user_id, user.display_name, paged_scraped_at) for user in page],
            )
            await self._database.executemany(
                "insert or replace into stats (id, won, lost, draws, abandoned, kills, assists, deaths, points, game_hours) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
                        user.stats.point,
                        user.stats.game_hours,
                    )
                    for user in page
                ],
            )
            await self._database.commit()

            page_id += 1

        result = await self._database.execute(
            "select id, name from users where last_scraped < ?", [self.last_scrape_at]
        )
        rows = list(await result.fetchall())
        self.users_failed_scrape = len(rows)
        if self.users_failed_scrape > 0:
            _logger.warn("%s users failed to fetch", self.users_failed_scrape)
            _logger.warn("failed to fetch %s", ", ".join([row[1] for row in rows]))

        self.last_scrape_at = time.time()
        self.last_scrape_duration = self.last_scrape_at - self.started_scraping_at
        _logger.info("scrape took %s seconds", self.last_scrape_duration)

    async def get_page(
        self, page_id: int, page_size: int = 100
    ) -> list[ScoreLeaderboardPlayer]:
        error: Exception | None = None
        for retry_attempt in range(5):
            async with self._rate_limiter.acquire():
                _logger.debug("fetching page %s", page_id)
                try:
                    response = await self._session.get(
                        "/api/leaderboard",
                        params={
                            "leaderboardCode": "score",
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
