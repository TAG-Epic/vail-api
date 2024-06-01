import asyncio
from logging import getLogger
import typing
import aiosqlite
import time
from aiohttp import ClientSession
import traceback

from slowstack.asynchronous.times_per import TimesPerRateLimiter
from nextcore.http import HTTPClient, Route

from vail_scraper.utils.unique_queue import UniqueQueue

from .database.quest import QuestDBWrapper
from .models.accelbyte import AccelBytePlayerInfo, AccelByteStatCode
from .client.accelbyte import AccelByteClient
from .client.epic_games import EpicGamesClient
from .utils.circuit_breaker import CircuitBreaker
from .utils.exclusive_lock import ExclusiveLock
from .config import ScraperConfig
from .errors import ExternalServiceError

_logger = getLogger(__name__)


class VailScraper:
    def __init__(
        self,
        database: aiosqlite.Connection,
        database_lock: ExclusiveLock,
        quest_db: QuestDBWrapper,
        accel_byte_client: AccelByteClient,
        epic_games_client: EpicGamesClient,
        config: ScraperConfig,
    ) -> None:
        self._rate_limiter: TimesPerRateLimiter = TimesPerRateLimiter(
            config.rate_limiter.times, config.rate_limiter.per
        )
        self.circuit_breaker: CircuitBreaker = CircuitBreaker(5, 10)
        self._database: aiosqlite.Connection = database
        self._database_lock: ExclusiveLock = database_lock
        self._quest_db: QuestDBWrapper = quest_db
        self._accel_byte_client: AccelByteClient = accel_byte_client
        self._epic_games_client: EpicGamesClient = epic_games_client
        self._discord_client: HTTPClient = HTTPClient()
        self._config: ScraperConfig = config

        # Accel fast
        self._user_ids_pending_scrape: UniqueQueue[str] = UniqueQueue()

    async def run(self) -> None:
        await self._discord_client.setup()

        tasks: list[asyncio.Task[None]] = []
        if not self._config.bans.accelbyte:
            tasks.append(asyncio.create_task(self._fast_scrape_accelbyte_discoverer()))
            tasks.append(asyncio.create_task(self._fast_scrape_accelbyte_updater()))
        try:
            await asyncio.gather(*tasks)
        except:
            error_details = traceback.format_exc()

            async with ClientSession() as session:
                response = await session.post("https://workbin.dev/api/new", json={
                    "content": error_details,
                    "language": "python"
                })
                data = await response.json()
                paste_url = f"https://workbin.dev/?id={data['key']}"

            route = Route("POST", "/webhooks/{webhook_id}/{webhook_token}")
            await self._discord_client.request(route, None, json={
                "content": f"<@{self._config.alert_webhook.target_user}> your code sucks: {paste_url}"
            })

    async def _fast_scrape_accelbyte_discoverer(self) -> None:
        page_id = 0
        spotted_user_ids: list[str] = []
        total_outdated_user_ids: list[str] = []
        while True:
            # Check if we need to fetch more items
            if len(self._user_ids_pending_scrape) > 50:
                await asyncio.sleep(10)
                continue

            try:
                leaderboard_page = await self._accel_byte_client.get_leaderboard_page(AccelByteStatCode.SCORE, page_id=page_id)
            except ExternalServiceError:
                _logger.error("failed to get leaderboard page %s, skipping for now", page_id, exc_info=True)
                page_id += 1
                continue

            if len(leaderboard_page) == 0:
                _logger.debug("finished checking @ page %s", page_id)
                started_post_scrape = time.time()

                # Find users not spotted (aka moved up ranking while we checked)
                result = await self._database.execute("select id from users")

                async for row in self._chunk_aiosqlite_response(result):
                    user_id = row[0]

                    if user_id not in spotted_user_ids:
                        _logger.debug("didn't spot %s during leaderboard scrape, checking just to make sure!", user_id)
                        self._user_ids_pending_scrape.add(user_id)

                    # Since we are going over quite a lot of rows, we chunk it and do frequent asyncio context switches to lighten the load
                    await asyncio.sleep(0)


                if len(total_outdated_user_ids) == 0:
                    _logger.debug("no updates, sleeping for 10s")
                    await asyncio.sleep(10)
                page_id = 0
                spotted_user_ids.clear()
                total_outdated_user_ids.clear()

                # Report user count
                result = await self._database.execute("select count(*) from users")
                row = await result.fetchone()
                assert row is not None
                await self._quest_db.ingest("user_count", [{"count": row[0]}])
                
                finished_post_scrape = time.time()
                _logger.debug("used %s seconds to do post-scrape", finished_post_scrape - started_post_scrape)

                continue

            user_id_to_score: dict[str, int] = {user.user_id:user.point for user in leaderboard_page}
            outdated_users: list[str] = []
            
            for user in leaderboard_page:
                spotted_user_ids.append(user.user_id)
                result = await self._database.execute("select value from stats where user_id = ? and code = 'score'", [user.user_id])
                row = await result.fetchone()

                if row is None:
                    outdated_users.append(user.user_id)
                    total_outdated_user_ids.append(user.user_id)
                    continue
                stored_score = row[0]
                if stored_score != user_id_to_score[user.user_id]:
                    outdated_users.append(user.user_id)

            _logger.debug("fetched page %s for fast-scraping (%s/%s outdated). %s/50 outdated users found", page_id, len(outdated_users), len(leaderboard_page), len(self._user_ids_pending_scrape))

            for user_id in outdated_users:
                self._user_ids_pending_scrape.add(user_id)

            page_id += 1

    async def _chunk_aiosqlite_response(self, cursor: aiosqlite.Cursor, chunk_size: int = 1000) -> typing.AsyncGenerator[aiosqlite.Row, None]:
        rows: list[aiosqlite.Row] = []

        while True:
            if len(rows) == 0:
                await asyncio.sleep(0) # Allow context switch
                rows = list(await cursor.fetchmany(chunk_size))
            row = rows.pop(0)
            if row is None:
                break
            yield row

    async def _fast_scrape_accelbyte_updater(self) -> None:
        while True:
            user_id = await self._user_ids_pending_scrape.get_item()
            try:
                user_info = await self._retry_get_player_info(user_id)
            except ExternalServiceError:
                _logger.warn(
                    "failed to fetch the info of user %s",
                    user_id,
                    exc_info=True,
                )
                continue
            if user_info is None:
                _logger.warn("user %s magically disappeared. Leaving it incase accelbyte did an oopsie", user_id)
                continue
            try:
                user_stats = await self._accel_byte_client.get_user_stats(user_id)
            except ExternalServiceError:
                _logger.warn(
                    "failed to fetch the stats of user %s",
                    user_id,
                    exc_info=True,
                )
                continue
            assert user_stats is not None

            scraped_at = time.time()

            await self._quest_db.ingest_user_stats(
                user_id, user_stats
            )
            async with self._database_lock.shared():
                await self._database.execute(
                    "insert or replace into users (id, name) values (?, ?)",
                    [user_id, user_info.display_name],
                )
                await self._database.executemany(
                    "insert or replace into stats (code, user_id, value, updated_at) values (?, ?, ?, ?)",
                    [
                        (stat_code, user_id, value, scraped_at)
                        for stat_code, value in user_stats.items()
                    ],
                )

                # Removed stat codes
                result = await self._database.execute(
                    "select code from stats where user_id = ?",
                    [user_id],
                )
                removed_stat_codes = []
                for row in await result.fetchall():
                    stat_code = row[0]
                    if stat_code not in user_stats.keys():
                        removed_stat_codes.append(stat_code)

                await self._database.executemany(
                    "delete from stats where user_id = ? and code = ?",
                    [
                        (user_id, removed_stat_code)
                        for removed_stat_code in removed_stat_codes
                    ],
                )

                await self._database.commit()


    async def _retry_get_player_info(self, user_id: str) -> AccelBytePlayerInfo | None:
        for i in range(3):
            try:
                return await self._accel_byte_client.get_user_info(user_id)
            except ExternalServiceError as error:
                if error.status == 500:
                    continue
                raise
        else:
            raise error  # type: ignore
