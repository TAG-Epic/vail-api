from logging import getLogger

from ..errors import NoContentPageBug
from ..models.aexlab import AexlabLeaderboardPage, AexLabLeaderboardPlayer, AexLabStatCode
from ..config import ScraperConfig
from .base import BaseService

_logger = getLogger(__name__)


class AexLabClient(BaseService):
    def __init__(self, config: ScraperConfig) -> None:
        super().__init__(config)

    async def get_leaderboard_page(
        self,
        stat_code: AexLabStatCode,
        *,
        page_id: int = 0,
        page_size: int = 100,
        priority: int = 0,
    ) -> list[AexLabLeaderboardPlayer]:
        session = await self.get_session()
        with self._circuit_breaker:
            async with self._rate_limiter.acquire(priority=priority):
                response = await session.get(
                    "https://aexlab.com/api/leaderboard",
                    params={
                        "leaderboardCode": stat_code,
                        "page": page_id + 1,
                        "pageLimit": page_size,
                    },
                )
            if response.status == 204:
                _logger.warn("204 page!")
                raise NoContentPageBug()
            await self.raise_for_status(response)
            data = await response.text()
            page = AexlabLeaderboardPage.validate_json(data)
            return page
