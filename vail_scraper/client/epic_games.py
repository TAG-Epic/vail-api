import typing

import aiohttp
from slowstack.asynchronous.times_per import TimesPerRateLimiter

from vail_scraper.config import ScraperConfig
from vail_scraper.utils.circuit_breaker import CircuitBreaker
from .base import BaseService, get_token_expiry_in_seconds, TOKEN_MARGIN_SECONDS

_EPIC_DEPLOYMENT_ID: str = "db1cb57993ef44bab8084fb3c4ecb334"


class EpicGamesClient(BaseService):
    def __init__(self, config: ScraperConfig) -> None:
        self._config: typing.Final[ScraperConfig] = config
        self._session: aiohttp.ClientSession | None = None
        self._rate_limiter: TimesPerRateLimiter = TimesPerRateLimiter(
            config.rate_limiter.times, config.rate_limiter.per
        )
        self._circuit_breaker: CircuitBreaker = CircuitBreaker(5, 60)
        self._access_token: str | None = None

    async def _get_token(self) -> str:
        session = await self.get_session()

        if self._access_token is not None:
            if get_token_expiry_in_seconds(self._access_token) > TOKEN_MARGIN_SECONDS:
                return self._access_token

        response = await session.post(
            "https://api.epicgames.dev/auth/v1/oauth/token",
            data={
                "grant_type": "client_credentials",
                "deployment_id": _EPIC_DEPLOYMENT_ID,
            },
        )
        with self._circuit_breaker:
            response.raise_for_status()
            data = await response.json()

        return data["access_token"]
