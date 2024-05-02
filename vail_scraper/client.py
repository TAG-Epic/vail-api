import json
from logging import getLogger
import typing
import time
import hashlib
from uuid import uuid4
import base64
import secrets
import asyncio
from yarl import URL
from urllib.parse import quote

from slowstack.asynchronous.times_per import TimesPerRateLimiter
import aiohttp

from .errors import NoContentPageBug
from .models import (
    AccelByteLeaderboardPage,
    AccelByteLeaderboardPlayer,
    AccelBytePlayerInfo,
    AccelBytePlayerStatItemsPage,
    AexlabLeaderboardPage,
    AexlabLeaderboardPlayer,
    AccelByteStatCode,
    AexlabStatCode,
)
from .utils.circuit_breaker import CircuitBreaker
from .config import ScraperConfig

_CLIENT_ID: str = "8e1fbe68aef14404882e88358be5536b"
_logger = getLogger(__name__)


class VailClient:
    def __init__(self, scraper_config: ScraperConfig) -> None:
        self._session: aiohttp.ClientSession | None = None
        self._config: ScraperConfig = scraper_config
        self._rate_limiter: TimesPerRateLimiter = TimesPerRateLimiter(
            scraper_config.rate_limiter.times, scraper_config.rate_limiter.per
        )
        self._circuit_breaker: CircuitBreaker = CircuitBreaker(5, 60)
        self._token_lock: asyncio.Lock = asyncio.Lock()
        self._refresh_token: str | None = None
        self._access_token: str | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession(
                headers={"Bot": "true", "User-Agent": self._config.user_agent}
            )
        return self._session

    async def _request(
        self,
        method: typing.Literal["GET", "POST", "PATCH", "DELETE"],
        url: str,
        *,
        priority: int = 0,
        **kwargs: typing.Any,
    ) -> aiohttp.ClientResponse:
        session = await self._get_session()
        with self._circuit_breaker:
            async with self._rate_limiter.acquire(priority=priority):
                return await session.request(method, url, **kwargs)

    async def _get_refresh_token(self) -> tuple[str, str]:
        session = await self._get_session()

        challenge_verifier = secrets.token_urlsafe(50)
        challenge = (
            base64.urlsafe_b64encode(
                hashlib.sha256(challenge_verifier.encode()).digest()
            )
            .decode()
            .rstrip("=")
        )

        _logger.debug(
            "made challenge: %s (%s) verifier: %s (%s)",
            challenge,
            len(challenge),
            challenge_verifier,
            len(challenge_verifier),
        )

        csrf = str(uuid4())

        state = {"csrf": csrf, "payload": {"path": "/account/overview"}}

        # Get request id
        with self._circuit_breaker:
            async with self._rate_limiter.acquire(priority=100):
                response = await session.get(
                    "https://login.vailvr.com/iam/v3/oauth/authorize",
                    params={
                        "response_type": "code",
                        "client_id": _CLIENT_ID,
                        "redirect_url": "https://login.vailvr.com",
                        "code_challenge": challenge,
                        "code_challenge_method": "S256",
                        "createHeadless": "false",
                        "state": json.dumps(state),
                    },
                    allow_redirects=False,
                )
        response.raise_for_status()
        url = URL(response.headers["Location"])
        _logger.debug("authorize query: %s", url.query_string)
        request_id = url.query.get("request_id")
        # TODO: Error handling
        assert request_id is not None, "no request id :("

        session.cookie_jar.update_cookies({"request_id": request_id})
        response = await session.post(
            "https://login.vailvr.com/iam/v3/authenticate",
            data={
                "request_id": request_id,
                "redirect_uri": "https://login.vailvr.com",
                "client_id": _CLIENT_ID,
                "user_name": self._config.user.email,
                "password": self._config.user.password,
            },
            allow_redirects=False,
        )
        url = URL(response.headers["Location"])
        _logger.debug("authenticate query: %s", url.query_string)
        code = url.query.get("code")
        # TODO: Error handling
        assert code is not None, "code missing :("
        _logger.debug("got code: %s", code)

        with self._circuit_breaker:
            async with self._rate_limiter.acquire(priority=100):
                response = await session.post(
                    "https://login.vailvr.com/iam/v3/oauth/token",
                    data={
                        "grant_type": "authorization_code",
                        "code": code,
                        "code_verifier": challenge_verifier,
                        "client_id": _CLIENT_ID,
                    },
                    allow_redirects=False,
                )

            if not response.ok:
                raise AssertionError(f"fuck: {await response.text()}")

        data = await response.json()
        _logger.debug("code data: %s", data)

        return data["refresh_token"], data["access_token"]

    def _is_token_expired(self, token: str) -> bool:
        header, body, signature = token.split(".")
        del header, signature
        body_data = json.loads(base64.b64decode(body + "=="))

        return not (body_data["iat"] + 10 > time.time())

    async def _get_token(self) -> str:
        async with self._token_lock:
            if self._refresh_token is None or self._access_token is None:
                self._refresh_token, self._access_token = (
                    await self._get_refresh_token()
                )
            if self._is_token_expired(self._refresh_token):
                self._refresh_token, self._access_token = (
                    await self._get_refresh_token()
                )
            if self._is_token_expired(self._access_token):
                # TODO: Actually refresh
                self._refresh_token, self._access_token = (
                    await self._get_refresh_token()
                )

        return self._access_token

    async def get_aexlab_leaderboard_page(
        self, stat_code: AexlabStatCode, *, page_id: int = 0, page_size: int = 100
    ) -> list[AexlabLeaderboardPlayer]:
        session = await self._get_session()
        with self._circuit_breaker:
            async with self._rate_limiter.acquire():
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
            if not response.ok:
                _logger.error("failed to parse result: %s", await response.text())
                response.raise_for_status()
            data = await response.text()
            page = AexlabLeaderboardPage.validate_json(data)
            return page

    async def get_accelbyte_leaderboard_page(
        self, stat_code: AccelByteStatCode, *, page_id: int = 0, page_size=100
    ) -> list[AccelByteLeaderboardPlayer]:
        session = await self._get_session()
        token = await self._get_token()

        with self._circuit_breaker:
            async with self._rate_limiter.acquire():
                response = await session.get(
                    f"https://login.vailvr.com/leaderboard/v3/public/namespaces/vailvr/leaderboards/{stat_code}/alltime",
                    headers={"Authorization": f"Bearer {token}"},
                    params={"limit": page_size, "offset": page_id * page_size},
                )
            if not response.ok:
                _logger.error("failed to parse result: %s", await response.text())
                response.raise_for_status()
            data = await response.text()
            page = AccelByteLeaderboardPage.model_validate_json(data)
            return page.data

    async def get_accelbyte_user_stats(
        self, user_id: str
    ) -> dict[AccelByteStatCode, float]:
        session = await self._get_session()
        token = await self._get_token()

        with self._circuit_breaker:
            async with self._rate_limiter.acquire():
                response = await session.get(
                    f"https://login.vailvr.com/social/v1/public/namespaces/vailvr/users/{quote(user_id)}/statitems",
                    params={"limit": 1000},
                    headers={"Authorization": f"Bearer {token}"},
                )
            data = await response.text()
            page = AccelBytePlayerStatItemsPage.model_validate_json(data)
            return {stat_item.stat_code: stat_item.value for stat_item in page.data}

    async def get_accelbyte_user_info(self, user_id: str) -> AccelBytePlayerInfo:
        session = await self._get_session()
        token = await self._get_token()

        with self._circuit_breaker:
            async with self._rate_limiter.acquire():
                response = await session.get(
                    f"https://login.vailvr.com/iam/v3/public/namespaces/vailvr/users/{quote(user_id)}",
                    params={"limit": 1000},
                    headers={"Authorization": f"Bearer {token}"},
                )
            data = await response.text()
            user = AccelBytePlayerInfo.model_validate_json(data)
            return user
