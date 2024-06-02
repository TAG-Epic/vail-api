import typing
from aiohttp import ClientResponse, ClientSession
from urllib.parse import quote
from vail_scraper.errors import ExternalServiceError, Service

from vail_scraper.models.meilisearch import SearchResults

class MeiliSearch:
    def __init__(self, base_url: str) -> None:
        self._base_url: str = base_url
        self._session: ClientSession | None = None

    async def _get_session(self) -> ClientSession:
        if self._session is None:
            self._session = ClientSession(base_url=self._base_url)
        return self._session

    async def _raise_for_status(self, response: ClientResponse):
        if not response.ok:
            raise ExternalServiceError(
                Service.MEILISEARCH,
                response.status,
                await response.text(),
            )
    
    async def ingest_documents(self, index_name: str, primary_key: str, documents: list[typing.Any]) -> None:
        session = await self._get_session()
        response = await session.post(f"/indexes/{quote(index_name)}/documents", params={"primaryKey": primary_key}, json=documents)
        response.raise_for_status()

    async def search(self, index_name: str, query: str) -> SearchResults:
        session = await self._get_session()
        response = await session.post(f"/indexes/{quote(index_name)}/search", json={"q": query})
        await self._raise_for_status(response)

        raw_data = await response.text()
        return SearchResults.model_validate_json(raw_data)
