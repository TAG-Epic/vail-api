import csv
import io
from typing import Any
from aiohttp import ClientSession

class QuestDBWrapper:
    def __init__(self, base_url: str) -> None:
        self._base_url: str = base_url
        self._session: ClientSession | None = None

    async def _get_session(self) -> ClientSession:
        if self._session is None:
            self._session = ClientSession(base_url=self._base_url)
        return self._session


    async def ingest(self, table_name: str, records: list[dict[str, Any]]) -> None:
        if len(records) == 0:
            return
        session = await self._get_session()
        
        file = io.StringIO()
        writer = csv.DictWriter(file, records[0].keys())
        writer.writeheader()
        writer.writerows(records)

        response = await session.post("/imp", params={"name": table_name}, data=file)
        response.raise_for_status()
