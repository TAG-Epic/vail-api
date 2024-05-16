import csv
import io
from datetime import datetime
from logging import getLogger
from typing import Any
from aiohttp import ClientSession, FormData

_logger = getLogger(__name__)


class QuestDBWrapper:
    def __init__(self, base_url: str) -> None:
        self._base_url: str = base_url
        self._session: ClientSession | None = None

    async def _get_session(self) -> ClientSession:
        if self._session is None:
            self._session = ClientSession(base_url=self._base_url)
        return self._session

    async def ingest(self, table_name: str, records: list[dict[str, Any]]) -> None:
        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        _logger.info("ingesting %s records into %s", len(records), table_name)
        if len(records) == 0:
            return

        for record in records:
            if "timestamp" not in record:
                record["timestamp"] = timestamp

        session = await self._get_session()

        _logger.debug("creating ingest csv")
        file = io.StringIO()
        writer = csv.DictWriter(file, records[0].keys())
        writer.writeheader()
        writer.writerows(records)
        _logger.debug("created ingest csv")
        file.seek(0)
        text = file.read()

        form = FormData()
        form.add_field("data", text, content_type="text/csv", filename="data.csv")

        response = await session.post(
            "/imp",
            params={
                "name": table_name,
                "timestamp": "timestamp",
                "overwrite": "true",
                "fmt": "json",
            },
            data=form,
        )
        response.raise_for_status()

        data = await response.json()
        if data["status"] != "OK":
            raise UploadError(data["status"])
        _logger.debug("ingested successfully")

    async def ingest_user_stats(self, user_id: str, stats: dict[str, float]) -> None:
        await self.ingest(
            "user_stats",
            [
                {"code": stat_code, "value": value, "user_id": user_id}
                for stat_code, value in stats.items()
            ],
        )


class UploadError(Exception):
    pass
