import aiosqlite

from .base import BaseMigration


class AddIndexesMigration(BaseMigration):
    @property
    def migration_id(self) -> str:
        return "add-indexes"

    async def upgrade(self, connection: aiosqlite.Connection) -> None:
        await connection.execute("create index stats_code on stats(code)")
        await connection.execute("create index stats_user_id on stats(user_id)")
