import aiosqlite

from .base import BaseMigration


class CreateUsersTableMigration(BaseMigration):
    @property
    def migration_id(self) -> str:
        return "create-users-table"

    async def upgrade(self, connection: aiosqlite.Connection) -> None:
        await connection.execute(
            """
            create table users (
                id text primary key,
                name text not null,
                last_scraped real not null
            )
         """
        )
