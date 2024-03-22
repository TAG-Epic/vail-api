import aiosqlite

from .base import BaseMigration


class CreateStatsTableMigration(BaseMigration):
    @property
    def migration_id(self) -> str:
        return "create-stats-table"

    async def upgrade(self, connection: aiosqlite.Connection) -> None:
        await connection.execute(
            """
            create table stats (
                id text primary key,
                won integer not null,
                lost integer not null,
                draws integer not null,
                abandoned integer not null,
                kills integer not null,
                assists integer not null,
                deaths integer not null,
                points integer not null,
                game_hours real not null,

                foreign key (id) references users(id)
            )
         """
        )
