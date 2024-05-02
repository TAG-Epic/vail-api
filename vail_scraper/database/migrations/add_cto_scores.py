import aiosqlite

from .base import BaseMigration


class AddCTOScoresMigration(BaseMigration):
    @property
    def migration_id(self) -> str:
        return "add-cto-scores"

    async def upgrade(self, connection: aiosqlite.Connection) -> None:
        # Move last_scraped to per stat table definitions
        await connection.execute("alter table stats rename to general_stats")
        await connection.execute(
            "alter table general_stats add column last_scraped_at real not null default 0"
        )
        await connection.execute(
            "update general_stats set last_scraped_at = (select last_scraped from users where id = general_stats.id)"
        )
        await connection.execute("alter table users drop column last_scraped")

        # Score stats
        await connection.execute(
            """
            create table xp_stats (
                id text not null,
                xp integer not null,
                last_scraped_at real not null
            )
         """
        )
        await connection.execute(
            "insert into xp_stats (id, xp, last_scraped_at) select id, points, last_scraped_at from general_stats"
        )
        await connection.execute("alter table general_stats drop column points")

        # CTO Stats
        await connection.execute(
            """
        create table cto_steal_stats (
            id text not null,
            steals integer not null,
            last_scraped_at real not null
        )
        """
        )
        await connection.execute(
            """
        create table cto_recover_stats (
            id text not null,
            recovers integer not null,
            last_scraped_at real not null
        )
        """
        )
