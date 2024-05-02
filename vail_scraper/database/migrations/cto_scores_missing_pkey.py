import aiosqlite

from .base import BaseMigration


class CTOScoresMissingPKey(BaseMigration):
    @property
    def migration_id(self) -> str:
        return "cto-scores-missing-pkey"

    async def upgrade(self, connection: aiosqlite.Connection) -> None:
        # XP stats
        await connection.execute("alter table xp_stats rename to old_xp_stats")
        await connection.execute(
            """
            create table xp_stats (
                id text not null primary key,
                xp integer not null,
                last_scraped_at real not null
            )
         """
        )
        await connection.execute(
            "insert or replace into xp_stats (id, xp, last_scraped_at) select id, xp, last_scraped_at from old_xp_stats"
        )
        await connection.execute("drop table old_xp_stats")

        await connection.execute(
            "alter table cto_steal_stats rename to old_cto_steal_stats"
        )
        await connection.execute(
            """
        create table cto_steal_stats (
            id text not null primary key,
            steals integer not null,
            last_scraped_at real not null
        )
        """
        )
        await connection.execute(
            "insert or replace into cto_steal_stats (id, steals, last_scraped_at) select id, steals, last_scraped_at from old_cto_steal_stats"
        )
        await connection.execute("drop table old_cto_steal_stats")

        await connection.execute(
            "alter table cto_recover_stats rename to old_cto_recover_stats"
        )
        await connection.execute(
            """
        create table cto_recover_stats (
            id text not null primary key,
            recovers integer not null,
            last_scraped_at real not null
        )
        """
        )
        await connection.execute(
            "insert or replace into cto_recover_stats (id, recovers, last_scraped_at) select id, recovers, last_scraped_at from old_cto_recover_stats"
        )
        await connection.execute("drop table old_cto_recover_stats")
