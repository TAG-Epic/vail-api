import aiosqlite

from .base import BaseMigration


class SaveStatsIndependentlyMigration(BaseMigration):
    @property
    def migration_id(self) -> str:
        return "save-stats-independently"

    async def upgrade(self, connection: aiosqlite.Connection) -> None:
        await connection.executescript("""
            create table stats (
                code string not null,
                user_id string not null,
                value real not null,
                updated_at real not null,

                primary key (code, user_id)
            );
            drop table general_stats;
            drop table xp_stats;
            drop table cto_steal_stats;
            drop table cto_recover_stats;
        """)
