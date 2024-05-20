import aiosqlite
from logging import getLogger

from .migrations.base import BaseMigration
from .migrations.create_users_table import CreateUsersTableMigration
from .migrations.create_stats_table import CreateStatsTableMigration
from .migrations.add_cto_scores import AddCTOScoresMigration
from .migrations.cto_scores_missing_pkey import CTOScoresMissingPKeyMigration
from .migrations.save_stats_independently import SaveStatsIndependentlyMigration
from .migrations.add_indexes import AddIndexesMigration

_logger = getLogger(__name__)

MIGRATIONS: list[BaseMigration] = [
    CreateUsersTableMigration(),
    CreateStatsTableMigration(),
    AddCTOScoresMigration(),
    CTOScoresMissingPKeyMigration(),
    SaveStatsIndependentlyMigration(),
    AddIndexesMigration()
]


async def do_migrations(connection: aiosqlite.Connection) -> None:
    # Hard coded init migration
    await connection.execute(
        "create table if not exists migrations (migration_id text primary key)"
    )

    for migration in MIGRATIONS:
        migration_id = migration.migration_id

        result = await connection.execute(
            "select migration_id from migrations where migration_id = ?", [migration_id]
        )
        row = await result.fetchone()
        if row is None:
            _logger.info("running migration %s", migration_id)
            await migration.upgrade(connection)
            await connection.execute(
                "insert into migrations (migration_id) values (?)", [migration_id]
            )
            await connection.commit()
