from abc import ABC, abstractmethod, abstractproperty

import aiosqlite


class BaseMigration(ABC):
    @abstractproperty
    def migration_id(self) -> str: ...

    @abstractmethod
    async def upgrade(self, connection: aiosqlite.Connection) -> None: ...
