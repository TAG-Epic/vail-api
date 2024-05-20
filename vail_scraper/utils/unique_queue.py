import asyncio
from typing import Generic, TypeVar

ItemT = TypeVar("ItemT")

class UniqueQueue(Generic[ItemT]):
    def __init__(self) -> None:
        self._items: set[ItemT] = set()
        self._items_available: asyncio.Event = asyncio.Event()

    def add(self, item: ItemT) -> None:
        if item not in self._items:
            self._items.add(item)
            self._items_available.set()

    async def get_item(self) -> ItemT:
        await self._items_available.wait()
        item = self._items.pop()
        if len(self._items) == 0:
            self._items_available.clear()
        return item

    def __len__(self) -> int:
        return len(self._items)
