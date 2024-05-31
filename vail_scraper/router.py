from typing import Sequence, overload, Iterator
from aiohttp.web import RouteTableDef, AbstractRouteDef

class CombinerRouteTableDef(Sequence[AbstractRouteDef]):
    def __init__(self) -> None:
        self._items: list[AbstractRouteDef] = []

    def add_router(self, router: RouteTableDef) -> None:
        self._items.extend(router)

    @overload
    def __getitem__(self, index: int) -> AbstractRouteDef:
        ...

    @overload
    def __getitem__(self, index: slice) -> list[AbstractRouteDef]:
        ...

    def __getitem__(self, index):  # type: ignore[no-untyped-def]
        return self._items[index]

    def __iter__(self) -> Iterator[AbstractRouteDef]:
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)

    def __contains__(self, item: object) -> bool:
        return item in self._items
