import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator


class ExclusiveLock:
    def __init__(self) -> None:
        self._reads: list[asyncio.Future[None]] = []
        self._read_opened: asyncio.Event = asyncio.Event()
        self._read_opened.set()
        self._write_lock: asyncio.Lock = asyncio.Lock()

    @asynccontextmanager
    async def shared(self) -> AsyncGenerator[None, None]:
        await self._read_opened.wait()
        future: asyncio.Future[None] = asyncio.Future()
        self._reads.append(future)
        try:
            yield
        finally:
            future.set_result(None)
            self._reads.remove(future)

    @asynccontextmanager
    async def exclusive(self) -> AsyncGenerator[None, None]:
        async with self._write_lock:
            self._read_opened.clear()
            try:
                await asyncio.gather(*self._reads)
                yield
            finally:
                self._read_opened.set()
