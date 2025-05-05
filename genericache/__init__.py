from typing import Callable, Generic, Iterable, Optional, Protocol, Tuple, TypeVar
import logging
import os

from .digest import ContentDigest

logger = logging.getLogger(__name__)


U = TypeVar("U")

class CacheException(Exception):
    pass

class FetchInterrupted(CacheException, Generic[U]):
    def __init__(self, *, url: U) -> None:
        self.url = url
        super().__init__(f"Downloading of '{url}' was interrupted")

class BytesReader(Protocol):
    def read(self, size: int = -1, /) -> bytes: ...
    def readable(self) -> bool: ...
    def seek(self, offset: int, whence: int = os.SEEK_SET, /) -> int: ...
    def seekable(self) -> bool: ...
    def tell(self) -> int: ...
    @property
    def closed(self) -> bool: ...

class Cache(Protocol[U]):
    def hits(self) -> int: ...
    def misses(self) -> int: ...
    def get_by_url(self, *, url: U) -> Optional[Tuple[BytesReader, ContentDigest]]: ...
    def get(self, *, digest: ContentDigest) -> Optional[BytesReader]: ...
    def try_fetch(self, url: U, fetcher: Callable[[U], Iterable[bytes]]) -> "Tuple[BytesReader, ContentDigest] | FetchInterrupted[U]": ...
    def fetch(self, url: U, fetcher: Callable[[U], Iterable[bytes]], retries: int = 3) -> "Tuple[BytesReader, ContentDigest]":
        for _ in range(retries):
            result = self.try_fetch(url, fetcher)
            if not isinstance(result, FetchInterrupted):
                return result
        raise RuntimeError("Number of retries exhausted")

from .disk_cache import DiskCache as DiskCache
from .memory_cache import MemoryCache as MemoryCache
from .noop_cache import NoopCache as NoopCache
