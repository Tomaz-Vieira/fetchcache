from pathlib import Path
from datetime import datetime
from typing import Any, Callable, Final, Generic, Iterable, Optional, Protocol, Type, TypeVar
import logging
import os

from .digest import ContentDigest, UrlDigest

logger = logging.getLogger(__name__)


U = TypeVar("U")

class CacheException(Exception):
    pass

class ContentUnavailable(CacheException):
    def __init__(self, content_digest: ContentDigest) -> None:
        self.content_digest = content_digest
        super().__init__(f"Contents unavailable for digest {content_digest}")

class FetchInterrupted(CacheException, Generic[U]):
    def __init__(self, *, url: U) -> None:
        self.url = url
        super().__init__(f"Downloading of '{url}' was interrupted")

class CacheUrlTypeMismatch(CacheException):
    def __init__(
        self,
        cache_dir: Path,
        expected_url_type: Type[Any],
        found_url_type: Type[Any],
    ) -> None:
        self.expected_url_type_name = expected_url_type.__qualname__
        self.found_url_type_name = found_url_type.__qualname__
        super().__init__(
            f"Expected cache at {cache_dir} to have URLs of type {self.expected_url_type_name}"
            f" but request was {self.found_url_type_name}"
        )

class CacheFsLinkUsageMismatch(CacheException):
    def __init__(
        self,
        cache_dir: Path,
        expected: bool,
        found: bool,
    ) -> None:
        self.expected_symlink_usage = expected,
        self.found_symlink_usage = found
        super().__init__(
            f"Expected cache at {cache_dir} to have symlinking set to {expected}, requested {found}"
        )

class BytesReaderP(Protocol):
    def read(self, size: int = -1, /) -> bytes: ...
    def readable(self) -> bool: ...
    def seek(self, offset: int, whence: int = os.SEEK_SET, /) -> int: ...
    def seekable(self) -> bool: ...
    def tell(self) -> int: ...
    @property
    def closed(self) -> bool: ...

class CacheEntry(BytesReaderP):
    url_digest: Final[UrlDigest]
    content_digest: Final[ContentDigest]
    timestamp: Final[datetime]

    def __init__(
        self,
        url_digest: UrlDigest,
        content_digest: ContentDigest,
        reader: BytesReaderP,
        timestamp: datetime,
    ) -> None:
        self.url_digest = url_digest
        self.content_digest = content_digest
        self._reader = reader
        self.timestamp = timestamp
        super().__init__()

    def read(self, size: int = -1, /) -> bytes:
        return self._reader.read(size)

    def readable(self) -> bool:
        return True

    def seek(self, offset: int, whence: int = os.SEEK_SET, /) -> int:
        return self._reader.seek(offset, whence)

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._reader.tell()

    @property
    def closed(self) -> bool:
        return self._reader.closed



class Cache(Protocol[U]):
    def hits(self) -> int: ...
    def misses(self) -> int: ...
    def get_by_url(self, *, url: U) -> Optional[CacheEntry]: ...
    def get(self, *, digest: ContentDigest) -> Optional[CacheEntry]: ...
    def try_fetch(
        self,
        url: U,
        fetcher: "Callable[[U], Iterable[bytes]]",
        force_refetch: "bool | ContentDigest",
    ) -> "CacheEntry | FetchInterrupted[U] | ContentUnavailable":
        ...

    def fetch(
        self,
        url: U,
        fetcher: "Callable[[U], Iterable[bytes]]",
        force_refetch: "bool | ContentDigest" = False,
        retries: int = 3
    ) -> "CacheEntry":
        for _ in range(retries):
            result = self.try_fetch(url, fetcher, force_refetch=force_refetch)
            if isinstance(result, CacheEntry):
                return result
            if isinstance(result, FetchInterrupted):
                continue
            if isinstance(result, Exception): # pyright: ignore[reportUnnecessaryIsInstance]
                raise result
        raise RuntimeError("Number of retries exhausted")

from .disk_cache import DiskCache as DiskCache
from .memory_cache import MemoryCache as MemoryCache
from .noop_cache import NoopCache as NoopCache
