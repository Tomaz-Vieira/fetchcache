from hashlib import sha256
from typing import Callable, Iterable, Optional, Tuple, TypeVar
from io import BytesIO
import logging

from genericache import BytesReader, Cache, FetchInterrupted
from genericache.digest import ContentDigest

logger = logging.getLogger(__name__)

U = TypeVar("U")
class NoopCache(Cache[U]):
    def __init__(self):
        super().__init__()
        self._misses: int = 0

    def hits(self) -> int:
        return 0

    def misses(self) -> int:
        return self._misses

    def get_by_url(self, *, url: U) -> Optional[Tuple[BytesReader, ContentDigest]]:
        return None

    def get(self, *, digest: ContentDigest) -> Optional[BytesReader]:
        return None

    def try_fetch(self, url: U, fetcher: Callable[[U], Iterable[bytes]]) -> "Tuple[BytesReader, ContentDigest] | FetchInterrupted[U]":
        self._misses += 1
        chunks = fetcher(url)
        contents = bytearray()
        contents_sha = sha256()
        for chunk in chunks:
            contents.extend(chunk)
            contents_sha.update(chunk)
        return (BytesIO(contents), ContentDigest(digest=contents_sha.digest()))



