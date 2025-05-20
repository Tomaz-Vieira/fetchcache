from datetime import datetime
from hashlib import sha256
from typing import Callable, Iterable, Optional, TypeVar
from io import BytesIO
import logging

from genericache import Cache, CacheEntry, FetchInterrupted
from genericache.digest import ContentDigest, UrlDigest

logger = logging.getLogger(__name__)

U = TypeVar("U")
class NoopCache(Cache[U]):

    def __init__(self, *, url_hasher: "Callable[[U], UrlDigest]"):
        super().__init__()
        self._misses: int = 0
        self.url_hasher = url_hasher

    def hits(self) -> int:
        return 0

    def misses(self) -> int:
        return self._misses

    def get_by_url(self, *, url: U) -> Optional[CacheEntry]:
        return None

    def get(self, *, digest: ContentDigest) -> Optional[CacheEntry]:
        return None

    def try_fetch(
        self,
        url: U,
        fetcher: Callable[[U], Iterable[bytes]],
        force_refetch: "bool | ContentDigest",
    ) -> "CacheEntry | FetchInterrupted[U]":
        self._misses += 1
        try:
            chunks = fetcher(url)
            contents = bytearray()
            contents_sha = sha256()
            for chunk in chunks:
                contents.extend(chunk)
                contents_sha.update(chunk)
            return CacheEntry(
                reader=BytesIO(contents),
                url_digest=self.url_hasher(url),
                content_digest=ContentDigest(digest=contents_sha.digest()),
                timestamp=datetime.now(),
            )
        except Exception as e:
            return FetchInterrupted(url=url).with_traceback(e.__traceback__)
        



