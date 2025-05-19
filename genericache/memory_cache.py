from hashlib import sha256
from datetime import datetime
from threading import Event, Lock
from concurrent.futures import Future
from typing import Callable, Dict, Iterable, List, Optional, Tuple, TypeVar, Final
from io import BytesIO
import logging

from genericache import Cache, FetchInterrupted, CacheEntry
from genericache.digest import ContentDigest, UrlDigest

logger = logging.getLogger(__name__)


U = TypeVar("U")

class _EntryBytes:
    def __init__(
        self,
        contents: bytearray,
        contents_digest: ContentDigest,
        url_digest: UrlDigest,
        timestamp: datetime,
    ) -> None:
        super().__init__()
        self.contents: Final[bytearray] = contents
        self.content_digest: Final[ContentDigest] = contents_digest
        self.url_digest: Final[UrlDigest] = url_digest
        self.timestamp = timestamp

    def open(self) -> CacheEntry:
        return CacheEntry(
            content_digest=self.content_digest,
            reader=BytesIO(self.contents),
            timestamp=self.timestamp,
            url_digest=self.url_digest,
        )

class MemoryCache(Cache[U]):
    url_hasher: Final[Callable[[U], UrlDigest]]
        
    def __init__(
        self,
        *,
        url_hasher: Callable[[U], UrlDigest],
    ):
        super().__init__()
        self.url_hasher = url_hasher
        self._cache_lock: Final[Lock] = Lock()
        self._url_locks: Dict[UrlDigest, Event] = {}
        self._downloads_by_url: Dict[UrlDigest, List[_EntryBytes]] = {}
        self._downloads_by_content: Dict[ContentDigest, "_EntryBytes"] = {}
        self._hits: int = 0
        self._misses: int = 0

    def hits(self) -> int:
        return self._hits

    def misses(self) -> int:
        return self._misses

    def get_by_url(self, *, url: U) -> Optional[CacheEntry]:
        url_digest = self.url_hasher(url)
        out: "None | _EntryBytes" = None

        with self._cache_lock:
            
        
        for entry_path in self.dir_path.iterdir():
            entry = _EntryPath.try_from_path(entry_path)
            if not entry:
                continue
            if entry.url_digest != url_digest:
                continue
            if not out:
                out = entry
            elif entry.timestamp > out.timestamp:
                out = entry
        if not out:
            return None
        return out.open()
        url_digest = self.url_hasher(url)
        with self._cache_lock:
            dl = self._downloads_by_url.get(url_digest)
        if not dl:
            return None
        result = dl.result()
        if isinstance(result, Exception):
            return None
        return result.open()

    def get(self, *, digest: ContentDigest) -> Optional[CacheEntry]:
        with self._cache_lock:
            result = self._downloads_by_content.get(digest)
        if result is None:
            return None
        return result.open()

    def try_fetch(self, url: U, fetcher: Callable[[U], Iterable[bytes]]) -> "CacheEntry | FetchInterrupted[U]":
        url_digest = self.url_hasher(url)

        _ = self._cache_lock.acquire() # <<<<<<<<<
        dl_fut = self._downloads_by_url.get(url_digest)
        if dl_fut: # some other thread is downloading it
            self._cache_lock.release() # >>>>>>>>>>
            result = dl_fut.result()
            if isinstance(result, Exception):
                return result

            self._hits += 1
            return (BytesIO(result.contents), result.content_digest)
        else:
            self._misses += 1
            dl_fut = self._downloads_by_url[url_digest] = Future()
            _ = dl_fut.set_running_or_notify_cancel() # we still hold the lock, so fut._condition is insta-acquired
            self._cache_lock.release() # >>>>>>>>>

        try:
            contents = bytearray()
            contents_sha  = sha256()
            for chunk in fetcher(url):
                contents_sha.update(chunk)
                contents.extend(chunk)
            content_digest = ContentDigest(digest=contents_sha.digest())
            result = _EntryBytes(contents=contents, contents_digest=content_digest)
            with self._cache_lock:
                self._downloads_by_content[content_digest] = result
            dl_fut.set_result(result)
            return result.open()
        except Exception as e:
            with self._cache_lock:
                del self._downloads_by_url[url_digest] # remove Future before set_result so failures can be retried
            error = FetchInterrupted(url=url).with_traceback(e.__traceback__)
            dl_fut.set_result(error)
            return error


