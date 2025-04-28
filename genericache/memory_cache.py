from hashlib import sha256
from threading import Lock
from concurrent.futures import Future
from typing import BinaryIO, Callable, Dict, Iterable, Optional, Tuple, TypeVar, Final
from io import BytesIO
import logging

from genericache import Cache, FetchInterrupted
from genericache.digest import ContentDigest, UrlDigest

logger = logging.getLogger(__name__)


U = TypeVar("U")

class _CacheEntry:
    def __init__(self, contents: bytearray, digest: ContentDigest) -> None:
        super().__init__()
        self.contents: Final[bytearray] = contents
        self.digest: Final[ContentDigest] = digest

    def open(self) -> Tuple[BinaryIO, ContentDigest]:
        return (BytesIO(self.contents), self.digest)

class MemoryCache(Cache[U]):
    fetcher: Final[Callable[[U], Iterable[bytes]]]
    url_hasher: Final[Callable[[U], UrlDigest]]
        
    def __init__(
        self,
        *,
        fetcher: Callable[[U], Iterable[bytes]],
        url_hasher: Callable[[U], UrlDigest],
    ):
        super().__init__()
        self.fetcher = fetcher
        self.url_hasher = url_hasher
        self._downloads_lock: Final[Lock] = Lock()
        self._downloads_by_url: Dict[UrlDigest, Future["_CacheEntry | FetchInterrupted[U]"]] = {}
        self._downloads_by_content: Dict[ContentDigest, "_CacheEntry"] = {}
        self._hits: int = 0
        self._misses: int = 0

    def hits(self) -> int:
        return self._hits

    def misses(self) -> int:
        return self._misses

    def get_by_url(self, *, url: U) -> Optional[Tuple[BinaryIO, ContentDigest]]:
        url_digest = self.url_hasher(url)
        with self._downloads_lock:
            dl = self._downloads_by_url.get(url_digest)
        if not dl:
            return None
        result = dl.result()
        if isinstance(result, Exception):
            return None
        return result.open()

    def get(self, *, digest: ContentDigest) -> Optional[BinaryIO]:
        with self._downloads_lock:
            result = self._downloads_by_content.get(digest)
        if result is None:
            return None
        return result.open()[0]

    def try_fetch(self, url: U) -> "Tuple[BinaryIO, ContentDigest] | FetchInterrupted[U]":
        url_digest = self.url_hasher(url)

        _ = self._downloads_lock.acquire() # <<<<<<<<<
        dl_fut = self._downloads_by_url.get(url_digest)
        if dl_fut: # some other thread is downloading it
            self._downloads_lock.release() # >>>>>>>>>>
            result = dl_fut.result()
            if isinstance(result, Exception):
                return result

            self._hits += 1
            return (BytesIO(result.contents), result.digest)
        else:
            self._misses += 1
            dl_fut = self._downloads_by_url[url_digest] = Future()
            _ = dl_fut.set_running_or_notify_cancel() # we still hold the lock, so fut._condition is insta-acquired
            self._downloads_lock.release() # >>>>>>>>>

        try:
            contents = bytearray()
            contents_sha  = sha256()
            for chunk in self.fetcher(url):
                contents_sha.update(chunk)
                contents.extend(chunk)
            content_digest = ContentDigest(digest=contents_sha.digest())
            result = _CacheEntry(contents=contents, digest=content_digest)
            with self._downloads_lock:
                self._downloads_by_content[content_digest] = result
            dl_fut.set_result(result)
            return result.open()
        except Exception as e:
            with self._downloads_lock:
                del self._downloads_by_url[url_digest] # remove Future before set_result so failures can be retried
            error = FetchInterrupted(url=url).with_traceback(e.__traceback__)
            dl_fut.set_result(error)
            return error


