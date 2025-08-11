import logging
from concurrent.futures import Future
from datetime import datetime
from hashlib import sha256
from io import BytesIO
from threading import Lock
from typing import Callable, Dict, Final, Iterable, Optional, TypeVar

from genericache import Cache, CacheEntry, DigestMismatch, FetchInterrupted
from genericache.digest import ContentDigest, UrlDigest

logger = logging.getLogger(__name__)


U = TypeVar("U")


class _EntryData:
    def __init__(
        self,
        url_digest: UrlDigest,
        content_digest: ContentDigest,
        contents: bytearray,
        timestamp: datetime,
    ) -> None:
        super().__init__()
        self.contents: Final[bytearray] = contents
        self.content_digest: Final[ContentDigest] = content_digest
        self.url_digest: Final[UrlDigest] = url_digest
        self.timestamp: Final[datetime] = timestamp

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
        self._instance_lock: Final[Lock] = Lock()
        self._downloads_by_url: Dict[
            UrlDigest, Future["_EntryData | FetchInterrupted[U]"]
        ] = {}
        self._downloads_by_content: Dict[ContentDigest, "_EntryData"] = {}
        self._hits: int = 0
        self._misses: int = 0

    def hits(self) -> int:
        return self._hits

    def misses(self) -> int:
        return self._misses

    def get_by_url(self, *, url: U) -> Optional[CacheEntry]:
        url_digest = self.url_hasher(url)
        with self._instance_lock:
            dl = self._downloads_by_url.get(url_digest)
        if not dl:
            return None
        result = dl.result()
        if isinstance(result, Exception):
            return None
        return result.open()

    def get(self, *, digest: ContentDigest) -> Optional[CacheEntry]:
        with self._instance_lock:
            result = self._downloads_by_content.get(digest)
        if result is None:
            return None
        return result.open()

    def try_fetch(
        self,
        url: U,
        fetcher: Callable[[U], Iterable[bytes]],
        force_refetch: "bool | ContentDigest",
    ) -> "CacheEntry | FetchInterrupted[U] | DigestMismatch[U]":
        url_digest = self.url_hasher(url)

        _ = self._instance_lock.acquire()  # <<<<<<<<<
        dl_fut = self._downloads_by_url.get(url_digest)
        if dl_fut:  # some other thread IN THIS PROCESS is downloading it
            self._instance_lock.release()  # >>>>>>>
            result = dl_fut.result()
            if isinstance(result, Exception):
                return result
            self._hits += 1
            if (
                isinstance(force_refetch, ContentDigest)
                and result.content_digest != force_refetch
            ):
                return DigestMismatch(
                    url=url,
                    expected_content_digest=force_refetch,
                    actual_content_digest=result.content_digest,
                )
            return result.open()

        self._misses += 1
        dl_fut = self._downloads_by_url[url_digest] = Future()
        _ = (
            dl_fut.set_running_or_notify_cancel()
        )  # we still hold the lock, so fut._condition is insta-acquired
        self._instance_lock.release()  # >>>>>>>>>

        try:
            contents = bytearray()
            contents_sha = sha256()
            for chunk in fetcher(url):
                contents_sha.update(chunk)
                contents.extend(chunk)
            content_digest = ContentDigest(digest=contents_sha.digest())
            entry_data = _EntryData(
                url_digest, content_digest, contents, datetime.now()
            )
            with self._instance_lock:
                self._downloads_by_content[content_digest] = entry_data
            dl_fut.set_result(entry_data)
        except Exception as e:
            with self._instance_lock:
                # remove Future before set_result so failures can be retried
                del self._downloads_by_url[url_digest]

            error = FetchInterrupted(url=url).with_traceback(e.__traceback__)
            dl_fut.set_result(error)
            return error
        if (
            isinstance(force_refetch, ContentDigest)
            and entry_data.content_digest != force_refetch
        ):
            return DigestMismatch(
                url=url,
                expected_content_digest=force_refetch,
                actual_content_digest=entry_data.content_digest,
            )
        return entry_data.open()
