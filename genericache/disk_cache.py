from concurrent.futures import Future
from datetime import datetime
from collections.abc import Iterable
from hashlib import sha256
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any, Callable, ClassVar, Dict, Final, Optional, Tuple, Type, TypeVar

from filelock import FileLock
from genericache import Cache, CacheEntry, CacheFsLinkUsageMismatch, ContentUnavailable, FetchInterrupted, CacheUrlTypeMismatch
from genericache.digest import ContentDigest, UrlDigest
import logging
import threading

logger = logging.getLogger(__name__)


class _EntryPath:
    """The file path used inside the cache directory

    The file name encodes both the sha of the URL as well as the sha of the contents
    so that an entry can be searched by either of them.

    Because Windows doesn't allow symlinks out of "developer mode", all digests must be
    encoded into the file name itself, and a file has to be found by iterating over the
    directory entries
    """

    PREFIX = "entry__url_"
    INFIX = "_contents_"

    def __init__(
        self,
        url_digest: UrlDigest,
        content_digest: ContentDigest,
        *,
        cache_dir: Path,
        timestamp: datetime,
    ) -> None:
        super().__init__()
        self.url_digest: Final[UrlDigest] = url_digest
        self.content_digest: Final[ContentDigest] = content_digest
        self.timestamp: Final[datetime] = timestamp
        self.path: Final[Path] = cache_dir / f"{self.PREFIX}{self.url_digest}{self.INFIX}{content_digest}"

    @classmethod
    def try_from_path(cls, path: Path) -> "Optional[_EntryPath]":
        name = path.name
        if not name.startswith(cls.PREFIX):
            return None
        name = name[len(cls.PREFIX):]
        urldigest_contentsdigest = name.split(cls.INFIX)
        if urldigest_contentsdigest.__len__() != 2:
            return None
        (url_hexdigest, contents_hexdigest) = urldigest_contentsdigest
        if len(url_hexdigest) != 64 or len(contents_hexdigest) != 64:
            return None
        mtime = os.path.getmtime(path)
        return _EntryPath(
            cache_dir=path.parent,
            url_digest=UrlDigest.parse(hexdigest=url_hexdigest),
            content_digest=ContentDigest.parse(hexdigest=contents_hexdigest),
            timestamp=datetime.fromtimestamp(mtime),
        )

    def open(self) -> CacheEntry:
        return CacheEntry(
            content_digest=self.content_digest,
            reader=open(self.path, "rb"),
            timestamp=self.timestamp,
            url_digest=self.url_digest,
        )


U = TypeVar("U")
class DiskCache(Cache[U]):
    _caches: ClassVar[Dict[ Path, Tuple[Type[Any], "DiskCache[Any]"] ]] = {}
    _caches_lock: ClassVar[threading.Lock] = threading.Lock()

    class __PrivateMarker:
        pass

    def __init__(
        self,
        *,
        cache_dir: Path,
        url_hasher: "Callable[[U], UrlDigest]",
        _private_marker: __PrivateMarker,
    ):
        # FileLock is reentrant, so multiple threads would be able to acquire the lock without a threading Lock
        self._instance_lock: Final[threading.Lock] = threading.Lock()
        self._ongoing_downloads: Dict[UrlDigest, Future["_EntryPath | FetchInterrupted[U]"]] = {}

        self._hits = 0
        self._misses = 0

        self.dir_path: Final[Path] = cache_dir
        self.url_hasher: Final[ Callable[[U], UrlDigest] ] = url_hasher
        super().__init__()

    @classmethod
    def try_create(
        cls,
        *,
        url_type: Type[U],
        cache_dir: Path,
        url_hasher: "Callable[[U], UrlDigest]",
    ) -> "DiskCache[U] | CacheUrlTypeMismatch | CacheFsLinkUsageMismatch":
        with cls._caches_lock:
            url_type_and_entry = cls._caches.get(cache_dir)
            if url_type_and_entry is None:
                cache = DiskCache(
                    cache_dir=cache_dir,
                    url_hasher=url_hasher,
                    _private_marker=cls.__PrivateMarker()
                )
                cls._caches[cache_dir] = (url_type, cache)
                return cache

        entry_url_type, entry = url_type_and_entry
        if entry_url_type is not url_type:
            return CacheUrlTypeMismatch(
                cache_dir=cache_dir,
                expected_url_type=entry_url_type,
                found_url_type=url_type
            )
        return entry

    @classmethod
    def create(
        cls,
        *,
        url_type: Type[U],
        cache_dir: Path,
        url_hasher: "Callable[[U], UrlDigest]",
    ) -> "DiskCache[U]":
        out = cls.try_create(
            url_type=url_type, cache_dir=cache_dir, url_hasher=url_hasher
        )
        if isinstance(out, Exception):
            raise out
        return out

    def hits(self) -> int:
        return self._hits

    def misses(self) -> int:
        return self._misses

    def _get_entry_by_url(self, *, url: U) -> Optional[_EntryPath]:
        url_digest = self.url_hasher(url)
        out: "None | _EntryPath" = None
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
        return out

    def get_by_url(self, *, url: U) -> Optional[CacheEntry]:
        out = self._get_entry_by_url(url=url)
        if not out:
            return None
        return out.open()

    def get(self, *, digest: ContentDigest) -> Optional[CacheEntry]:
        for entry_path in self.dir_path.iterdir():
            entry = _EntryPath.try_from_path(entry_path)
            if entry and entry.content_digest == digest:
                return entry.open()
        return None

    def try_fetch(
        self,
        url: U,
        fetcher: "Callable[[U], Iterable[bytes]]",
        force_refetch: "bool | ContentDigest",
    ) -> "CacheEntry | FetchInterrupted[U] | ContentUnavailable":
        url_digest = self.url_hasher(url)

        _ = self._instance_lock.acquire() # <<<<<<<<<
        dl_fut = self._ongoing_downloads.get(url_digest)
        if dl_fut: # some other thread IN THIS PROCESS is downloading it
            self._instance_lock.release() # >>>>>>>
            result = dl_fut.result()
            if isinstance(result, Exception):
                return result
            self._hits += 1
            if isinstance(force_refetch, ContentDigest) and result.content_digest != force_refetch:
                return ContentUnavailable(content_digest=force_refetch)
            return result.open()

        dl_fut = self._ongoing_downloads[url_digest] = Future() # this thread will download it
        _ = dl_fut.set_running_or_notify_cancel()
        self._instance_lock.release() # >>>>>>

        interproc_lock = FileLock(self.dir_path / f"downloading_url_{url_digest}.lock")
        with interproc_lock:
            logger.debug(f"pid{os.getpid()}:tid{threading.get_ident()} gets the file lock for {interproc_lock.lock_file}")
            try:
                if force_refetch != True:
                    out = self._get_entry_by_url(url=url)
                    if out and (force_refetch is False or out.content_digest == force_refetch):
                        logger.debug(f"pid{os.getpid()}:{threading.get_ident()} uses CACHED file {out.path}")
                        self._hits += 1
                        dl_fut.set_result(out)
                        return out.open()

                self._misses += 1
                chunks = fetcher(url)
                temp_file = tempfile.NamedTemporaryFile(delete=False)
                contents_sha  = sha256()
                for chunk in chunks:
                    contents_sha.update(chunk)
                    _ = temp_file.write(chunk) #FIXME: check num bytes written?
                temp_file.close()
                content_digest = ContentDigest(digest=contents_sha.digest())

                cache_entry_path = _EntryPath(url_digest, content_digest, cache_dir=self.dir_path, timestamp=datetime.now())
                logger.debug(f"Moving temp file to {cache_entry_path.path}")
                _ = shutil.move(src=temp_file.name, dst=cache_entry_path.path)
                dl_fut.set_result(cache_entry_path)
                logger.debug(f"pid{os.getpid()}:tid{threading.get_ident()} RELEASES the file lock for {interproc_lock.lock_file}")
            except Exception as e:
                with self._instance_lock:
                    del self._ongoing_downloads[url_digest] # remove the Event so this download can be retried
                dl_fut.set_result(FetchInterrupted(url=url).with_traceback(e.__traceback__))
                raise
        if isinstance(force_refetch, ContentDigest) and cache_entry_path.content_digest != force_refetch:
            return ContentUnavailable(content_digest=force_refetch)
        return cache_entry_path.open()

