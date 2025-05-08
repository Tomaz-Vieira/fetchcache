from collections.abc import Iterable
from hashlib import sha256
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any, Callable, ClassVar, Dict, Final, Optional, Tuple, Type, TypeVar

from filelock import FileLock
from genericache import BytesReader, Cache, CacheFsLinkUsageMismatch, FetchInterrupted, CacheUrlTypeMismatch
from genericache.digest import ContentDigest, UrlDigest
import logging
import threading

logger = logging.getLogger(__name__)


class _CacheEntryPath:
    """The file path used inside the cache directory

    The file name encodes both the sha of the URL as well as the sha of the contents
    so that an entry can be searched by either of them.

    Because Windows doesn't allow symlinks out of "developer mode", all digests must be
    encoded into the file name itself, and a file has to be found by iterating over the
    directory entries
    """

    PREFIX = "entry__url_"
    INFIX = "_contents_"

    def __init__(self, url_digest: UrlDigest, content_digest: ContentDigest, *, cache_dir: Path) -> None:
        super().__init__()
        self.url_digest: Final[UrlDigest] = url_digest
        self.content_digest: Final[ContentDigest] = content_digest
        self.path: Final[Path] = cache_dir / f"{self.PREFIX}{self.url_digest}{self.INFIX}{content_digest}"

    @classmethod
    def try_from_path(cls, path: Path) -> "Optional[_CacheEntryPath]":
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
        return _CacheEntryPath(
            cache_dir=path.parent,
            url_digest=UrlDigest.parse(hexdigest=url_hexdigest),
            content_digest=ContentDigest.parse(hexdigest=contents_hexdigest),
        )

    def open(self) -> "Tuple[BytesReader, ContentDigest]":
        return (open(self.path, "rb"), self.content_digest)


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
        self._ongoing_downloads_lock: Final[threading.Lock] = threading.Lock()
        self._ongoing_downloads: Dict[UrlDigest, threading.Event] = {}

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
        use_symlinks: bool = True,
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
        use_symlinks: bool = True,
    ) -> "DiskCache[U]":
        out = cls.try_create(
            url_type=url_type, cache_dir=cache_dir, url_hasher=url_hasher, use_symlinks=use_symlinks
        )
        if isinstance(out, Exception):
            raise out
        return out

    def hits(self) -> int:
        return self._hits

    def misses(self) -> int:
        return self._misses

    def get_by_url(self, *, url: U) -> Optional[Tuple[BytesReader, ContentDigest]]:
        url_digest = self.url_hasher(url)
        for entry_path in self.dir_path.iterdir():
            entry = _CacheEntryPath.try_from_path(entry_path)
            if entry and entry.url_digest == url_digest:
                return entry.open()
        return None

    def get(self, *, digest: ContentDigest) -> Optional[BytesReader]:
        for entry_path in self.dir_path.iterdir():
            entry = _CacheEntryPath.try_from_path(entry_path)
            if entry and entry.content_digest == digest:
                return open(entry_path, "rb")
        return None

    def try_fetch(self, url: U, fetcher: "Callable[[U], Iterable[bytes]]") -> "Tuple[BytesReader, ContentDigest] | FetchInterrupted[U]":
        url_digest = self.url_hasher(url)
        interproc_lock = FileLock(self.dir_path / f"downloading_url_{url_digest}.lock")

        _ = self._ongoing_downloads_lock.acquire() # <<<<<<<<<
        dl_event = self._ongoing_downloads.get(url_digest)
        if dl_event: # some other thread is downloading it
            self._ongoing_downloads_lock.release() # >>>>>>>
            _ = dl_event.wait()
            out = self.get_by_url(url=url)
            if out is None:
                return FetchInterrupted(url=url)
            else:
                self._hits += 1
                return out
        else:
            dl_event = self._ongoing_downloads[url_digest] = threading.Event() # this thread will download it
            self._ongoing_downloads_lock.release() # >>>>>>

        with interproc_lock:
            logger.debug(f"pid{os.getpid()}:tid{threading.get_ident()} gets the file lock for {interproc_lock.lock_file}")
            try:
                out = self.get_by_url(url=url)
                if out is not None: # some other process already downloaded it
                    logger.debug(f"pid{os.getpid()}:{threading.get_ident()} uses CACHED file {interproc_lock.lock_file}")
                    self._hits += 1
                    return out

                self._misses += 1
                chunks = fetcher(url)
                temp_file = tempfile.NamedTemporaryFile(delete=False)
                contents_sha  = sha256()
                for chunk in chunks:
                    contents_sha.update(chunk)
                    _ = temp_file.write(chunk) #FIXME: check num bytes written?
                temp_file.close()
                content_digest = ContentDigest(digest=contents_sha.digest())

                cache_entry_path = _CacheEntryPath(url_digest, content_digest, cache_dir=self.dir_path)
                logger.debug(f"Moving temp file to {cache_entry_path.path}")
                _ = shutil.move(src=temp_file.name, dst=cache_entry_path.path)
            except Exception:
                with self._ongoing_downloads_lock:
                    del self._ongoing_downloads[url_digest] # remove the Event so this download can be retried
                raise
            finally:
                dl_event.set() # notify threads that download is done. It'll have failed if file is not there
                logger.debug(f"pid{os.getpid()}:tid{threading.get_ident()} RELEASES the file lock for {interproc_lock.lock_file}")
            return cache_entry_path.open()

    def fetch(self, url: U, fetcher: "Callable[[U], Iterable[bytes]]", retries: int = 3) -> "Tuple[BytesReader, ContentDigest]":
        for _ in range(retries):
            result = self.try_fetch(url, fetcher)
            if not isinstance(result, FetchInterrupted):
                return result
        raise RuntimeError("Number of retries exhausted")
