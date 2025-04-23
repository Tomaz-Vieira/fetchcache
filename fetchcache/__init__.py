import shutil
from pathlib import Path
import threading
from typing import BinaryIO, Callable, Dict, Final, Generic, Iterable, Optional, Tuple, TypeVar
from filelock import FileLock
from hashlib import sha256
import tempfile
import os
import logging

from .digest import ContentDigest, UrlDigest

logger = logging.getLogger(__name__)


U = TypeVar("U")

class DownloadCacheException(Exception):
    pass

class FetchInterrupted(DownloadCacheException, Generic[U]):
    def __init__(self, *, url: U) -> None:
        self.url = url
        super().__init__(f"Downloading of '{url}' was interrupted")


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

    def open(self) -> "Tuple[BinaryIO, ContentDigest]":
        return (open(self.path, "rb"), self.content_digest)

class _CacheEntrySymlinkBase:
    path: Final[Path]
    def __init__(self, path: Path) -> None:
        super().__init__()
        self.path = path

    def exists(self) -> bool:
        return self.path.exists()

    def link(self, *, src: _CacheEntryPath):
        logger.info(f"Linking src {src.path} as {self.path}")
        os.symlink(src=src.path, dst=self.path)

    def readlink(self) -> _CacheEntryPath:
        raw_entry_path = Path(os.readlink(self.path))
        entry_path = _CacheEntryPath.try_from_path(raw_entry_path)
        if entry_path is None:
            raise RuntimeError(f"Cache entry not found at {raw_entry_path}")
        return entry_path

    def try_open(self) -> "Tuple[BinaryIO, ContentDigest] | None":
        if not self.path.exists():
            return None
        return self.readlink().open()


class _UrlHashSymlinkPath(_CacheEntrySymlinkBase):
    def __init__(self, digest: UrlDigest, *, cache_dir: Path) -> None:
        super().__init__(cache_dir / f"url_{digest}")

class _ContentHashSymlinkPath(_CacheEntrySymlinkBase):
    def __init__(self, digest: ContentDigest, *, cache_dir: Path) -> None:
        super().__init__(cache_dir / f"content_{digest}")


class DiskCache(Generic[U]):
    def __init__(
        self,
        *,
        cache_dir: Path,
        fetcher: Callable[[U], Iterable[bytes]],
        url_hasher: Callable[[U], UrlDigest],
        use_symlinks: bool = True,
    ):
        # FileLock is reentrant, so multiple threads would be able to acquire the lock without a threading Lock
        self._ongoing_downloads_lock: Final[threading.Lock] = threading.Lock()
        self._ongoing_downloads: Dict[UrlDigest, threading.Event] = {}

        self._hits = 0
        self._misses = 0

        self.dir_path: Final[Path] = cache_dir
        self.fetcher: Final[ Callable[[U], Iterable[bytes]] ] = fetcher
        self.url_hasher: Final[ Callable[[U], UrlDigest] ] = url_hasher
        self.use_symlinks: Final[bool] = use_symlinks
        super().__init__()

    def hits(self) -> int:
        return self._hits

    def misses(self) -> int:
        return self._misses

    def _contents_path(self, *, sha: str) -> Path: #FIXME: use HASH type?
        return self.dir_path / sha

    def get_by_url(self, *, url: U) -> Optional[Tuple[BinaryIO, ContentDigest]]:
        url_digest = self.url_hasher(url)
        if self.use_symlinks and os.name == "posix":
            url_symlink = _UrlHashSymlinkPath(url_digest, cache_dir=self.dir_path)
            return url_symlink.try_open()

        for entry_path in self.dir_path.iterdir():
            entry = _CacheEntryPath.try_from_path(entry_path)
            if entry and entry.url_digest == url_digest:
                return entry.open()
        return None

    def get(self, *, digest: ContentDigest) -> Optional[BinaryIO]:
        if self.use_symlinks and os.name == "posix":
            content_symlink = _ContentHashSymlinkPath(digest, cache_dir=self.dir_path)
            reader_digest = content_symlink.try_open()
            if reader_digest is None:
                return None
            return reader_digest[0]

        for entry_path in self.dir_path.iterdir():
            entry = _CacheEntryPath.try_from_path(entry_path)
            if entry and entry.content_digest == digest:
                return open(entry_path, "rb")
        return None

    def try_fetch(self, url: U) -> "Tuple[BinaryIO, ContentDigest] | FetchInterrupted[U]": #FIXME: URL class?
        url_digest = self.url_hasher(url)
        interproc_lock = FileLock(self.dir_path / f"downloading_url_{url_digest}.lock")
        url_symlink_path = _UrlHashSymlinkPath(cache_dir=self.dir_path, digest=url_digest)

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
                chunks = self.fetcher(url)
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
                if self.use_symlinks and os.name == "posix":
                    url_symlink_path.link(src=cache_entry_path)
                    contents_symlink_path = _ContentHashSymlinkPath(cache_dir=self.dir_path, digest=content_digest)
                    contents_symlink_path.link(src=cache_entry_path)
            except Exception:
                with self._ongoing_downloads_lock:
                    del self._ongoing_downloads[url_digest] # remove the Event so this download can be retried
                raise
            finally:
                dl_event.set() # notify threads that download is done. It'll have failed if file is not there
                logger.debug(f"pid{os.getpid()}:tid{threading.get_ident()} RELEASES the file lock for {interproc_lock.lock_file}")
            return cache_entry_path.open()

    def fetch(self, url: U, retries: int = 3) -> "Tuple[BinaryIO, ContentDigest]":
        for _ in range(retries):
            result = self.try_fetch(url)
            if not isinstance(result, FetchInterrupted):
                return result
        raise RuntimeError("Number of retries exhausted")
