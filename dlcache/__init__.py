import shutil
from pathlib import Path
import threading
from typing import BinaryIO, Dict, Final, Optional, Tuple
from filelock import FileLock
import httpx
from hashlib import sha256
import tempfile
import os
import logging

logger = logging.getLogger()

class DownloadCacheException(Exception):
    pass

class DownloadInterrupted(DownloadCacheException):
    def __init__(self, *, url: str) -> None:
        super().__init__(f"Downloading of {url} was interrupted")

class Digest:
    def __init__(self, *, digest: str) -> None:
        super().__init__()
        self.digest: Final[str] = digest

    def __eq__(self, value: object, /) -> bool:
        if isinstance(value, Digest):
            return self.digest == value.digest
        if isinstance(value, sha256().__class__):
            return self.digest == value.hexdigest()
        return False


class DiskDownloadCache:
    class __PrivateMarker:
        pass


    def __init__(
        self,
        *,
        dir_path: Path,
        _private_marker: __PrivateMarker, # pyright: ignore []
    ):
        # FileLock is reentrant, so multiple threads would be able to acquire the lock without a threading Lock
        self._ongoing_downloads_lock: Final[threading.Lock] = threading.Lock()
        self._ongoing_downloads: Dict[str, threading.Event] = {}

        self._hits = 0
        self._misses = 0

        self.dir_path: Final[Path] = dir_path
        self._client: Final[httpx.Client] = httpx.Client()
        super().__init__()

    def hits(self) -> int:
        return self._hits

    def misses(self) -> int:
        return self._misses

    @classmethod
    def create(cls, dir_path: Path) -> "DiskDownloadCache | DownloadCacheException":
        # FIXME: test writable?
        return DiskDownloadCache(dir_path=dir_path, _private_marker=cls.__PrivateMarker())

    def _contents_path(self, *, sha: str) -> Path: #FIXME: use HASH type?
        return self.dir_path / sha

    def download(self, url: str) -> "Tuple[BinaryIO, Digest] | DownloadInterrupted": #FIXME: URL class?
        url_sha = sha256(url.encode("utf8")).hexdigest()
        interproc_lock = FileLock(self.dir_path / f"downloading_url_{url_sha}.lock")
        url_symlink = self.dir_path / f"url_{url_sha}.contents"

        def open_cached_file() -> Tuple[BinaryIO, Digest]:
            return (
                open(url_symlink, "rb"),
                Digest(digest=Path(os.readlink(url_symlink)).name)
            )

        _ = self._ongoing_downloads_lock.acquire() # <<<<<<<<<
        dl_event = self._ongoing_downloads.get(url)
        if dl_event: # some other thread is downloading it
            self._ongoing_downloads_lock.release() # >>>>>>>
            _ = dl_event.wait()
            if url_symlink.exists():
                self._hits += 1
                return open_cached_file()
            else:
                return DownloadInterrupted(url=url)
        else:
            dl_event = self._ongoing_downloads[url] = threading.Event() # this thread will download it
            self._ongoing_downloads_lock.release() # >>>>>>

        with interproc_lock:
            logger.debug(f"pid{os.getpid()}:tid{threading.get_ident()} gets the file lock for {interproc_lock.lock_file}")
            try:
                if url_symlink.exists(): # some other process already downloaded it
                    logger.debug(f"pid{os.getpid()}:{threading.get_ident()} uses CACHED file {interproc_lock.lock_file}")
                    self._hits += 1
                    return open_cached_file()

                self._misses += 1
                resp = httpx.get(url).raise_for_status()
                temp_file = tempfile.NamedTemporaryFile(delete=False)
                contents_sha  = sha256()
                for chunk in resp.iter_bytes(chunk_size=4096):
                    contents_sha.update(chunk)
                    _ = temp_file.write(chunk) #FIXME: check num bytes written?
                temp_file.close()

                contents_path = self._contents_path(sha=contents_sha.hexdigest())
                logger.info(f"Moving temp file to {contents_path}")
                _ = shutil.move(src=temp_file.file.name, dst=contents_path)
                logger.info(f"Linking src {contents_path} as {url_symlink}")
                os.symlink(src=contents_path, dst=url_symlink)
            except Exception:
                with self._ongoing_downloads_lock:
                    del self._ongoing_downloads[url] # remove the Event so this download can be retried
                raise
            finally:
                dl_event.set() # notify threads that download is done. It'll have failed if file is not there
                logger.debug(f"pid{os.getpid()}:tid{threading.get_ident()} RELEASES the file lock for {interproc_lock.lock_file}")
            return open_cached_file()

    def get_cached(self, digest: Digest) -> Optional[BinaryIO]:
        for entry in self.dir_path.iterdir():
            if digest.digest == entry.name:
                return open(entry, "rb")
        else:
            return None
