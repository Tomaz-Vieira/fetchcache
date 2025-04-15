import shutil
from pathlib import Path
import threading
from typing import BinaryIO, Dict, Final, Tuple
from filelock import FileLock
import httpx
from hashlib import sha256
import tempfile
import os

class DownloadCacheException(Exception):
    pass

class DownloadInterrupted(DownloadCacheException):
    def __init__(self, *, url: str) -> None:
        super().__init__(f"Downloading of {url} was interrupted")

class DlCache:
    class __PrivateMarker:
        pass


    def __init__(
        self,
        *,
        dir_path: Path,
        __private_marker: __PrivateMarker, # pyright: ignore []
    ):
        # FileLock is reentrant, so multiple threads would be able to acquire the lock without a threading Lock
        self._ongoing_downloads_lock: Final[threading.Lock] = threading.Lock()
        self._ongoing_downloads: Dict[str, threading.Event] = {}


        self.dir_path: Final[Path] = dir_path
        self._client: Final[httpx.Client] = httpx.Client()
        super().__init__()

    @classmethod
    def create(cls, dir_path: Path) -> "DlCache | DownloadCacheException":
        # FIXME: test writable?
        return DlCache(dir_path=dir_path, __private_marker=cls.__PrivateMarker())

    def _contents_path(self, *, sha: str) -> Path: #FIXME: use HASH type?
        return self.dir_path / sha

    def download(self, url: str) -> "Tuple[BinaryIO, str] | DownloadInterrupted": #FIXME: URL class?
        url_sha = sha256(url.encode("utf8"))
        interproc_lock = FileLock(self.dir_path / f"downloading_url_{url_sha}.lock")
        url_symlink = self.dir_path / f"url_{url_sha.hexdigest()}.contents"

        def open_cached_file() -> Tuple[BinaryIO, str]:
            return (open(url_symlink, "rb"), Path(os.readlink(url_symlink)).name)

        _ = self._ongoing_downloads_lock.acquire() # <<<<<<<<<
        dl_event = self._ongoing_downloads.get(url)
        if dl_event: # some other thread is downloading it
            self._ongoing_downloads_lock.release() # >>>>>>>
            _ = dl_event.wait()
            if url_symlink.exists():
                return open_cached_file()
            else:
                return DownloadInterrupted(url=url)
        else:
            dl_event = self._ongoing_downloads[url] = threading.Event() # this thread will download it
            self._ongoing_downloads_lock.release() # >>>>>>

        with interproc_lock:
            try:
                if url_symlink.exists(): # some other process already downloaded it
                    return open_cached_file()

                resp = httpx.get(url).raise_for_status()
                temp_file = tempfile.NamedTemporaryFile(delete=False)
                contents_sha  = sha256()
                for chunk in resp.iter_bytes(chunk_size=4096):
                    contents_sha.update(chunk)
                    _ = temp_file.write(chunk) #FIXME: check num bytes written?
                temp_file.close()

                _ = shutil.move(src=temp_file.file.name, dst=self._contents_path(sha=contents_sha.hexdigest()))
                os.symlink(src=self._contents_path(sha=contents_sha.hexdigest()), dst=url_symlink)
            except Exception:
                with self._ongoing_downloads_lock:
                    del self._ongoing_downloads[url] # remove the Event so this download can be retried
                raise
            finally:
                dl_event.set() # notify threads that download is done. It'll have failed if file is not there
            return open_cached_file()

