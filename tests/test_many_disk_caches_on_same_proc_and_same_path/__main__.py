from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
import tempfile
from genericache import CacheUrlTypeMismatch
from genericache.digest import UrlDigest
from genericache.disk_cache import DiskCache
from tests import hash_url
from typing import List


if __name__ == "__main__":
    cache_dir = tempfile.TemporaryDirectory(suffix="_disk_cache")
    cache1 = DiskCache[str].try_create(
        cache_dir=Path(cache_dir.name),
        url_hasher=hash_url,
        url_type=str,
        use_symlinks=True,
    )
    assert not isinstance(cache1, Exception)

    cache2 = DiskCache[str].try_create(
        cache_dir=Path(cache_dir.name),
        url_hasher=hash_url,
        url_type=str,
        use_symlinks=True,
    )
    assert not isinstance(cache2, Exception)
    assert cache1 is cache2

    failed_cache_creation1 = DiskCache[bytes].try_create(
        cache_dir=Path(cache_dir.name),
        url_hasher=lambda url: UrlDigest(digest=url),
        url_type=bytes,
        use_symlinks=True,
    )
    assert isinstance(failed_cache_creation1, CacheUrlTypeMismatch)

    tpe = ThreadPoolExecutor(max_workers=10)
    futs: "List[Future[DiskCache[str] | Exception]]" = []
    for _ in range(10):
        futs.append(tpe.submit(
            DiskCache[str].try_create,
            cache_dir=Path(cache_dir.name),
            url_hasher=hash_url,
            url_type=str,
            use_symlinks=True,
        ))
    cache_instances = [f.result() for f in futs]
    for c in cache_instances:
        assert isinstance(c, DiskCache)
        for c2 in cache_instances:
            assert c is c2
