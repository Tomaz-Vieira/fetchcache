# Genericache

A thread-safe, process-safe cache for slow fetching operations, like web requests.

## Usage

```python
    from genericache import DiskCache, UrlDigest
    from pathlib import Path
    from typing import Iterable
    from hashlib import sha256

    def my_fetch(url: str) -> Iterable[bytes]:
        import httpx
        return httpx.get(url).raise_for_status().iter_bytes(4096)

    def url_hasher(url: str) -> UrlDigest:
        return UrlDigest.from_str(url)

    cache = DiskCache(
        cache_dir=Path("/tmp/my_cache"),
        fetcher=my_fetch,
        url_hasher=url_hasher,
    )

    reader, contents_digest = cache.fetch("https://www.ilastik.org/documentation/pixelclassification/snapshots/training2.png")
    assert sha256(reader.read()).digest() == contents_digest.digest
```
