# Genericache

A thread-safe, process-safe cache for slow fetching operations, like web requests.

## Usage

```python
    from genericache import DiskCache
    from genericache.digest import UrlDigest
    from pathlib import Path
    from typing import Iterable
    from hashlib import sha256

    def my_fetch(url: str) -> Iterable[bytes]:
        import httpx
        return httpx.get(url).raise_for_status().iter_bytes(4096)

    def url_hasher(url: str) -> UrlDigest:
        return UrlDigest.from_str(url)

    cache = DiskCache[str].create(
        url_type=str,
        cache_dir=Path("/tmp/my_cache"),
        url_hasher=url_hasher,
    )

    reader, contents_digest = cache.fetch(
        "https://www.ilastik.org/documentation/pixelclassification/snapshots/training2.png",
        fetcher=my_fetch,
    )
    assert sha256(reader.read()).digest() == contents_digest.digest
    assert cache.hits() == 0
    assert cache.misses() == 1

    reader, contents_digest = cache.fetch(
        "https://www.ilastik.org/documentation/pixelclassification/snapshots/training2.png",
        fetcher=my_fetch,
    )
    assert cache.hits() == 1
    assert cache.misses() == 1
```

## Testing

Run the modules inside `tests/`, e.g.:

`uv run --python 3.11 python -m tests.test_disk_cache`

You can run them all with something like

```bash
    find tests/ -name "__main__.py" | \
    tr '/' '.' | sed 's@\.__main__.py@@' | \
    xargs -t -I test_mod_name \
        uv run --python 3.12 python -m test_mod_name
```

