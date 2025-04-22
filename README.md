# Fetchcache

A thread-safe, process-safe cache for slow fetching operations, like web requests.

## Usage

```python
    import httpx
    from fetchcache import DiskCache
    from pathlib import Path
    from typing import Final, Iterable

    class MyFetcher:
      def __init__(self) -> None:
          super().__init__()
          self._client: Final[httpx.Client] = httpx.Client()

      def __call__(self, url: str) -> Iterable[bytes]:
          return self._client.get(url).raise_for_status().iter_bytes(4096)


    cache = DiskCache(cache_dir=Path("/tmp/my_cache"), fetcher=MyFetcher())

    result = cache.fetch("https://www.ilastik.org/documentation/pixelclassification/snapshots/training2.png")
    assert not isinstance(result, Exception)
    reader = result[0]
    data: bytes = reader.read()
    print(f"Got {data.__len__()} bytes")
```
