from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import multiprocessing
from pathlib import Path
from typing import Any, Callable, Final, Iterable, List
import random
import time
import logging
from dataclasses import dataclass

import httpx

from genericache import Cache
from genericache.digest import ContentDigest, UrlDigest
from genericache.disk_cache import DiskCache

logger = logging.getLogger(__name__)


class HttpxFetcher:
    def __init__(self) -> None:
        super().__init__()
        self._client: Final[httpx.Client] = httpx.Client()

    def __call__(self, url: str) -> Iterable[bytes]:
        return self._client.get(url).raise_for_status().iter_bytes(4096)


def hash_url(url: str) -> UrlDigest:
    return UrlDigest.from_str(url)


def random_range(*, seed: int, len: int) -> List[int]:
    rng = random.Random()
    rng.seed(seed)
    return sorted(range(len), key=lambda _: rng.random())


@dataclass
class HitsAndMisses:
    hits: int
    misses: int


def dl_and_check(
    cache: Cache[str],
    fetcher: Callable[[str], Iterable[bytes]],
    server_port: int,
    idx: int,
):
    res = cache.fetch(
        f"http://localhost:{server_port}/{idx}", fetcher=fetcher, force_refetch=False
    )
    assert ContentDigest(sha256(res.read()).digest()) == res.content_digest


def download_all_payloads_simultaneously_via_disk_cache(
    process_idx: int,
    server_port: int,
    payloads: List[bytes],
    cache_dir: Path,
) -> HitsAndMisses:
    cache = DiskCache[str].create(
        url_type=str,
        cache_dir=cache_dir,
        url_hasher=hash_url,
    )
    fetcher = HttpxFetcher()
    pool = ThreadPoolExecutor(max_workers=payloads.__len__())
    payload_indices = random_range(seed=process_idx, len=payloads.__len__())
    futs = [
        pool.submit(
            cache.fetch, url=f"http://localhost:{server_port}/{idx}", fetcher=fetcher
        )
        for idx in payload_indices
    ]
    _ = [f.result() for f in futs]

    cache_entry = cache.fetch(
        f"http://localhost:{server_port}/{payload_indices[0]}",
        fetcher=fetcher,
        force_refetch=False,
    )

    computed_digest = ContentDigest(digest=sha256(cache_entry.read()).digest())
    assert cache_entry.content_digest == computed_digest

    cached_reader = cache.get(digest=cache_entry.content_digest)
    assert cached_reader is not None
    assert (
        ContentDigest(digest=sha256(cached_reader.read()).digest()) == computed_digest
    )

    return HitsAndMisses(hits=cache.hits(), misses=cache.misses())


def _do_start_test_server(
    *,
    payloads: List[bytes],
    chunk_len: int,
    server_port: int,
):
    class HttpHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            pass

        def do_GET(self):
            payload_index = int(self.path.strip("/"))
            payload = payloads[payload_index]

            self.send_response(HTTPStatus.OK)
            self.send_header("Content-type", "application/octet-stream")
            self.send_header("Content-Length", str(payload.__len__()))
            self.end_headers()

            data_len = len(payload)
            for start in range(0, len(payload), chunk_len):
                end = min(start + chunk_len, data_len)
                sent_bytes = self.wfile.write(payload[start:end])
                assert sent_bytes == end - start
                # sleep_time = random.random() * 0.5
                # logger.debug(f"Sent {start}:{end} of {self.path}. Will sleep for {sleep_time:.2f}")
                # time.sleep(sleep_time)

    server_address = ("", server_port)
    httpd = ThreadingHTTPServer(server_address, HttpHandler)
    httpd.serve_forever()


def start_test_server(
    payloads: List[bytes],
    server_port: int,
) -> multiprocessing.Process:
    server_proc = multiprocessing.Process(
        target=_do_start_test_server,
        kwargs={"server_port": server_port, "payloads": payloads, "chunk_len": 4096},
    )
    server_proc.start()

    for _ in range(10):
        try:
            _ = httpx.head(f"http://localhost:{server_port}/0")
            break
        except Exception:
            logger.debug(
                "Dummy server is not ready yet",
            )
            pass
        time.sleep(0.05)
    else:
        raise RuntimeError("Dummy server did not become ready")
    return server_proc
