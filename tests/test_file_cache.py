#!/usr/bin/env python

from pathlib import Path
import multiprocessing
import time
import secrets
import tempfile
import random
from concurrent.futures import Future, ThreadPoolExecutor, ProcessPoolExecutor
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from http import HTTPStatus
from typing import Final, Iterable, Tuple
from typing_extensions import List
import logging

import httpx

from genericache import DiskCache, UrlDigest
from genericache.digest import ContentDigest

logger = logging.getLogger(__name__)


PAYLOADS = [secrets.token_bytes(4096 * 5) for _ in range(10)]
HASHES = [sha256(payload) for payload in PAYLOADS]

SERVER_PORT=8123 #FIXME: get a free port

class HttpHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        payload_index = int(self.path.strip("/"))
        payload = PAYLOADS[payload_index]

        self.send_response(HTTPStatus.OK)
        self.send_header("Content-type", "application/octet-stream")
        self.send_header("Content-Length", str(payload.__len__()))
        self.end_headers()

        piece_len = 4096
        data_len = len(payload)
        for start in range(0, len(payload), piece_len):
            end = min(start + piece_len, data_len)
            sent_bytes = self.wfile.write(payload[start:end])
            assert sent_bytes == end - start
            sleep_time = random.random() * 0.5
            logger.debug(f"Sent {start}:{end} of {self.path}. Will sleep for {sleep_time:.2f}")
            time.sleep(sleep_time)

def download_with_many_threads(process_idx: int, cache_dir: Path, use_symlinks: bool) -> Tuple[int, int]:
    cache = DiskCache(
        cache_dir=cache_dir,
        fetcher=HttpxFetcher(),
        url_hasher=url_hasher,
        use_symlinks=use_symlinks,
    )
    assert not isinstance(cache, Exception)

    def dl_and_check(idx: int):
        res = cache.fetch(f"http://localhost:{SERVER_PORT}/{idx}")
        assert not isinstance(res, Exception)
        (reader, digest) = res
        assert ContentDigest(sha256(reader.read()).digest()) == digest

    tp = ThreadPoolExecutor(max_workers=10)
    rng = random.Random()
    rng.seed(process_idx)
    payload_indices = sorted(range(PAYLOADS.__len__()), key=lambda _: rng.random())
    _ = list(tp.map(dl_and_check, payload_indices))

    reader_digest = cache.fetch(f"http://localhost:{SERVER_PORT}/0")
    assert not isinstance(reader_digest, Exception)
    (reader, digest) = reader_digest

    computed_digest = ContentDigest(digest=sha256(reader.read()).digest())
    assert digest == computed_digest
    cached_reader = cache.get(digest=digest)
    assert cached_reader is not None
    assert ContentDigest(digest=sha256(cached_reader.read()).digest()) == computed_digest

    return (cache.hits(), cache.misses())

def do_start_server(*, server_port: int):
    server_address = ('', server_port)
    httpd = ThreadingHTTPServer(server_address, HttpHandler)
    httpd.serve_forever()

def start_dummy_server() -> multiprocessing.Process:

    server_proc = multiprocessing.Process(
        target=do_start_server,
        kwargs={"server_port": SERVER_PORT}
    )
    server_proc.start()

    for _ in range(10):
        try:
            _ = httpx.head(f"http://localhost:{SERVER_PORT}/0")
            break
        except Exception:
            logger.debug("Dummy server is not ready yet", )
            pass
        time.sleep(0.1)
    else:
        raise RuntimeError("Dummy server did not become ready")
    return server_proc

class HttpxFetcher:
    def __init__(self) -> None:
        super().__init__()
        self._client: Final[httpx.Client] = httpx.Client()

    def __call__(self, url: str) -> Iterable[bytes]:
        return self._client.get(url).raise_for_status().iter_bytes(4096)

def url_hasher(url: str) -> UrlDigest:
    return UrlDigest.from_str(url)

if __name__ == "__main__":
    logging.basicConfig()
    # import genericache
    # logging.getLogger(fetchcace.__name__).setLevel(logging.DEBUG)

    server_proc = start_dummy_server()
    try:
        pp = ProcessPoolExecutor(max_workers=len(PAYLOADS))

        for use_symlinks in (True, False):
            cache_dir = tempfile.TemporaryDirectory(suffix="_cache")
            hits_and_misses_futs: "List[Future[Tuple[int, int]]]" = [
                pp.submit(
                    download_with_many_threads,
                    process_idx=process_idx,
                    cache_dir=Path(cache_dir.name),
                    use_symlinks=use_symlinks
                )
                for process_idx in range(10)
            ]
            misses = sum(f.result()[1] for f in hits_and_misses_futs)
            assert misses == len(PAYLOADS)
    finally:
        server_proc.terminate()
