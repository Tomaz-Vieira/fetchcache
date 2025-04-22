#!/usr/bin/env python

from pathlib import Path
import multiprocessing
import time
import secrets
import tempfile
import random
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from http import HTTPStatus
from typing import Final, Iterable, Tuple
from typing_extensions import List
import logging

import httpx

from fetchcache import DiskCache
from fetchcache.digest import ContentDigest

logger = logging.getLogger(__name__)


PAYLOADS = [secrets.token_bytes(4096 * 5) for _ in range(10)]
HASHES = [sha256(payload) for payload in PAYLOADS]

CACHE_DIR = tempfile.TemporaryDirectory(suffix="_cache")
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

def download_stuff(process_idx: int) -> Tuple[int, int]:
    cache = DiskCache.create(Path(CACHE_DIR.name), fetcher=HttpxFetcher())
    assert not isinstance(cache, Exception)

    def dl_and_check(idx: int):
        res = cache.download(f"http://localhost:{SERVER_PORT}/{idx}")
        assert not isinstance(res, Exception)
        (reader, digest) = res
        assert ContentDigest(sha256(reader.read()).digest()) == digest

    tp = ThreadPoolExecutor(max_workers=10)
    rng = random.Random()
    rng.seed(process_idx)
    payload_indices = sorted(range(PAYLOADS.__len__()), key=lambda _: rng.random())
    _ = list(tp.map(dl_and_check, payload_indices))

    reader_digest = cache.download(f"http://localhost:{SERVER_PORT}/0")
    assert not isinstance(reader_digest, Exception)
    (reader, digest) = reader_digest

    computed_digest = ContentDigest(digest=sha256(reader.read()).digest())
    assert digest == computed_digest
    cached_reader = cache.get(digest=digest)
    assert cached_reader is not None
    assert ContentDigest(digest=sha256(cached_reader.read()).digest()) == computed_digest

    return (cache.hits(), cache.misses())

def start_dummy_server() -> multiprocessing.Process:
    def do_start_server(*, server_port: int):
        server_address = ('', server_port)
        httpd = ThreadingHTTPServer(server_address, HttpHandler)
        httpd.serve_forever()

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

if __name__ == "__main__":
    logging.basicConfig()
    # import fetchcache
    # logging.getLogger(fetchcace.__name__).setLevel(logging.DEBUG)

    server_proc = start_dummy_server()
    try:
        pp = ProcessPoolExecutor(max_workers=10)
        hits_and_misses: List[Tuple[int, int]] = list(pp.map(download_stuff, range(10)))
        misses = sum(hnm[1] for hnm in hits_and_misses)
        assert misses == PAYLOADS.__len__()
    finally:
        server_proc.terminate()
