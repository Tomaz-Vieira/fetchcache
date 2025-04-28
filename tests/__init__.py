from hashlib import sha256
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import multiprocessing
from typing import Final, Iterable, List, Type
import random
import time
import logging
from dataclasses import dataclass

import httpx

from genericache import Cache
from genericache.digest import ContentDigest, UrlDigest

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

def make_http_handler_class(
    payloads: List[bytes],
    chunk_len: int,
) -> Type[BaseHTTPRequestHandler]:
    class HttpHandler(BaseHTTPRequestHandler):
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
                sleep_time = random.random() * 0.5
                logger.debug(f"Sent {start}:{end} of {self.path}. Will sleep for {sleep_time:.2f}")
                time.sleep(sleep_time)


    return HttpHandler

@dataclass
class HitsAndMisses:
    hits: int
    misses: int

def dl_and_check(cache: Cache[str], server_port: int, idx: int):
    res = cache.fetch(f"http://localhost:{server_port}/{idx}")
    assert not isinstance(res, Exception)
    (reader, digest) = res
    assert ContentDigest(sha256(reader.read()).digest()) == digest

def _do_start_test_server(
    *,
    http_handler_class: Type[BaseHTTPRequestHandler],
    server_port: int,
):
    server_address = ('', server_port)
    httpd = ThreadingHTTPServer(server_address, http_handler_class)
    httpd.serve_forever()

def start_test_server(
    payloads: List[bytes],
    server_port: int,
    http_handler_class: "Type[BaseHTTPRequestHandler] | None" = None,
) -> multiprocessing.Process:
    http_handler_class = http_handler_class or make_http_handler_class(
        payloads,
        chunk_len=4096,
    )
    server_proc = multiprocessing.Process(
        target=_do_start_test_server,
        kwargs={"http_handler_class": http_handler_class, "server_port": server_port}
    )
    server_proc.start()

    for _ in range(10):
        try:
            _ = httpx.head(f"http://localhost:{server_port}/0")
            break
        except Exception:
            logger.debug("Dummy server is not ready yet", )
            pass
        time.sleep(0.05)
    else:
        raise RuntimeError("Dummy server did not become ready")
    return server_proc
