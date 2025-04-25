#!/usr/bin/env python

from pathlib import Path
import multiprocessing
import time
import tempfile
from concurrent.futures import Future, ProcessPoolExecutor
from http.server import ThreadingHTTPServer
from typing_extensions import List
import logging
import secrets

import httpx

from genericache.disk_cache import DiskCache
from tests import HitsAndMisses, HttpxFetcher, download_with_many_threads, make_http_handler_class, url_hasher

logger = logging.getLogger(__name__)


def do_start_server(*, server_port: int, payloads: List[bytes]):
    server_address = ('', server_port)
    http_handler_class = make_http_handler_class(
        payloads,
        chunk_len=4096,
    )
    httpd = ThreadingHTTPServer(server_address, http_handler_class)
    httpd.serve_forever()

def start_dummy_server(payloads: List[bytes], server_port: int) -> multiprocessing.Process:

    server_proc = multiprocessing.Process(
        target=do_start_server,
        kwargs={"server_port": server_port, "payloads": payloads}
    )
    server_proc.start()

    for _ in range(10):
        try:
            _ = httpx.head(f"http://localhost:{server_port}/0")
            break
        except Exception:
            logger.debug("Dummy server is not ready yet", )
            pass
        time.sleep(0.1)
    else:
        raise RuntimeError("Dummy server did not become ready")
    return server_proc

def do_download(
    process_idx: int,
    server_port: int,
    payloads: List[bytes],
    cache_dir: Path,
    use_symlinks: bool,
) -> HitsAndMisses:
    cache=DiskCache(
        cache_dir=cache_dir,
        use_symlinks=use_symlinks,
        fetcher=HttpxFetcher(),
        url_hasher=url_hasher,
    )
    return download_with_many_threads(
        payloads=payloads,
        cache=cache,
        process_idx=process_idx,
        server_port=server_port,
    )
    

if __name__ == "__main__":
    logging.basicConfig()
    # import genericache
    # logging.getLogger(genericache.__name__).setLevel(logging.DEBUG)

    server_port = 8123 # DIXME: allocate a free one
    payloads = [secrets.token_bytes(4096 * 5) for _ in range(10)]
    server_proc = start_dummy_server(payloads, server_port=server_port)
    try:
        pp = ProcessPoolExecutor(max_workers=len(payloads))

        for use_symlinks in (True, False):
            cache_dir = tempfile.TemporaryDirectory(suffix="_cache")
            logger.debug(f"Cache dir: {cache_dir.name}")
            hits_and_misses_futs: "List[Future[HitsAndMisses]]" = [
                pp.submit(
                    do_download,
                    process_idx=process_idx,
                    payloads=payloads,
                    server_port=server_port,
                    use_symlinks=use_symlinks,
                    cache_dir=Path(cache_dir.name),
                )
                for process_idx in range(10)
            ]
            misses = sum(f.result().misses for f in hits_and_misses_futs)
            assert misses == len(payloads)
    finally:
        server_proc.terminate()
