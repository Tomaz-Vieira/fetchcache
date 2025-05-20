#!/usr/bin/env python

from pathlib import Path
import tempfile
from concurrent.futures import Future, ProcessPoolExecutor
from typing_extensions import List
import logging
import secrets

from tests import HitsAndMisses, download_all_payloads_simultaneously_via_disk_cache, start_test_server

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logging.basicConfig()
    # import genericache
    # logging.getLogger(genericache.__name__).setLevel(logging.DEBUG)

    server_port = 8123 # FIXME: allocate a free one
    payloads = [secrets.token_bytes(4096 * 5) for _ in range(10)]
    server_proc = start_test_server(payloads, server_port=server_port)
    try:
        pp = ProcessPoolExecutor(max_workers=len(payloads))

        cache_dir = tempfile.TemporaryDirectory(suffix="_cache")
        logger.debug(f"Cache dir: {cache_dir.name}")
        hits_and_misses_futs: "List[Future[HitsAndMisses]]" = [
            pp.submit(
                download_all_payloads_simultaneously_via_disk_cache,
                process_idx=process_idx,
                payloads=payloads,
                server_port=server_port,
                cache_dir=Path(cache_dir.name),
            )
            for process_idx in range(10)
        ]
        misses = sum(f.result().misses for f in hits_and_misses_futs)
        assert misses == len(payloads)
    finally:
        server_proc.terminate()
