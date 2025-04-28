from concurrent.futures import Future, ThreadPoolExecutor
import tempfile
from typing import List
from genericache.memory_cache import MemoryCache
from tests import HitsAndMisses, HttpxFetcher, hash_url
import secrets
import logging

from tests import random_range, start_test_server, dl_and_check

def download_everything(*, thread_idx: int, cache: MemoryCache[str], payloads: List[bytes]):
    pool = ThreadPoolExecutor(max_workers=payloads.__len__())
    payload_indices = random_range(seed=thread_idx, len=payloads.__len__())
    futs = [
        pool.submit(dl_and_check, server_port=server_port, cache=cache, idx=idx)
        for idx in payload_indices
    ]
    _ = [f.result() for f in futs]
    

if __name__ == "__main__":
    logging.basicConfig()
    logger = logging.getLogger(__name__)

    server_port = 8123 # FIXME: allocate a free one
    payloads = [secrets.token_bytes(4096 * 5) for _ in range(10)]
    server_proc = start_test_server(payloads, server_port=server_port)
    try:
        cache = MemoryCache(
            fetcher=HttpxFetcher(),
            url_hasher=hash_url,
        )
        pp = ThreadPoolExecutor(max_workers=len(payloads))

        cache_dir = tempfile.TemporaryDirectory(suffix="_cache")
        logger.debug(f"Cache dir: {cache_dir.name}")
        hits_and_misses_futs: "List[Future[HitsAndMisses]]" = [
            pp.submit(
                download_with_many_threads,
                process_idx=process_idx,
                payloads=payloads,
                server_port=server_port,
                cache=cache,
            )
            for process_idx in range(10)
        ]
        misses = sum(f.result().misses for f in hits_and_misses_futs)
        assert misses == len(payloads)
    finally:
        server_proc.terminate()
