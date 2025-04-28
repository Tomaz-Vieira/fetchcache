from concurrent.futures import ThreadPoolExecutor
from genericache.memory_cache import MemoryCache
from tests import HttpxFetcher, hash_url
import secrets
import logging

from tests import random_range, start_test_server, dl_and_check

if __name__ == "__main__":
    logging.basicConfig()
    logger = logging.getLogger(__name__)

    server_port = 8124 # FIXME: allocate a free one
    payloads = [secrets.token_bytes(4096 * 5) for _ in range(10)]
    server_proc = start_test_server(payloads, server_port=server_port)
    try:
        cache = MemoryCache(
            fetcher=HttpxFetcher(),
            url_hasher=hash_url,
        )
        pool = ThreadPoolExecutor(max_workers=payloads.__len__())
        payload_indices = random_range(seed=0, len=payloads.__len__())
        futs = [
            pool.submit(dl_and_check, server_port=server_port, cache=cache, idx=idx)
            for idx in payload_indices
        ]
        _ = [f.result() for f in futs]
        print(f"misses: {cache.misses()} hits: {cache.hits()}")
        assert cache.misses() == payloads.__len__()
    finally:
        server_proc.terminate()
