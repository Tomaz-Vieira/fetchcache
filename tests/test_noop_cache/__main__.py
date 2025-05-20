
from concurrent.futures import Future, ThreadPoolExecutor
from typing import List
from genericache import NoopCache
from tests import HttpxFetcher, hash_url
import secrets
import logging

from tests import random_range, start_test_server, dl_and_check

if __name__ == "__main__":
    logging.basicConfig()
    logger = logging.getLogger(__name__)

    import genericache.memory_cache as mc
    logging.getLogger(mc.__name__).setLevel(logging.DEBUG)

    server_port = 8125 # FIXME: allocate a free one
    payloads = [secrets.token_bytes(4096 * 5) for _ in range(10)]
    server_proc = start_test_server(payloads, server_port=server_port)
    fetcher = HttpxFetcher()
    try:
        cache: NoopCache[str] = NoopCache(url_hasher=hash_url)
        pool = ThreadPoolExecutor(max_workers=payloads.__len__())
        num_dl_groups = 13
        futs: "List[Future[None]]" = []
        for i in range(num_dl_groups):
            payload_indices = random_range(seed=i, len=payloads.__len__())
            futs += [
                pool.submit(dl_and_check, server_port=server_port, fetcher=fetcher, cache=cache, idx=idx)
                for idx in payload_indices
            ]
        a = [f.result() for f in futs]
        logger.debug(f"misses: {cache.misses()} hits: {cache.hits()}")
        assert cache.misses() == num_dl_groups * payloads.__len__()
    finally:
        server_proc.terminate()
