#!/usr/bin/env python

from hashlib import sha256
from pathlib import Path
import tempfile
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from typing_extensions import List
import logging
import secrets

from genericache import ContentDigest, DiskCache
from tests import HitsAndMisses, HttpxFetcher, random_range, start_test_server, hash_url

logger = logging.getLogger(__name__)


fetcher = HttpxFetcher()

def download_all_payloads_simultaneously(
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
    pool = ThreadPoolExecutor(max_workers=payloads.__len__())
    payload_indices = random_range(seed=process_idx, len=payloads.__len__())
    futs = [
        pool.submit(cache.fetch, url=f"http://localhost:{server_port}/{idx}", fetcher=fetcher)
        for idx in payload_indices
    ]
    _ = [f.result() for f in futs]

    cache_entry = cache.fetch(f"http://localhost:{server_port}/{payload_indices[0]}", fetcher=fetcher, force_refetch=False)

    computed_digest = ContentDigest(digest=sha256(cache_entry.read()).digest())
    assert cache_entry.content_digest == computed_digest

    cached_reader = cache.get(digest=cache_entry.content_digest)
    assert cached_reader is not None
    assert ContentDigest(digest=sha256(cached_reader.read()).digest()) == computed_digest

    return HitsAndMisses(hits=cache.hits(), misses=cache.misses())


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
                download_all_payloads_simultaneously,
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
