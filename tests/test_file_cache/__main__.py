#!/usr/bin/env python

from hashlib import sha256
from pathlib import Path
import tempfile
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from typing_extensions import List
import logging
import secrets

from genericache.digest import ContentDigest
from genericache.disk_cache import DiskCache
from tests import HitsAndMisses, HttpxFetcher, dl_and_check, random_range, start_test_server, hash_url

logger = logging.getLogger(__name__)



def process_target_do_downloads(
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
        url_hasher=hash_url,
    )

    pool = ThreadPoolExecutor(max_workers=10)
    payload_indices = random_range(seed=process_idx, len=payloads.__len__())
    futs = [
        pool.submit(dl_and_check, server_port=server_port, cache=cache, idx=idx)
        for idx in payload_indices
    ]
    _ = [f.result() for f in futs]

    reader_digest = cache.fetch(f"http://localhost:{server_port}/0")
    assert not isinstance(reader_digest, Exception)
    (reader, digest) = reader_digest

    computed_digest = ContentDigest(digest=sha256(reader.read()).digest())
    assert digest == computed_digest
    cached_reader = cache.get(digest=digest)
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

        for use_symlinks in (True, False):
            cache_dir = tempfile.TemporaryDirectory(suffix="_cache")
            logger.debug(f"Cache dir: {cache_dir.name}")
            hits_and_misses_futs: "List[Future[HitsAndMisses]]" = [
                pp.submit(
                    process_target_do_downloads,
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
