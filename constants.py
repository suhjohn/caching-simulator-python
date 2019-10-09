from typing import List

import caches

BLOOM_FILTER_M = 100000
BLOOM_FILTER_K = 3

# 1000 bytes = 1 kb
# 1mb = 1000 kb
CACHE_CAPACITY = 1000 * 1000
CACHE_CLASSES: List[caches.Cache] = [
    caches.FIFOCache,
    caches.LRUCache,
]

