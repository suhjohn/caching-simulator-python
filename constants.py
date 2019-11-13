import caches

BLOOM_FILTER_M = 100000
BLOOM_FILTER_K = 3

# 1000 bytes = 1 kb
# 1mb = 1000 kb
CACHE_CAPACITY = 100000 * 5
CACHE_CLASSES = [
    caches.LRUCache,
]