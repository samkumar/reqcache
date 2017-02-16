LRU Cache with Request Management
=================================
This implements an LRU cache for a I/O resource. On a cache miss, the cache
requests the resource, blocking until it is available. The cache has the
property that if multiple requests are concurrently made for a resource that is
not in the cache, then only one request for the resource is made, and all
requests block until it is available.

Because this library is designed for caching resources that require an I/O to
fetch, it is fully concurrent. Fetches are made without holding any locks,
meaning that:

1. While one operation is blocking due to a cache miss, additional operations
on the cache will not block.
2. Multiple fetches for resources can be concurrently pending.
