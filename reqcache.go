/*
 * Copyright (c) 2017 Sam Kumar <samkumar@berkeley.edu>
 * Copyright (c) 2017 University of California, Berkeley
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the University of California, Berkeley nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNERS OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

// Package reqcache provides an LRU cache with request management. See the
// Github page for more information.
package reqcache

import (
	"container/list"
	"sync"
)

// LRUCache represents a Cache with an LRU eviction policy
type LRUCache struct {
	cache    map[interface{}]*LRUCacheEntry
	fetch    func(key interface{}) (interface{}, uint64, error)
	onEvict  func(key, value interface{})
	lruList  *list.List
	size     uint64
	capacity uint64

	// Lock ordering is to always acquire the cacheLock before the lruLock
	cacheLock *sync.Mutex
	lruLock   *sync.Mutex
}

// LRUCacheEntry represents an entry in the LRU Cache. The size of this struct
// is the overhead of an entry existing in the cache. You should not have to
// actually use it.
type LRUCacheEntry struct {
	key     interface{}
	value   interface{}
	size    uint64
	pending bool
	err     error
	ready   *sync.Cond
	element *list.Element
}

// NewLRUCache returns a new instance of LRUCache.
// capacity is the capacity of the cache. If the sum of the sizes of elements in
// the cache exceeds the capacity, the least recently used elements are evicted
// from the cache.
// fetch is a function that is called on cache misses to fetch the element that
// is missing in the cache. The key that missed in the cache is passed as the
// argument. The function should return the corresponding value, and the size
// of the result (used to make sure that the total size does not exceed the
// cache's capacity). It can also return an error, in which case the result is
// not cached and the error is propagated to callers of Get(). No locks are
// held when fetch is called, so it is suitable to do blocking operations to
// fetch data.
// onEvict is a function that is whenever an element is evicted from the cache
// according to the LRU replacement policy. It is called with the key and value
// of the evicted element passed as arguments. It is not called with locks held,
// so it can perform blocking operations or even interact with this cache. It
// can be set to nil if the onEvict callback is not needed.
func NewLRUCache(capacity uint64, fetch func(key interface{}) (interface{}, uint64, error), onEvict func(key, value interface{})) *LRUCache {
	return &LRUCache{
		cache:     make(map[interface{}]*LRUCacheEntry),
		fetch:     fetch,
		onEvict:   onEvict,
		lruList:   list.New(),
		capacity:  capacity,
		cacheLock: &sync.Mutex{},
		lruLock:   &sync.Mutex{},
	}
}

// The cacheLock, but not the lruLock, must be held when this function executes.
func (lruc *LRUCache) addEntryToLRU(entry *LRUCacheEntry) []*LRUCacheEntry {
	lruc.lruLock.Lock()
	defer lruc.lruLock.Unlock()

	entry.element = lruc.lruList.PushFront(entry)
	lruc.size += entry.size
	return lruc.evictEntriesIfNecessary()
}

// The cacheLock and lruLock must both be held when this function executes.
func (lruc *LRUCache) evictEntriesIfNecessary() []*LRUCacheEntry {
	pruned := []*LRUCacheEntry{}
	for lruc.size > lruc.capacity {
		element := lruc.lruList.Back()
		lruc.lruList.Remove(element)
		entry := element.Value.(*LRUCacheEntry)
		delete(lruc.cache, entry.key)
		lruc.size -= entry.size
		pruned = append(pruned, entry)
	}
	return pruned
}

// Calls the onEvict callback for a list of evicted entries. Should be called
// without the cacheLock or lruLock acquired.
func (lruc *LRUCache) callOnEvict(evicted []*LRUCacheEntry) {
	if lruc.onEvict != nil {
		for _, entry := range evicted {
			lruc.onEvict(entry.key, entry.value)
		}
	}
}

// SetCapacity sets the capacity of the cache, evicting elements if necessary.
func (lruc *LRUCache) SetCapacity(capacity uint64) {
	lruc.cacheLock.Lock()
	lruc.lruLock.Lock()
	lruc.capacity = capacity
	evicted := lruc.evictEntriesIfNecessary()
	lruc.lruLock.Unlock()
	lruc.cacheLock.Unlock()
	lruc.callOnEvict(evicted)
}

// Get returns the value corresponding to the specialized key, caching the
// result. Returns an error if and only if there was a cache miss and the
// provided fetch() function returned an error. If Put() is called while
// a fetch is blocking, then the result of the fetch is thrown away and the
// value specified by Put() is returned.
func (lruc *LRUCache) Get(key interface{}) (interface{}, error) {
	lruc.cacheLock.Lock()
	entry, ok := lruc.cache[key]
	if ok {
		/* Wait for the result if it's still pending. */
		for entry.pending {
			entry.ready.Wait()
		}
		if entry.err != nil {
			/* There was an error fetching this value. */
			return nil, entry.err
		}
		/* Cache hit. */
		lruc.lruLock.Lock()
		lruc.lruList.MoveToFront(entry.element)
		lruc.lruLock.Unlock()
		value := entry.value
		lruc.cacheLock.Unlock()
		return value, nil
	}

	/* Cache miss. Create placeholder. */
	entry = &LRUCacheEntry{
		key:     key,
		pending: true,
		err:     nil,
		ready:   sync.NewCond(lruc.cacheLock),
	}
	lruc.cache[key] = entry
	lruc.cacheLock.Unlock()

	/* Fetch the value. */
	value, size, err := lruc.fetch(key)

	/*
	 * If the pending flag is no longer set, then someone called Put()
	 * meanwhile. We don't want to touch the cache or use the new value we
	 * got; instead, just use the value that was put there.
	 */
	if entry.pending {
		/* Check for and handle any error in fetching the value. */
		if err != nil {
			lruc.cacheLock.Lock()
			delete(lruc.cache, key)
			entry.err = err
			entry.pending = false
			entry.ready.Broadcast()
			lruc.cacheLock.Unlock()
			return nil, err
		}

		/* Store the result in the cache. */
		lruc.cacheLock.Lock()
		entry.value = value
		entry.size = size
		entry.pending = false
		entry.ready.Broadcast()
		evicted := lruc.addEntryToLRU(entry)
		lruc.cacheLock.Unlock()

		lruc.callOnEvict(evicted)
	}

	return entry.value, nil
}

// Put an entry with a known value into the cache.
func (lruc *LRUCache) Put(key interface{}, value interface{}, size uint64) bool {
	lruc.cacheLock.Lock()
	entry, ok := lruc.cache[key]

	/* Check for case where it's already in the cache. */
	if ok {
		var evicted []*LRUCacheEntry
		entry.value = value

		/* If the entry is still pending, wake up any waiting threads. */
		if entry.pending {
			entry.pending = false
			entry.ready.Broadcast()

			/* Add to the LRU list. */
			evicted = lruc.addEntryToLRU(entry)
		}
		lruc.cacheLock.Unlock()

		if evicted != nil {
			lruc.callOnEvict(evicted)
		}

		return true
	}

	/* Put it in the cache. */
	entry = &LRUCacheEntry{
		key:     key,
		value:   value,
		size:    size,
		pending: false,
		err:     nil,
		ready:   sync.NewCond(lruc.cacheLock),
	}
	lruc.cache[key] = entry
	evicted := lruc.addEntryToLRU(entry)
	lruc.cacheLock.Unlock()

	lruc.callOnEvict(evicted)

	return false
}

// Evict an entry from the cache.
func (lruc *LRUCache) Evict(key interface{}) bool {
	lruc.cacheLock.Lock()
	entry, ok := lruc.cache[key]
	if !ok {
		lruc.cacheLock.Unlock()
		return false
	}
	lruc.lruLock.Lock()
	lruc.lruList.Remove(entry.element)
	delete(lruc.cache, entry.key)
	lruc.size -= entry.size
	lruc.lruLock.Unlock()
	lruc.cacheLock.Unlock()
	return true
}

// Invalidate empties the cache, calling the onEvict callback as appropriate.
func (lruc *LRUCache) Invalidate() {
	lruc.cacheLock.Lock()
	lruc.lruLock.Lock()
	oldCapacity := lruc.capacity

	lruc.capacity = 0
	entries := lruc.evictEntriesIfNecessary()

	lruc.capacity = oldCapacity
	lruc.lruLock.Unlock()
	lruc.cacheLock.Unlock()

	lruc.callOnEvict(entries)
}
