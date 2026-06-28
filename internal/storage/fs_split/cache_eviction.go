package fs_split

type evictionRing struct {
	front  *cacheEntry
	back   *cacheEntry
	length int
}

func (ring *evictionRing) pushFront(entry *cacheEntry) {
	entry.previous = nil
	entry.next = ring.front
	if ring.front == nil {
		ring.back = entry
	} else {
		ring.front.previous = entry
	}
	ring.front = entry
	ring.length++
}

func (ring *evictionRing) moveToFront(entry *cacheEntry) {
	if ring.front == entry {
		return
	}
	ring.remove(entry)
	ring.pushFront(entry)
}

func (ring *evictionRing) remove(entry *cacheEntry) {
	if entry.previous == nil {
		ring.front = entry.next
	} else {
		entry.previous.next = entry.next
	}
	if entry.next == nil {
		ring.back = entry.previous
	} else {
		entry.next.previous = entry.previous
	}
	entry.previous = nil
	entry.next = nil
	ring.length--
}
