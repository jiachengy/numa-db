#include <glog/logging.h>
#include <cassert>
#include "hashtable.h"
#include <cstring>

#include "atomic.h"
#include "util.h"

using namespace std;

HashTable::HashTable()
{
	capacity_ = 0;
}


HashTable::HashTable(size_t capacity)
{
	capacity_ = capacity;
	entries_ = (Entry*)alloc(capacity * sizeof(Entry));
	memset(entries_, 0, capacity * sizeof(Entry));
}


HashTable::~HashTable()
{
	if (entries_ != NULL)
		dealloc(entries_, capacity_ * sizeof(Entry));
}


val_t HashTable::Get(data_t key)
{
	uint32_t h = hash32(key) % capacity_;
	while (entries_[h].key != 0 && entries_[h].key != key)
		h = (h + 1) % capacity_;

	return entries_[h].val;
}


void HashTable::Put(data_t key, val_t value)
{
	size_++;
	uint32_t h = hash32(key) % capacity_;
	
	while (entries_[h].key != 0 && entries_[h].key != key)
		h = (h + 1) % capacity_;

	entries_[h].key = key;
	entries_[h].val = value;
}


void LocalAggrTable::Aggregate(data_t key, val_t value)
{
	uint32_t h = hash32(key) % capacity_;
	
	while (entries_[h].key != 0 && entries_[h].key != key)
		h = (h + 1) % capacity_;

	if (entries_[h].key == 0)
		entries_[h].key = key;
	entries_[h].val += value;
}

GlobalAggrTable::GlobalAggrTable(size_t capacity)
{
	capacity_ = capacity;
	entries_ = (GlobalEntry*)alloc(capacity * sizeof(GlobalEntry));
	memset(entries_, 0, capacity * sizeof(GlobalEntry));
	for (unsigned int i = 0; i < capacity_; i++)
		mutex_init(&entries_[i].lock);
}


GlobalAggrTable::~GlobalAggrTable()
{
	for (unsigned int i = 0; i < capacity_; i++)
		mutex_destroy(&entries_[i].lock);

	if (entries_ != NULL)
		dealloc(entries_, capacity_ * sizeof(GlobalEntry));
}

val_t GlobalAggrTable::Get(data_t key)
{
	uint32_t h = hash32(key) % capacity_;
	while (entries_[h].key != 0 && entries_[h].key != key)
		h = (h + 1) % capacity_;

	return entries_[h].val;
}

void GlobalAggrTable::Aggregate(data_t key, val_t value)
{
	uint32_t h = hash32(key) % capacity_;
	GlobalEntry *entry = &entries_[h];

	bool done = false;
	if (entry->key == 0) {
		mutex_lock(&entry->lock);
		if (entry->key == 0) {
			entry->key = key;
			entry->val = value;
			done = true;
		}
		mutex_unlock(&entry->lock);
	}
	
	uint32_t first = h;
	while (!done) {
		h = first;

		while (entries_[h].key != 0 && entries_[h].key != key) {
			h = (h + 1) % capacity_;
		}
		
		entry = &entries_[h];
		
		if (entry->key == key) {
			atomic_add_32(&entry->val, value);
			done = true;
		}
		else {
			mutex_lock(&entry->lock);
			if (entry->key == 0) {
				entry->key = key;
				entry->val = value;
				done = true;
			}
			mutex_unlock(&entry->lock);
		}
	}
}



