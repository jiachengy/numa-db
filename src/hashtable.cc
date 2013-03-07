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

LocalAggrTable::LocalAggrTable(size_t capacity, int node)
{
	capacity_ = capacity;
	if (local) {
		entries_ = (Entry*)alloc(capacity * sizeof(Entry));
		LOG(INFO) << "Allcocating local";
	}
	else {
		entries_ = (Entry*)alloc_on_node(capacity * sizeof(Entry), node);
		LOG(INFO) << "Allcocating on node " << node;
	}
	memset(entries_, 0, capacity * sizeof(Entry));
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
	entries_ = (GlobalEntry*)alloc_interleaved(capacity * sizeof(GlobalEntry));
	memset(entries_, 0, capacity * sizeof(GlobalEntry));
}


GlobalAggrTable::~GlobalAggrTable()
{
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


// 0 is a special key
void GlobalAggrTable::Aggregate(data_t key, val_t value)
{
	uint32_t h = hash32(key) % capacity_;
	for (;;) {
		// check if the key matches with bucket
		if (entries_[h].key == key) {
			// update and leave
			atomic_add(&entries_[h].count, 1);
			atomic_add(&entries_[h].val, value);
			return;
		}
		// check if bucket is empty
		if (entries_[h].key == 0) {
			// check if you are the first to update
			if (__sync_bool_compare_and_swap(&entries_[h].key, 0, key)) {
				// update and leave
				atomic_add(&entries_[h].count, 1);
				atomic_add(&entries_[h].val, value);
				return;
			}
			// the bucket is not yours, but go back check if same key
			h--;
		}
		// reached the end of the table ? -> restart
		if (++h == capacity_) h = 0;
    } 
}



