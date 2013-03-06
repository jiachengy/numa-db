#include "hashtable.h"
#include <cstring>

#include "util.h"

using namespace std;

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


val_t HashTable::Get(key_t key)
{
	uint32_t h = hash32(key) % capacity_;
	while (entries_[h].key != 0 && entries_[h].key != key)
		h = (h + 1) % capacity_;

	return entries_[h].val;
}


void HashTable::Put(key_t key, val_t value)
{
	size_++;
	uint32_t h = hash32(key) % capacity_;
	
	while (entries_[h].key != 0 && entries_[h].key != key)
		h = (h + 1) % capacity_;

	entries_[h].key = key;
	entries_[h].val = value;
}


void LocalAggrTable::Aggregate(key_t key, val_t value)
{
	uint32_t h = hash32(key) % capacity_;
	
	while (entries_[h].key != 0 && entries_[h].key != key)
		h = (h + 1) % capacity_;

	if (entries_[h].key == 0)
		entries_[h].key = key;
	entries_[h].val += value;
}



