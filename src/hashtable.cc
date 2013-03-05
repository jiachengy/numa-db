#include "hashtable.h"
#include <cstring>

using namespace std;

HashTable::HashTable(size_t capacity)
{
	keys_ = new key_t[capacity];
	vals_ = new val_t[capacity];
	memset(keys_, 0, capacity * sizeof(key_t));
	memset(vals_, 0, capacity * sizeof(val_t));
	capacity_ = capacity;
}


HashTable::~HashTable()
{
	if (keys_ != NULL)
		delete [] keys_;
	if (vals_ != NULL)
		delete [] vals_;
}


val_t HashTable::Get(key_t key)
{
	uint32_t h = hash32(key) % capacity_;
	while (keys_[h] != 0 && keys_[h] != key)
		h = (h + 1) % capacity_;

	return vals_[h];
}


void HashTable::Put(key_t key, val_t value)
{
	size_++;
	uint32_t h = hash32(key) % capacity_;
	
	while (keys_[h] != 0 && keys_[h] != key)
		h = (h + 1) % capacity_;

	keys_[h] = key;
	vals_[h] = value;
}



