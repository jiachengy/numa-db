#ifndef HASHTABLE_H_
#define HASHTABLE_H_

#include <stdint.h>
#include <unistd.h>

#include "hash.h"

typedef int32_t key_t;
typedef int32_t val_t;

struct Entry
{
	key_t key;
	val_t val;
};


class HashTable
{
 protected:
	size_t capacity_;
	size_t size_;

	Entry *entries_;
 public:
	HashTable(size_t capacity);
	virtual ~HashTable();

	val_t Get(key_t key);
	void Put(key_t key, val_t val);
	size_t size() { return size_; }
	size_t capacity() { return capacity_; }

	void resize() {}

};


class LocalAggrTable : public HashTable
{
 private:
	
 public:
	LocalAggrTable(size_t capacity)
		: HashTable(capacity) {}

	void Aggregate(key_t key, val_t val);

};


#endif // HASHTABLE_H_
