#ifndef HASHTABLE_H_
#define HASHTABLE_H_

#include <stdint.h>
#include <unistd.h>

#include "numadb.h"
#include "hash.h"
#include "atomic.h"

struct Entry
{
	volatile data_t key;
	volatile val_t val;
};

struct GlobalEntry
{
	volatile data_t key;
	volatile val_t val;
	uint32_t count;
};

class HashTable
{
 protected:
	size_t capacity_;
	size_t size_;

	Entry *entries_;
 public:
	HashTable();
	HashTable(size_t capacity);
	virtual ~HashTable();

    val_t Get(data_t key);
	void Put(data_t key, val_t val);
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

	void Aggregate(data_t key, val_t val);

};

class GlobalAggrTable : public HashTable
{
 private:
	// override the parent
	GlobalEntry *entries_;
 public:
	GlobalAggrTable(size_t capacity);
	~GlobalAggrTable();
	val_t Get(data_t key);
	void Aggregate(data_t key, val_t val);

};


#endif // HASHTABLE_H_
