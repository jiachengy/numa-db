#ifndef HASHTABLE_H_
#define HASHTABLE_H_

#include <stdint.h>
#include <unistd.h>

#include "hash.h"

typedef uint32_t key_t;
typedef uint32_t val_t;

class HashTable
{
 private:
	size_t capacity_;
	size_t size_;
	
	key_t *keys_;
	val_t *vals_;
	
 public:
	HashTable(size_t capacity);
	~HashTable();


	val_t Get(key_t key);
	void Put(key_t key, val_t val);
	size_t size() { return size_; }
	size_t capacity() { return capacity_; }

	void resize() {}

};

#endif // HASHTABLE_H_
