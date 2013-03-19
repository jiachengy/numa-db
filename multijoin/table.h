#ifndef TABLE_H_
#define TABLE_H_

#include <string.h>
#include <list>
#include <cassert>

class Partition;
class Table;

#include "types.h" // OpType
#include "util.h" // get_running_node

class Partition {
 private:
	int node_; // numa location
	int key_; // partition key

	tuple_t *tuples_; // actual data
	size_t size_;

	//	HashTable *hashtable_; 	// normal partition and hash table share the same structure

	bool done_; // indidate the partition has been processed
	bool ready_; // indicate that the partiiton is ready to be processed

 public:
	static const size_t kPartitionSize = 32768;

	Partition(int node, int key) {
		// NOTE: the constructor must run on the right node
		assert(get_running_node() == node);

		node_ = node;
		key_ = key;
		size_ = 0;
		done_ = false;
		ready_ =false;
		tuples_ = (tuple_t*)alloc(sizeof(tuple_t) * kPartitionSize);
		memset(tuples_, 0, sizeof(tuple_t) * kPartitionSize);
	}

	~Partition() {
		dealloc(tuples_, sizeof(tuple_t) * kPartitionSize);
	}

	void Reset() { 	// used by the recycler
		// node will not be changed
		key_ = -1;
		done_ = false;
		ready_ =false;
		size_ = 0;
	}

	// for efficiency purpose, we expose the 
	// raw data and allow us to set the size
	// at once
	tuple_t *tuples() { return tuples_; }
	void set_size(size_t sz) { size_ = sz; }
	size_t size() { return size_; }

	int node() { return node_; }
	int key() { return key_;}
	void set_key(int key) { key_ = key;}
	
	bool ready() { return ready_; }
	void set_ready() { ready_ = true; }
	bool done() { return done_; }
	void set_done() { done_ = true; }
};

class Table {
 private:
	int id_;
	OpType type_;
	
	std::list<Partition*> *pnodes_;
	uint32_t nnodes_;

	std::list<Partition*> *pkeys_;
	uint32_t nkeys_;
	
	bool ready_;
	bool done_;
	
	uint32_t nparts_;
	uint32_t done_count_;

	// output buffer
	size_t nbuffers_; 
	Partition** buffers_;

 public:
	Table(int id, OpType type, uint32_t nnodes, uint32_t nkeys, size_t nbuffers) {
		id_ = id;
		type_ = type;

		nkeys_ = nkeys;
		if (nkeys)
			pkeys_ = (std::list<Partition*>*)malloc(sizeof(std::list<Partition*>) * nkeys);
		else
			pkeys_ = NULL;

		nnodes_ = nnodes;
		pnodes_ = (std::list<Partition*>*)malloc(sizeof(std::list<Partition*>) * nnodes);
		
		nbuffers_ = nbuffers;
		done_count_ = 0;
		done_ = false;
		ready_ = false;
		buffers_ = (Partition**)malloc(sizeof(Partition*) * nbuffers);
		memset(buffers_, 0, sizeof(Partition*) * nbuffers);
	}

	~Table() {
		free(buffers_);
		if (nkeys_)
			free(pkeys_);
		free(pnodes_);
	}

	void AddPartition(Partition *p) {
		nparts_++;

		pnodes_[p->node()].push_back(p);

		// if it has a partition key
		if (nkeys_)
			pkeys_[p->key()].push_back(p);
	}

	std::list<Partition*>& GetPartitionsByNode(int node) {
		return pnodes_[node];
	}


	std::list<Partition*>& GetPartitionsByKey(int key) {
		return pkeys_[key];
	}

	// commit when a partition has finished processing
	void Commit() {
		done_count_++;
		if (done_count_ == nparts_)
			set_done();
	}

	Partition* GetBuffer(int buffer_id) {
		return buffers_[buffer_id];
	}

	void SetBuffer(int buffer_id, Partition *buffer) {
		buffers_[buffer_id] = buffer;
	}

	OpType type() { return type_; }
	int id() { return id_;}
	bool ready() { return ready_; }
	void set_ready() { ready_ = true; }
	bool done() { return done_;}
	void set_done() { done_ = true; }
};


#endif // TABLE_H_
