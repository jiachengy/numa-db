#ifndef TABLE_H_
#define TABLE_H_

#include <string.h>
#include <list>
#include "params.h"

enum OperationType {
	OpPartition,
	OpBuild,
	OpProbe
};

class Partition {
 private:
	tuple_t *tuples_;
	size_t size_;

	//	HashTable *hashtable_; 	// a hashtable partition

	bool done_; // indidate the partition has been processed
	bool ready_; // indicate that the partiiton is ready to be processed

 public:
 Partition() : size_(0) {
		tuples_ = (tuple_t*)malloc(sizeof(tuple_t) * PARTITION_SIZE);
	}

	~Partition() {
		free(tuples_);
	}

	friend class Task;
	friend class PartitionTask;
};

class Table {
 private:
	int id_;

	std::list<Partition*> parts_;
	unsigned int done_count_;

	// output buffer
	size_t nbuffers_; 
	Partition** buffers_;

	OperationType type_;
	
 public:
	Table(size_t nbuffers) {
		nbuffers_ = nbuffers;
		done_count_ = 0;
		buffers_ = (Partition**)malloc(sizeof(Partition*) * nbuffers);
		memset(buffers_, 0, sizeof(Partition*) * nbuffers);
	}

	~Table() {
		free(buffers_);
	}

	void AddPartition(Partition *part) {
		parts_.push_back(part);
	}

	// commit when a partition has finished processing
	// return true if this is the last partition
	bool Commit() {
		done_count_++;
		return done_count_ == parts_.size();
	}

	OperationType type() { return type_; }

	friend class Task;
	friend class PartitionTask;
};


#endif // TABLE_H_
