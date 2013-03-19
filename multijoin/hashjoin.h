#ifndef HASHJOIN_H_
#define HASHJOIN_H_

#include "types.h"
#include "multijoin.h"
#include "task.h"

class PartitionTask : public Task
{
 private:
	// Input partition
	Partition *part_;
	// Input table
	Table *in_;
	// Output table
	Table *out_;

	// Parameters
	int offset_; // cluster bits
	int nbits_; // radix bits per pass

	void ProcessBlock(thread_t *args, block_t block);
	void Unblock(thread_t *threads, int nthreads);

 public:
	PartitionTask(Partition *part, int offset, int nbits, Table *out) {
		this->part_ = part;
		this->offset_ = offset;
		this->nbits_ = nbits;
		this->out_ = out;
	}

	virtual ~PartitionTask() {
		delete part_;
	}

	virtual void Run(thread_t *args);
};


class BuildTask : public Task
{
 private:
	Partition part_;

	// output
	Table *in_;
	Table *out_;

 public:
	virtual void Run(thread_t *args) {
		// Get the hash table from out
		// If hash table does not exists, build a new one

		// Insert new element into the hash table
	}

};

class ProbeTask : public Task
{
 private:
	Partition part_;
	//	HashTable *ht; // the hash table to probe

	Table *in_;
	Table *out_;

	// info for next operator
	// probe will create the next partition task

 public:
	virtual void Run(thread_t *args) {
		//		for (record r in part) {
			// probe
			
			// check if the output buffer is full
			// if not, add to the buffer
			// else create a new buffer and schedule
		//		}
		// mark current out as ready
	}

};


#endif // HASHJOIN_H_
