#ifndef HASHJOIN_H_
#define HASHJOIN_H_

#include "types.h"
#include "taskqueue.h" // Task
#include "env.h"

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

  void ProcessBlock(thread_t *args, block_t block, uint32_t mask, uint32_t fanout, uint32_t hist[], tuple_t *dst[]);
  void Finish(thread_t *args);

 public:
 PartitionTask(OpType type, Partition *part, Table *in, Table *out, int offset, int nbits) : Task(type) {
    this->in_ = in;
    this->out_ = out;
    this->part_ = part;
    this->offset_ = offset;
    this->nbits_ = nbits;
  }

  virtual ~PartitionTask() {
    delete part_;
  }

  virtual void Run(thread_t *args);
};


/*
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
*/

#endif // HASHJOIN_H_
