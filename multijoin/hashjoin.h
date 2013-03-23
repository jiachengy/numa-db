#ifndef HASHJOIN_H_
#define HASHJOIN_H_

#include "table.h" // Partition, Table
#include "types.h" // tuple_t, OpType
#include "taskqueue.h" // Task
#include "env.h" // thread_t

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

  void ProcessBlock(thread_t *my, block_t block, uint32_t mask, uint32_t fanout, uint32_t hist[], tuple_t *dst[]);
  void Finish(thread_t *my);
  
 public:
 PartitionTask(OpType type, Partition *part, Table *in, Table *out, int offset, int nbits) : Task(type) {
    this->in_ = in;
    this->out_ = out;
    this->part_ = part;
    this->offset_ = offset;
    this->nbits_ = nbits;
  }
  
  virtual void Run(thread_t *my);
};



class BuildTask : public Task
{
 private:
  Table *in_;
  Table *out_;
  int key_;

  // information required to create the probing task
  Table *probe_;
  Table *probe_out_;

  void Finish(thread_t *my, hashtable_t *ht);

 public:
 BuildTask(OpType type, Table *in, Table *out, Table *probe, Table *probe_out, int key) : Task(type) {
    in_ = in;
    out_ = out;
    key_ = key;
    
    probe_ = probe;
    probe_out_ = probe_out;
  }

  virtual void Run(thread_t *my);

};

class ProbeTask : public Task
{
 private:
  int key_;
 public:
 ProbeTask(OpType type, int key)
    : Task(type) {
    key_ = key;
  }
  
  virtual void Run(thread_t *my);
};



class UnitProbeTask : public Task
{
 private:
  Partition *part_;

  Table *in_;
  Table *out_;

  Table *build_;

  void ProbeBlock(thread_t *my, block_t block, hashtable_t *ht);
  void Finish(thread_t* my);

 public:
  UnitProbeTask(OpType type, Partition *part,
            Table *in, Table *out, Table *build)
    : Task(type) {
    part_ = part;
    in_ = in;
    out_ = out;
    build_ = build;
  }
  

  virtual void Run(thread_t *my);
};

#endif // HASHJOIN_H_
