#ifndef HASHJOIN_H_
#define HASHJOIN_H_

class PartitionTask;
class P2Task;
class BuildTask;
class ProbeTask;
class UnitProbeTask;

#include "table.h" // partition_t, Table
#include "types.h" // tuple_t, OpType
#include "taskqueue.h" // Task
#include "env.h" // thread_t

class PartitionTask : public Task
{
 private:
  // Input partition
  partition_t *part_;

  // Parameters
  int shift_; // cluster bits
  int bits_; // radix bits per pass
  int fanout_;
  int mask_;

  void Finish(thread_t *my);
  void DoPartition(thread_t *my);
  
 public:
 PartitionTask(partition_t *part, int shift, int bits) : Task(OpPartition) {
    part_ = part;
    shift_ = shift;
    bits_ = bits;
    fanout_ = 1 << bits;
    mask_ = (fanout_-1) << shift_;
  }
  
  virtual void Run(thread_t *my);
};


class P2Task : public Task
{
 private:
  Tasklist *subtasks_;

  // Parameters
  int key_;

  void Finish(thread_t *my);
  
 public:
 P2Task(int key, Table *in, Table *out) : Task(OpPartition2) {
    subtasks_ = new Tasklist(in, out, ShareLocal);
    key_ = key;
  }

  ~P2Task() {
  }

  virtual void Run(thread_t *my) {
    assert(my->batch_task == NULL);
    assert(my->localtasks == NULL);    
    my->batch_task = this;
    my->localtasks = subtasks_;
  }

  // the subtask has to be a PartitionTask
  void AddSubTask(PartitionTask *task) {
    subtasks_->AddTaskAtomic((Task*)task);
  }
};



class BuildTask : public Task
{
 private:
  int key_;

  // information required to create the probing task
  Table *probe_;
  Table *probe_out_;

  void Finish(thread_t *my, partition_t *htp);

 public:
 BuildTask(OpType type, Table *probe, int key) : Task(type) {
    key_ = key;
    
    probe_ = probe;
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
  partition_t *part_;

  Table *build_;

  //  void ProbeBlock(thread_t *my, block_t block, hashtable_t *ht);
  void Finish(thread_t* my);

 public:
  UnitProbeTask(OpType type, partition_t *part,
           Table *build)
    : Task(type) {
    part_ = part;
    build_ = build;
  }
  

  virtual void Run(thread_t *my);
};

#endif // HASHJOIN_H_
