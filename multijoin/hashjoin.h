#ifndef HASHJOIN_H_
#define HASHJOIN_H_

#include "table.h" // Partition, Table
#include "types.h" // tuple_t, OpType
#include "taskqueue.h" // Task
#include "env.h" // thread_t

class PartitionTask;
class P2Task;
class BuildTask;
class ProbeTask;
class UnitProbeTask;

class PartitionTask : public Task
{
 private:
  // Input partition
  Partition *part_;

  P2Task ***p2tasks_;

  // Parameters
  int offset_; // cluster bits
  int nbits_; // radix bits per pass

  void Finish(thread_t *my);
  void DispatchNewPartition(Partition *p, thread_t *my);
  
 public:
 PartitionTask(Partition *part, int offset, int nbits, P2Task ***p2tasks) : Task(OpPartition) {
    this->part_ = part;
    this->offset_ = offset;
    this->nbits_ = nbits;
    this->p2tasks_ = p2tasks;
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
    LOG(INFO) << key_ << " is fetched by " << my->tid;
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

  void Finish(thread_t *my, Partition *htp);

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
  Partition *part_;

  Table *build_;

  //  void ProbeBlock(thread_t *my, block_t block, hashtable_t *ht);
  void Finish(thread_t* my);

 public:
  UnitProbeTask(OpType type, Partition *part,
           Table *build)
    : Task(type) {
    part_ = part;
    build_ = build;
  }
  

  virtual void Run(thread_t *my);
};

#endif // HASHJOIN_H_
