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
  partition_t *input_;

  // Parameters
  int pass_;
  int shift_; // cluster bits
  int bits_; // radix bits per pass
  int fanout_;
  int mask_;

  void Finish(thread_t *my);
  void DoPartition(thread_t *my);
 public:
 PartitionTask(partition_t *input, int shift, int bits) : Task(OpPartition) {
    pass_ = (shift==0) ? 1 : 2;
    input_ = input;
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

  size_t size() { return subtasks_->size(); }

  // the subtask has to be a PartitionTask
  void AddSubTask(PartitionTask *task) {
    subtasks_->AddTaskAtomic((Task*)task);
  }
};



class BuildTask : public Task
{
 private:
  int radix_;

  // information required to create the probing task
  Table *probe_;

  partition_t* Build(thread_t * my);
  void Finish(thread_t *my, partition_t *htp);

 public:
 BuildTask(Table *probe, int radix) : Task(OpBuild) {
    radix_ = radix;
    probe_ = probe;
  }

  virtual void Run(thread_t *my);
};

class ProbeTask : public Task
{
 private:
  Tasklist *subtasks_;
  int radix_;

 public:
 ProbeTask(Table *in, Table *out, int radix)
    : Task(OpProbe) {
    radix_ = radix;
    subtasks_ = new Tasklist(in, out, ShareLocal);
  }
  
  virtual void Run(thread_t *my) {
    assert(my->batch_task == NULL);
    assert(my->localtasks == NULL);    
    my->batch_task = this;
    my->localtasks = subtasks_;
  }

  // the subtask has to be a PartitionTask
  void AddSubTask(UnitProbeTask *task) {
    subtasks_->AddTaskAtomic((Task*)task);
  }

  size_t size() { return subtasks_->size(); }

};



class UnitProbeTask : public Task
{
 private:
  partition_t *input_;

  void Finish(thread_t* my);
  void DoJoin(hashtable_t *hashtable, thread_t *my);

 public:
 UnitProbeTask(partition_t *input) : Task(OpUnitProbe) { input_ = input; }
  virtual void Run(thread_t *my);
};

inline uint32_t encode_radix(uint32_t low, uint32_t high, int offset)
{
  return low | (high << offset);
}

inline uint32_t mhash(uint32_t key, uint32_t mask, int shift)
{
  return (key & mask) >> shift;
}

void FlushBuffer(buffer_t *buffer, partition_t *p, Environment *env);
void FlushEntireBuffer(buffer_t *buffer, int node, Environment *env);

#endif // HASHJOIN_H_
