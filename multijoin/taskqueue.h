#ifndef TASKQUEUE_H_
#define TASKQUEUE_H_

#include <cassert>
#include <queue>
#include <vector>
#include <pthread.h>

class Task;
class Tasklist;
class Taskqueue;

#include "env.h"
#include "table.h"

using namespace std;

class Task
{
 protected:
  const OpType type_;
  Table *in_;
  Table *out_;

 public:
  Task(OpType type) : type_(type), in_(NULL), out_(NULL) {}
 
  virtual ~Task() {}

  virtual void Run(thread_t *args) = 0;    
  OpType type() { return type_; }

  void set_in(Table *in) { in_ = in; }
  void set_out(Table *out) { out_ = out; }

  int id() { assert(in_ != NULL); return in_->id(); }
};


class Tasklist
{
 private:
  queue<Task*> tasks_;
  Table *in_;
  Table *out_;
  ShareLevel share_level_;
  pthread_mutex_t mutex_;
  
 public:
  Tasklist(Table *in, Table *out, ShareLevel level) {
    in_ = in;
    out_ = out;
    share_level_ = level;
    pthread_mutex_init(&mutex_, NULL);
  }

  ~Tasklist() {
    pthread_mutex_destroy(&mutex_);
  }

  void AddTaskAtomic(Task *task) {
    pthread_mutex_lock(&mutex_);
    task->set_in(in_);
    task->set_out(out_);
    tasks_.push(task);
    pthread_mutex_unlock(&mutex_);
  }

  void AddTask(Task *task) {
    task->set_in(in_);
    task->set_out(out_);
    tasks_.push(task);
  }

  Task* Fetch();
  Task* FetchAtomic();

  bool exhausted() { return tasks_.empty() && in_->ready(); }
  bool empty() { return tasks_.empty(); }

  size_t size() { return tasks_.size(); }
  OpType type() { return in_->type(); }
  int id() { return in_->id(); }
  Table* in() { return in_; }
  Table* out() { return out_; }
  ShareLevel share_level() { return share_level_; }
  void set_share_level(ShareLevel level) { share_level_ = level; }
};

class Taskqueue
{
 private:
  list<Tasklist*> actives_;
  vector<Tasklist*> queues_;
  pthread_mutex_t mutex_;

 public:
  Taskqueue() {
    pthread_mutex_init(&mutex_, NULL);
  }
  ~Taskqueue() {
    pthread_mutex_destroy(&mutex_);
  }

  Tasklist* GetListByType(OpType type) {
    for (list<Tasklist*>::iterator it = actives_.begin();
         it != actives_.end(); it++) {
      if ((*it)->type() == type && !(*it)->empty())
        return *it;
    }
    return NULL;
  }

  void AddList(Tasklist *list) {
    pthread_mutex_lock(&mutex_);
    uint32_t taskid = list->id();
    if (taskid >= queues_.size()) {
      queues_.resize(taskid + 1);
    }
    queues_[taskid] = list;

    pthread_mutex_unlock(&mutex_);
  }


  Tasklist* GetTasklist(int taskid) {
    return queues_[taskid];
  }

  void AddTask(int taskid, Task *task) {
    pthread_mutex_lock(&mutex_);
    queues_[taskid]->AddTask(task);
    pthread_mutex_unlock(&mutex_);
  }


  Task* Fetch();
  
  // we allow putting back a task
  // when are in the middle of processing
  // if the task is put back
  // its tasklist must still be active
  void Putback(int taskid, Task *task) {
    pthread_mutex_lock(&mutex_);
    queues_[taskid]->AddTaskAtomic(task);
    pthread_mutex_unlock(&mutex_);
  }

  void Unblock(int taskid) {
    pthread_mutex_lock(&mutex_);
    actives_.push_back(queues_[taskid]);
    pthread_mutex_unlock(&mutex_);
  }

  void Promote(int taskid) {
    pthread_mutex_lock(&mutex_);
    actives_.push_front(queues_[taskid]);
    pthread_mutex_unlock(&mutex_);
  }

  int active_size() { return actives_.size(); }
};

#endif // TASKQUEUE_H_
