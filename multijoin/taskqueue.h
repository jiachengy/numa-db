#ifndef TASKQUEUE_H_
#define TASKQUEUE_H_

#include <cassert>
#include <list>
#include <vector>

class Task;
class Tasklist;
class Taskqueue;

#include "env.h"
#include "table.h"

using namespace std;

enum ShareLevel {
  ShareLocal,
  ShareNode,
  ShareGlobal
};

class Task
{
 protected:
  OpType type_;
 public:
 Task(OpType type) : type_(type) {}
  virtual void Run(thread_t *args) = 0;
    
  OpType type() { return type_; }
};


class Tasklist
{
 private:
  list<Task*> tasks_;
  Table *in_;
  Table *out_;
  ShareLevel share_;
 public:
  Tasklist(Table *in, Table *out, ShareLevel share) {
    in_ = in;
    out_ = out;
    share_ = share;
  }

  void AddTask(Task *task) {
    tasks_.push_back(task);
  }

  Task* Fetch();

  bool Empty() { return tasks_.empty(); }

  size_t size() { return tasks_.size(); }
  OpType type() { return in_->type(); }
  int id() { return in_->id(); }
  Table* in() { return in_; }
  Table* out() { return out_; }
  ShareLevel share() { return share_; }
};

class Taskqueue
{
 private:
  list<Tasklist*> actives_;
  vector<Tasklist*> queues_;

 public:
  Taskqueue() { }

  ~Taskqueue() {
  }

  int active_size() { return actives_.size(); }

  Tasklist* GetListByType(OpType type) {
    for (list<Tasklist*>::iterator it = actives_.begin();
         it != actives_.end(); it++) {
      if ((*it)->type() == type && !(*it)->Empty())
        return *it;
    }
    return NULL;
  }

  void AddList(Tasklist *list) {
    uint32_t taskid = list->id();
    if (taskid >= queues_.size()) {
      queues_.resize(taskid + 1);
    }

    LOG(INFO) << "taskid: " << taskid << " queue size: " << queues_.size();
    queues_[taskid] = list;
  }

  void AddTask(int taskid, Task *task) {
    queues_[taskid]->AddTask(task);
  }

  Task* Fetch();

  // Unblock
  // insert the tasklist into appropriate place
  // according to its priority
  void Unblock(int taskid) {
    LOG(INFO) << "Tasklist " << taskid;
    actives_.push_back(queues_[taskid]);
    LOG(INFO) << "Tasklist " << taskid 
              << "is unblocked, its size: " << queues_[taskid]->size();
  }

  void Promote(int taskid) {
    actives_.push_front(queues_[taskid]);
  }

};

#endif // TASKQUEUE_H_
