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
	int id_;
	int priority_;
 public:
	Tasklist(Table *in, Table *out, int id, int priority) {
		in_ = in;
		out_ = out;
		id_ = id;
		priority_ = priority;
	}

	void AddTask(Task *task) {
		tasks_.push_back(task);
	}

	Task* Fetch();


	OpType type() { return in_->type(); }
	int id() { return id_; }
	int priority() { return priority_; }
	Table* in() { return in_; }
	Table* out() { return out_; }
};

class Taskqueue
{
 private:
	list<Tasklist*> actives_;
	vector<Tasklist*> queues_;

 public:
    Taskqueue() { }

	~Taskqueue() {
		for (uint32_t i = 0; i < queues_.size(); i++)
			delete queues_[i];
	}

    int active_size() { return actives_.size(); }

	Tasklist* GetListByType(OpType type) {
		for (list<Tasklist*>::iterator it = actives_.begin();
			 it != actives_.end(); it++) {
			if ((*it)->type() == type)
				return *it;
		}
		return NULL;
	}

	void AddList(Tasklist *list) {
		queues_.push_back(list);
	}

	void AddTask(int taskid, Task *task) {
		queues_[taskid]->AddTask(task);
	}

	Task* Fetch();

	// Unblock
	// insert the tasklist into appropriate place
	// according to its priority
	void Unblock(int taskid) {
		actives_.push_back(queues_[taskid]);
	}

	void Promote(int taskid) {
		actives_.push_front(queues_[taskid]);
	}

};

#endif // TASKQUEUE_H_
