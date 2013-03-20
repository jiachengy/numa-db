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
 public:
	virtual void Run(thread_t *args) = 0;
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

	bool Empty() {
		return tasks_.empty();
	}

	void AddTask(Task *task) {
		tasks_.push_back(task);
	}

	Task* Fetch();
	
	int id() { return id_; }
	int priority() { return priority_; }
	Table* in() { return in_; }
	Table* out() { return out_; }
};

class Taskqueue
{
 private:
	Tasklist *current_;
	list<Tasklist*> actives_;
	vector<Tasklist*> queues_;

	// remove empty task list from actives
	void RetireList() {
		actives_.pop_front();
	}

 public:
    Taskqueue() : current_(NULL) { }

	~Taskqueue() {
		for (uint32_t i = 0; i < queues_.size(); i++)
			delete queues_[i];
	}

	bool Empty() {
		return current_ == NULL;
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
		current_ = actives_.front();
	}

};

#endif // TASKQUEUE_H_
