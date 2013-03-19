#ifndef TASKQUEUE_H_
#define TASKQUEUE_H_

#include <list>
#include <map>

class Taskqueue;

#include "table.h"
#include "task.h"

// A task queue for a single task
class Simplequeue
{
 private:
	std::list<Task*> tasks_;
	Table *in_;
	Table *out_;
 public:
	Simplequeue(Table *in, Table *out) {
		in_ = in;
		out_ = out;
	}

	bool Empty() {
		return tasks_.empty();
	}

	void Add(Task *task) {
		tasks_.push_back(task);
	}

	Task* Fetch() {
		Task* next = tasks_.front();
		tasks_.pop_front();
		return next;
	}
};


// All threads share a same task queue
// A task queue is composed of several subqueues
// Partition R, Build R, Partition S, Probe S....
// Each small task has a default
// blocked operators will not be scheduled
// For example, though Build R is before Partition S
// if will only be scheduled after it is waken up
// from the block queues.
class Taskqueue
{
 private:
	Simplequeue *current_;
	std::list<Simplequeue*> actives_;
	std::map<int, Simplequeue*> queues_;

 public:
	bool Empty() {
		return current_ == NULL;
	}

	void Add(int taskid, Task *task) {
		queues_[taskid]->Add(task);
	}

	Task* Fetch() {
		Task *next = current_->Fetch();
		if (current_->Empty()) {
			actives_.pop_front();
			if (!actives_.empty())
				current_ = actives_.front();
			else
				current_ = NULL;
		}
		return next;
	}


	// after partition R is finished
	// we wake up build R in the task queue

	// wake up the subqueue
	// and append it at the beginning
	void promote(int taskid) {
		actives_.push_front(queues_[taskid]);
	}

	// wake up the subqueue
	// and append it at the end
	void wakeup(int taskid) {
		actives_.push_back(queues_[taskid]);
	}
};

#endif // TASKQUEUE_H_
