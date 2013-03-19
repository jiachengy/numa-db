#ifndef TASKQUEUE_H_
#define TASKQUEUE_H_

#include <list>
#include <vector>

class Task;
class Tasklist;
class Taskqueue;

#include "env.h"
#include "table.h"

class Task
{
 public:
	virtual void Run(thread_t *args) = 0;
};


class Tasklist
{
 private:
	std::list<Task> tasks_;
	Table *in_;
	Table *out_;
	int priority_;
 public:
	Tasklist(Table *in, Table *out, int priority) {
		in_ = in;
		out_ = out;
		priority_ = priority;
	}

	bool Empty() {
		return tasks_.empty();
	}

	void AddTask(Task *task) {
		tasks_.push_back(task);
	}

	Task* Fetch() {
		Task* next = tasks_.front();
		tasks_.pop_front();
		return next;
	}
};

class Taskqueue
{
 private:
	Tasklist *current_;
	std::list<Tasklist*> actives_;
	std::vector<Tasklist*> queues_;

 public:
	Taskqueue() {}

	~Taskqueue() {
		for (int i = 0; i < queues_.size(); i++)
			delete queues_[i];
	}

	bool Empty() {
		return current_ == NULL;
	}

	void AddList(Tasklist *list) {
		queues_.push_back(list);
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

	void promote(int taskid) {
		actives_.push_front(queues_[taskid]);
	}


	// Unblock
	// insert the tasklist into appropriate place
	// according to its priority

	void Wakeup(int taskid) {
		actives_.push_back(queues_[taskid]);
	}
};

#endif // TASKQUEUE_H_
