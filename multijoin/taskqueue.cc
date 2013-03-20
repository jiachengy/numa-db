#include "taskqueue.h"

Task* Tasklist::Fetch() {
	assert(!Empty());
	Task* next = tasks_.front();
	tasks_.pop_front();
	return next;
}

Task* Taskqueue::Fetch() {
	Task *next = current_->Fetch();
	if (current_->Empty()) {
		RetireList();
		if (!actives_.empty())
			current_ = actives_.front();
		else
			current_ = NULL;
	}
	return next;
}

