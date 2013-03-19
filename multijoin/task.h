#ifndef TASK_H_
#define TASK_H_

#include "multijoin.h" // thread_t

class Task
{
 public:
	// run the task on thread tid
	virtual void Run(thread_t *args) = 0;
};


#endif // TASK_H_
