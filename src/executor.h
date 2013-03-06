#ifndef EXECUTOR_H_
#define EXECUTOR_H_

#include "numadb.h"
#include "partition.h"

#include <queue>
#include <pthread.h>

using namespace std;

class Partition;

class Executor
{
private:
	pthread_t thread_;
	int cpu_;
	int exit_;
	queue<Partition*> *active_;
	static void* execute(void *args);

public:
	Executor() {exit_ = false;}
	void start();
	void stop();

	friend class QueryEngine;
};

#endif // EXECUTOR_H_
