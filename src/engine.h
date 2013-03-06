#ifndef ENGINE_H_
#define ENGINE_H_

#include <queue>
#include <pthread.h>
#include "executor.h"
#include "partition.h"
#include "plan.h"

using namespace std;

class QueryEngine
{
 private:
	size_t nworkers_;
	queue<Partition*> *actives_;
	queue<Partition*> *blocked_;
	Executor *executors_;

	pthread_mutex_t done_mutex_;
	pthread_cond_t done_cv_;
	size_t done_count_;

	void initpipe(Plan *p);
	void start();
	void stop();
 public:
	void Query(Plan *p);

	friend class Executor;
};

#endif // ENGINE_H_
