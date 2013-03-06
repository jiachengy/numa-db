#ifndef ENGINE_H_
#define ENGINE_H_

#include <queue>
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

	void initpipe(Plan *p);
	void start();
	void stop();
 public:
	void Query(Plan *p);
};

#endif // ENGINE_H_
