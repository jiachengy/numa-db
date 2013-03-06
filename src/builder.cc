#include <ctime>
#include <numa.h>
#include <pthread.h>
#include <glog/logging.h>

#include "partition.h"
#include "builder.h"
#include "numadb.h"
#include "util.h"

using namespace std;

Partition* PartitionBuilder::Build(size_t sz, int cpu, PTable *table)
{
	cpu_bind(cpu);

	Partition *p = new Partition;
	p->node = node_of_cpu(cpu);
	p->cpu = cpu;
	p->size = sz;
	p->table = table;

	unsigned int seed = time(NULL);	
	data_t *data = (data_t*)alloc(sz * sizeof(data_t));
	rid_t *rids = (rid_t*)alloc(sz * sizeof(rid_t));
	for (unsigned int i = 0; i < sz; i++) {
		data[i] = rand_r(&seed) % 40;
		//		data[i] = i;
		rids[i] = i;
	}
	p->data = data;
	p->rids = rids;

	LOG(INFO) << "Partition <" << sz << "> " << cpu << ":" <<p->node;

	return p;
}

void* TableBuilder::build(void *targ)
{
	BuildArg *arg = (BuildArg*)targ;
	Partition *p = arg->builder->Build(arg->size, arg->cpu, arg->table);
	arg->p = p;
	return NULL;
}

PTable* TableBuilder::Build(size_t nparts, size_t psize)
{
	PTable *table = new PTable;

	BuildArg args[nparts];
	pthread_t threads[nparts];
	for (unsigned int i = 0; i < nparts; i++) {
		args[i].size = psize;
		args[i].cpu = i;
		args[i].table = table;
		args[i].builder = &pbuilder_;
		pthread_create(&threads[i], NULL, &TableBuilder::build, (void*)&args[i]);
	}

	for (unsigned int i = 0; i < nparts; i++)
		pthread_join(threads[i], NULL);

	for (unsigned int i = 0; i < nparts; i++)
		table->AddPartition(args[i].p);
	return table;
}



