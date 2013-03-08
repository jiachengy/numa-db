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

	Partition *p = new Partition(node_of_cpu(cpu), cpu);
	p->size = sz;
	p->table = table;

	unsigned int seed = time(NULL);	
	data_t *data;
	rid_t *rids;

	if (local2) {
		data = (data_t*)alloc(sz * sizeof(data_t));
		rids = (rid_t*)alloc(sz * sizeof(rid_t));
	}
	else {
		data = (data_t*)alloc_on_node(sz * sizeof(data_t), (node_of_cpu(cpu)+1)%num_numa_nodes());
		rids = (rid_t*)alloc_on_node(sz * sizeof(rid_t), (node_of_cpu(cpu)+1) % num_numa_nodes());
		LOG(INFO) << "Allocate base on node " << (node_of_cpu(cpu) + 1)%num_numa_nodes();
	}
	memset(data, 0, sz * sizeof(data_t));
	memset(rids, 0, sz * sizeof(rid_t));

	for (unsigned int i = 0; i < sz; i++) {
		data[i] = rand_r(&seed) % ngroups + 1;
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

		if (roundrobin)
			args[i].cpu = rrobin[i];
		else
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



