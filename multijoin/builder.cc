#include <ctime>
#include <pthread.h>
#include <glog/logging.h>

#include "builder.h"
#include "util.h"

using namespace std;

Partition* PartitionBuilder::Build(size_t size)
{
	Partition *p = new Partition(get_running_node(), -1);
	uint32_t seed = time(NULL);	

	tuple_t *tuples = p->tuples();
	for (unsigned int i = 0; i < size; i++) {
		tuples[i].key = rand_r(&seed);
		tuples[i].payload = i;
	}
	p->set_size(size);

	return p;
}

void* TableBuilder::build(void *params)
{
	BuildArg *arg = (BuildArg*)params;
	cpu_bind(arg->cpu);
	PartitionBuilder *pbuilder = arg->builder;


	for (uint32_t i = 0; i < arg->nparts; i++) {
		Partition *p = pbuilder->Build(Partition::kPartitionSize);
		arg->partitions[i] = p;
	}
	
	return NULL;
}

void TableBuilder::Build(Table *table, size_t size)
{
	uint32_t npartitions = size / Partition::kPartitionSize;

	uint32_t ncpus = cpus();
	BuildArg args[ncpus];
	pthread_t threads[ncpus];
	for (unsigned int i = 0; i < ncpus; i++) {
		args[i].nparts = npartitions / ncpus;
		args[i].cpu = i;
		args[i].builder = &pbuilder_;
		args[i].partitions = (Partition**)malloc(sizeof(Partition*) * args[i].nparts);
		pthread_create(&threads[i], NULL, &TableBuilder::build, (void*)&args[i]);
	}

	for (uint32_t i = 0; i < ncpus; i++)
		pthread_join(threads[i], NULL);

	for (uint32_t i = 0; i < ncpus; i++)
		for (uint32_t p = 0; p < args[i].nparts; p++)
			table->AddPartition(args[i].partitions[p]);

	for (uint32_t i = 0; i < ncpus; i++)
		free(args[i].partitions);
}



