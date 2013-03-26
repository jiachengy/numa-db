#include <ctime>
#include <pthread.h>
#include <glog/logging.h>

#include "params.h"
#include "builder.h"
#include "util.h"

using namespace std;

int sign = 1;

Partition* PartitionBuilder::Build(size_t size, Recycler *recycler)
{
#ifdef PRE_ALLOC
  Partition *p = recycler->GetEmptyPartition();
#else  
  Partition *p = new Partition(get_running_node(), -1);
  p->Alloc();
#endif

  uint32_t seed = rand();
  mt_state_t *state = mt_init(seed);

  // what is the right way to generate data?
  tuple_t *tuples = p->tuples();
  for (unsigned int i = 0; i < size; i++) {
    tuples[i].key = mt_next(state) % 100000;
    tuples[i].payload = tuples[i].key+ sign ;
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
    Partition *p = pbuilder->Build(Params::kPartitionSize, arg->recycler);
    arg->partitions[i] = p;
  }
	
  return NULL;
}

void TableBuilder::Build(Table *table, size_t size, uint32_t nthreads)
{

  sign *= -1;
  LOG(INFO) << "sign is " << sign;

  uint32_t npartitions = size / Params::kPartitionSize;

  nthreads = npartitions < nthreads ? npartitions : nthreads;

  // make sure each node has even number of partitions
  assert(nthreads % num_numa_nodes() == 0); 
  // make sure each thread has even size
  assert(npartitions / nthreads * nthreads == npartitions);

  BuildArg *args = (BuildArg*)malloc(sizeof(BuildArg) * nthreads);
  pthread_t threads[nthreads];
  for (uint32_t i = 0; i < nthreads; i++) {
    args[i].nparts = npartitions / nthreads;
    args[i].cpu = cpu_of_thread_rr(i);
    args[i].builder = &pbuilder_;
    args[i].partitions = (Partition**)malloc(sizeof(Partition*) * args[i].nparts);
    args[i].recycler = recyclers_[node_of_cpu(args[i].cpu)];

    pthread_create(&threads[i], NULL, &TableBuilder::build, (void*)&args[i]);
  }

  for (uint32_t i = 0; i < nthreads; i++)
    pthread_join(threads[i], NULL);

  for (uint32_t i = 0; i < nthreads; i++)
    for (uint32_t p = 0; p < args[i].nparts; p++)
      table->AddPartition(args[i].partitions[p]);

  for (uint32_t i = 0; i < nthreads; i++)
    free(args[i].partitions);
    
  free(args);

  table->set_ready();
}



