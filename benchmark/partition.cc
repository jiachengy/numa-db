/*
 * A simple benchmark to show that inter-operator / inter-query
 * parallelism is better than one operator at a time
 * Just to prove the concept
 * No sophisticated job stealing all these stuff, just uniform
 */

#include <pthread.h>
#include <numa.h>
#include <glog/logging.h>
#include "../src/util.h"

struct ThreadArg
{
	int cpu;
	size_t ntuples;
	int32_t *keys;
	int32_t *values;
};

size_t ngroups = 100;
const size_t nthreads = 32;
ThreadArg args[nthreads];

void* generate(void *arg)
{
	ThreadArg *a = (ThreadArg*)arg;
	cpu_bind(a->cpu);
	
	int32_t *keys = alloc(sizeof(int32_t) * a->ntuples);
	int32_t *values = alloc(sizeof(int32_t) * a->ntuples);

	uint32_t seed = time(NULL);
	for (unsigned int i = 0; i < a->ntuples; i++) {
		int r = rand_r(&seed) % ngroups;
		keys[i] = r;
	}
}

void Generate()
{
	pthread_t threads[nthreads];
	for (unsigned int i = 0; i < nthreads; i++) {
		args[i].cpu = i;
		pthread_create(&threads[i], NULL, generate, (void*)args[i]);
	}

	for (unsigned int i = 0; i < nthreads; i++) {
		pthread_join(threads[i], NULL);
	}
}

void IntraAggregation()
{
	for (unsigned int i = 0; i < 4; i++) {
		create();
		ParallelAggregation(32 threads);
		join();
	}
}


void InterAggregation()
{
	for (unsigned int i = 0; i < 4; i++) {
		create();
		ParallelAggregation(8 threads);
		ParallelAggregation(8 threads);
		ParallelAggregation(8 threads);
		ParallelAggregation(8 threads);
	}


	for (unsigned int i = 0; i < 4; i++) {
		join();
	}

}

int main(int argc, char *argv[])
{
	google::InitGoogleLogging(argv[0]);
	FLAGS_logtostderr = true;

	// 1. Generate the datasets


	// 2. Test one at a time

	// 3. Test four together
}
