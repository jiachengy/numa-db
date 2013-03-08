#include <numa.h>
#include <pthread.h>
#include <sys/time.h>
#include <stdint.h>
#include <iostream>

using namespace std;

bool interleaved = false;

uint64_t micro_time(void)
{
	struct timeval t;
	struct timezone z;
	gettimeofday(&t, &z);
	return t.tv_sec * 1000000 + t.tv_usec;
}

void cpu_bind(int cpu)
{
	cpu_set_t cpu_set;
	CPU_ZERO(&cpu_set);
	CPU_SET(cpu, &cpu_set);
	pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpu_set);
}

uint32_t hash32(uint32_t key)
{
    int h = key;
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
}

struct ThreadArg
{
	int cpu;
	int build_cpu;
	int32_t *data;
	int32_t *fake;
	size_t sz;
};


void* flush_cache(void *args)
{
	ThreadArg *arg = (ThreadArg*)args;
	cpu_bind(arg->cpu);
	int32_t sum = 0;
	
	int32_t *fake = arg->fake;
	for (unsigned int i = 0; i < arg->sz; i++) {
		int idx = hash32(i) % arg->sz;
		sum += fake[idx];
	}

	pthread_exit(NULL);
}

void* do_scan(void *args)
{
	ThreadArg *arg = (ThreadArg*)args;
	cpu_bind(arg->cpu);
	int32_t sum = 0;
	
	int32_t *data = arg->data;
	//	unsigned int seed = time(NULL);
	for (unsigned int i = 0; i < arg->sz; i++) {
		//		int idx = rand_r(&seed) % arg->sz;
		int idx = hash32(i) % arg->sz;
		//		sum += data[idx];
		data[idx] = idx;

		//		sum += data[i];
		//		data[i] = i;
	}

	pthread_exit(NULL);
}

void* do_build(void *args)
{
	ThreadArg *arg = (ThreadArg*)args;
	cpu_bind(arg->build_cpu);

	if (interleaved)
		arg->data = (int32_t*)numa_alloc_interleaved(sizeof(int32_t) * arg->sz);
	else
		//		arg->data = (int32_t*)numa_alloc_local(sizeof(int32_t) * arg->sz);
		arg->data = (int32_t*)numa_alloc_onnode(sizeof(int32_t) * arg->sz, numa_node_of_cpu(arg->build_cpu));

	memset(arg->data, 0, sizeof(int32_t) * arg->sz);

	pthread_exit(NULL);
}




int main(int argc, char *argv[])
{
	cpu_bind(0);

	size_t rounds = 1;
	size_t nthreads = atoi(argv[1]);
	size_t ntuples = 1024 * 1024 * 64;
	
	pthread_t threads[nthreads];
	ThreadArg args[nthreads];

	
	long total = 0;
	long t = 0;

	int32_t *fake = (int32_t*)numa_alloc_interleaved(sizeof(int32_t) * ntuples);
	memset(fake, 0, sizeof(int32_t) * ntuples);
	for (unsigned int i = 0; i < nthreads; i++) {
		args[i].fake = fake;
	}


	// interleaved = true;
	// for (int round = 0; round < rounds; round++) {
	// 	for (unsigned int i = 0; i < nthreads; i++) {
	// 		args[i].fake = fake;
	// 		args[i].sz = ntuples;
	// 		args[i].cpu = i;
	// 		args[i].build_cpu = (i + 8) % 64;
	// 		//			args[i].data = (int32_t*)numa_alloc_interleaved(sizeof(int32_t) * args[i].sz);
	// 		//			memset(args[i].data, 0, sizeof(int32_t) * args[i].sz);
	// 	}

	// 	for (unsigned int i = 0; i < nthreads; i++)
	// 		pthread_create(&threads[i], NULL, do_build, (void*)&args[i]);

	// 	for (unsigned int i = 0; i < nthreads; i++)
	// 		pthread_join(threads[i], NULL);


	// 	for (unsigned int i = 0; i < nthreads; i++)
	// 		pthread_create(&threads[i], NULL, flush_cache, (void*)&args[i]);

	// 	for (unsigned int i = 0; i < nthreads; i++)
	// 		pthread_join(threads[i], NULL);

	// 	t = micro_time();
	// 	for (unsigned int i = 0; i < nthreads; i++)
	// 		pthread_create(&threads[i], NULL, do_scan, (void*)&args[i]);

	// 	for (unsigned int i = 0; i < nthreads; i++)
	// 		pthread_join(threads[i], NULL);
		
	// 	total += (micro_time() -t);

	// 	for (unsigned int i = 0; i < nthreads; i++) {
	// 		numa_free(args[i].data, args[i].sz * sizeof(int32_t));
	// 	}
	// }

	// cout << "Interleaved scan:" << total << " msecs" << endl;


	interleaved = false;
	total = 0;
	for (int round = 0; round < rounds; round++) {
		// scan adjacent
		for (unsigned int i = 0; i < nthreads; i++) {
			args[i].sz = ntuples;
			args[i].cpu = i;
			args[i].build_cpu = (i+8) % 64;
			//			args[i].data = (int32_t*)numa_alloc_onnode(sizeof(int32_t) * args[i].sz, numa_node_of_cpu(args[i].build_cpu));
			//			memset(args[i].data, 0, sizeof(int32_t) * args[i].sz);

		}

		for (unsigned int i = 0; i < nthreads; i++)
			pthread_create(&threads[i], NULL, do_build, (void*)&args[i]);

		for (unsigned int i = 0; i < nthreads; i++)
			pthread_join(threads[i], NULL);

		for (unsigned int i = 0; i < nthreads; i++)
			pthread_create(&threads[i], NULL, flush_cache, (void*)&args[i]);

		for (unsigned int i = 0; i < nthreads; i++)
			pthread_join(threads[i], NULL);

		t = micro_time();
		for (unsigned int i = 0; i < nthreads; i++)
			pthread_create(&threads[i], NULL, do_scan, (void*)&args[i]);

		for (unsigned int i = 0; i < nthreads; i++)
			pthread_join(threads[i], NULL);
		
		total += (micro_time() -t);

		for (unsigned int i = 0; i < nthreads; i++) {
			numa_free(args[i].data, args[i].sz * sizeof(int32_t));
		}
	}

	cout << "Adjacent scan:" << total << " msecs" << endl;

	// total = 0;
	// for (int round = 0; round < rounds; round++) {
	// 	// scan adjacent
	// 	for (unsigned int i = 0; i < nthreads; i++) {
	// 		args[i].sz = ntuples;
	// 		args[i].cpu = i;
	// 		args[i].build_cpu = (i+16) % 64;
	// 	}

	// 	for (unsigned int i = 0; i < nthreads; i++)
	// 		pthread_create(&threads[i], NULL, do_build, (void*)&args[i]);

	// 	for (unsigned int i = 0; i < nthreads; i++)
	// 		pthread_join(threads[i], NULL);

	// 	t = micro_time();
	// 	for (unsigned int i = 0; i < nthreads; i++)
	// 		pthread_create(&threads[i], NULL, do_scan, (void*)&args[i]);

	// 	for (unsigned int i = 0; i < nthreads; i++)
	// 		pthread_join(threads[i], NULL);
		
	// 	total += (micro_time() -t);

	// 	for (unsigned int i = 0; i < nthreads; i++) {
	// 		numa_free(args[i].data, args[i].sz * sizeof(int32_t));
	// 	}
	// }

	// cout << "2 hops scan:" << total << " msecs" << endl;

	// scan local
	total = 0;
	for (int round = 0; round < rounds; round++) {

		for (unsigned int i = 0; i < nthreads; i++) {
			args[i].sz = ntuples;
			args[i].cpu = i;
			args[i].build_cpu = i;
			//			args[i].data = (int32_t*)numa_alloc_onnode(sizeof(int32_t) * args[i].sz, numa_node_of_cpu(args[i].build_cpu));
			//			memset(args[i].data, 0, sizeof(int32_t) * args[i].sz);

		}

		for (unsigned int i = 0; i < nthreads; i++)
			pthread_create(&threads[i], NULL, do_build, (void*)&args[i]);

		for (unsigned int i = 0; i < nthreads; i++)
			pthread_join(threads[i], NULL);

		for (unsigned int i = 0; i < nthreads; i++)
			pthread_create(&threads[i], NULL, flush_cache, (void*)&args[i]);

		for (unsigned int i = 0; i < nthreads; i++)
			pthread_join(threads[i], NULL);

		t = micro_time();
		for (unsigned int i = 0; i < nthreads; i++)
			pthread_create(&threads[i], NULL, do_scan, (void*)&args[i]);

		for (unsigned int i = 0; i < nthreads; i++)
			pthread_join(threads[i], NULL);
		total += (micro_time() - t);

		for (unsigned int i = 0; i < nthreads; i++) {
			numa_free(args[i].data, args[i].sz * sizeof(int32_t));
		}
	}

	cout << "Local scan:" << total << " msecs" << endl;

}
