#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include <pthread.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <emmintrin.h>
#include <unistd.h>
#include <sched.h>
#include <math.h>
#include <numa.h>

static uint64_t micro_time(void)
{
	struct timeval t;
	struct timezone z;
	gettimeofday(&t, &z);
	return t.tv_sec * 1000000 + t.tv_usec;
}

static int cpus(void)
{
	char cpu_name[40];
	struct stat st;
	strcpy(cpu_name, "/sys/devices/system/cpu/cpu");
	char *cpu_num_ptr = &cpu_name[strlen(cpu_name)];
	int cpu_num = -1;
	do {
		sprintf(cpu_num_ptr, "%d", ++cpu_num);
	} while (stat(cpu_name, &st) == 0);
	return cpu_num;
}

static void cpu_bind(int cpu_id)
{
	cpu_set_t cpu_set;
	CPU_ZERO(&cpu_set);
	CPU_SET(cpu_id, &cpu_set);
	pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpu_set);
}

static void memory_bind(int cpu_id)
{
	char numa_id_str[12];
	struct bitmask *numa_node;
	int numa_id = numa_node_of_cpu(cpu_id);
	sprintf(numa_id_str, "%d", numa_id);
	numa_node = numa_parse_nodestring(numa_id_str);
	numa_set_membind(numa_node);
	numa_free_nodemask(numa_node);
}

static void *mamalloc(size_t size)
{
	void *ptr = NULL;
	return posix_memalign(&ptr, 64, size) ? NULL : ptr;
}

static void schedule_threads(int *cpu, int *numa_node, int threads, int numa)
{
	int t, max_threads = cpus();
	int *thread_numa = (int*)malloc(max_threads * sizeof(int));
	for (t = 0 ; t != max_threads ; ++t)
		thread_numa[t] = numa_node_of_cpu(t);
	for (t = 0 ; t != threads ; ++t) {
		int i, n = t % numa;
		for (i = 0 ; i != max_threads ; ++i)
			if (thread_numa[i] == n) break;
		assert(i != max_threads);
		thread_numa[i] = -1;
		cpu[t] = i;
		if (numa_node != NULL)
			numa_node[t] = n;
		assert(numa_node_of_cpu(i) == n);
	}
	free(thread_numa);
}

typedef struct {
	uint64_t **data;
	uint64_t *size;
	uint64_t total_size;
	double *prob;
	double theta;
	double *factor;
	int threads;
	int ranks;
	int numa;
	int bits_on;
	int *cpu;
	int *numa_node;
	double *factors;
	pthread_barrier_t *barrier;
} numa_zipf_global_data_t;

typedef struct {
	int id;
	uint64_t seed;
	uint64_t *bits;
	uint64_t checksum;
	numa_zipf_global_data_t *global;
} numa_zipf_thread_data_t;

static inline uint64_t rand64_r(unsigned int *seed)
{
	uint64_t x = rand_r(seed) >> 9;
	uint64_t y = rand_r(seed) >> 10;
	uint64_t z = rand_r(seed) >> 10;
	return (x << 42) | (y << 21) | z;
}

static void *zipf_thread(void *arg)
{
	numa_zipf_thread_data_t *a = (numa_zipf_thread_data_t*) arg;
	numa_zipf_global_data_t *d = a->global;
	int i, t, id = a->id;
	int numa = d->numa;
	int numa_node = d->numa_node[id];
	int threads = d->threads;
	int threads_per_numa = threads / numa;
	// id in local numa threads
	int numa_local_id = 0;
	for (i = 0 ; i != id ; ++i)
		if (d->numa_node[i] == numa_node)
			numa_local_id++;
	// bind thread and its allocation
	cpu_bind(d->cpu[id]);
	memory_bind(d->cpu[id]);
	// allocate space
	uint64_t p, numa_size = d->size[numa_node];
	uint64_t total_size = d->total_size;
	// allocate space for data
	if (!numa_local_id && d->data[numa_node] == NULL)
      d->data[numa_node] = (uint64_t*)mamalloc(numa_size * sizeof(uint64_t));
	// offsets of probability array
	uint64_t size = d->total_size / threads;
	uint64_t offset = size * id;
	if (id + 1 == threads)
		size = total_size - offset;
	uint64_t end = offset + size;
	// compute probability sum (across all threads)
	double factor = 0.0;
	double neg_theta = -d->theta;
	double dp = offset;
	for (p = offset ; p != end ; ++p) {
		dp += 1.0;
		factor += pow(dp, neg_theta);
	}
	d->factor[id] = factor;
	// synchronize
	pthread_barrier_wait(&d->barrier[0]);
	// compute total factor from all threads
	factor = 0.0;
	for (t = 0 ; t != threads ; ++t)
		factor += d->factor[t];
	// compute probability sum up to this point
	double sum = 0.0;
	for (t = 0 ; t != id ; ++t)
		sum += d->factor[t];
	sum /= factor;
	// local probability array boundaries
	double *prob = d->prob;
	dp = offset;
	for (p = offset ; p != end ; ++p) {
		dp += 1.0;
		sum += pow(dp, neg_theta) / factor;
		prob[p] = sum;
	}
	// synchronize
	pthread_barrier_wait(&d->barrier[1]);
	// offsets of rank array
	size = numa_size / threads_per_numa;
	offset = size * numa_local_id;
	if (numa_local_id + 1 == threads_per_numa)
		size = numa_size - offset;
	uint64_t *data = &d->data[numa_node][offset];
	uint64_t *data_end = &data[size];
	uint64_t checksum = 0;
	uint64_t seed_64 = a->seed;
	unsigned short *seed = (unsigned short*) &seed_64;
	if (size) do {
		// binary search random item
		uint64_t low = 0;
		uint64_t high = total_size - 1;
		double x = erand48(seed);
		do {
			uint64_t mid = (low + high) >> 1;
			if (x <= prob[mid])
				high = mid;
			else
				low = mid + 1;
		} while (low != high);
		*data++ = low;
		checksum += low;
	} while (data != data_end);
	// update bits
	if (d->bits_on) {
      uint64_t *bits = (uint64_t*)calloc(65, sizeof(uint64_t));
		uint64_t m1 = -1;
		data -= size;
		if (size) do {
			// update bits
			uint64_t x = *data++;
			asm("bsrq	%0, %0\n\t"
			    "cmovzq	%1, %0"
			 : "=r"(x) : "r"(m1), "0"(x) : "cc");
			bits[x + 1]++;
		} while (data != data_end);
		a->bits = bits;
	}
	// re-write as unique 64-bit values using FNV hashing
	if (!d->ranks) {
		const uint64_t fnv_prime = 1099511628211ull;
		const uint64_t fnv_offset = 14695981039346656037ull;
		data -= size;
		checksum = 0;
		if (size) do {
			uint8_t *octet = (uint8_t*) data;
			uint64_t p, x = fnv_offset;
			for (p = 0 ; p != 8 ; ++p) {
				x ^= octet[p];
				x *= fnv_prime;
			}
			*data++ = x;
			checksum += x;
		} while (data != data_end);
	}
	a->checksum = checksum;
	pthread_exit(NULL);
}

uint64_t zipf(uint64_t **data, uint64_t *size, int numa,
              double theta, int ranks, uint64_t *bits)
{
	int max_threads = cpus();
	int i, n, t, threads = 0;
	for (t = 0 ; t != max_threads ; ++t)
		if (numa_node_of_cpu(t) < numa)
			threads++;
	pthread_barrier_t barrier[3];
	for (t = 0 ; t != 3 ; ++t)
		pthread_barrier_init(&barrier[t], NULL, threads);
	numa_zipf_global_data_t global;
	global.threads = threads;
	global.theta = theta;
	global.numa = numa;
	global.data = data;
	global.size = size;
	global.ranks = ranks;
	global.bits_on = bits != NULL;
	global.barrier = barrier;
	global.total_size = 0;
	for (n = 0 ; n != numa ; ++n)
		global.total_size += size[n];
	global.prob = (double*)numa_alloc_interleaved(global.total_size * sizeof(double));
	global.factor = (double*)malloc(threads * sizeof(double));
	global.cpu = (int*)malloc(threads * sizeof(int));
	global.numa_node = (int*)malloc(threads * sizeof(int));
	schedule_threads(global.cpu, global.numa_node, threads, numa);
	numa_zipf_thread_data_t *thread_data = (numa_zipf_thread_data_t*)malloc(threads * sizeof(numa_zipf_thread_data_t));
	pthread_t *id = (pthread_t*)malloc(threads * sizeof(pthread_t));
	srand(micro_time());
	for (t = 0 ; t != threads ; ++t) {
		thread_data[t].id = t;
		thread_data[t].seed = rand();
		thread_data[t].global = &global;
		pthread_create(&id[t], NULL, zipf_thread, (void*) &thread_data[t]);
	}
	uint64_t checksum = 0;
	for (t = 0 ; t != threads ; ++t) {
		pthread_join(id[t], NULL);
		checksum += thread_data[t].checksum;
	}
	if (bits != NULL) {
		for (i = 0 ; i != 65 ; ++i)
			bits[i] = 0;
		for (t = 0 ; t != threads ; ++t) {
			for (i = 0 ; i != 65 ; ++i)
				bits[i] += thread_data[t].bits[i];
			free(thread_data[t].bits);
		}
	}
	for (t = 0 ; t != 3 ; ++t)
		pthread_barrier_destroy(&barrier[t]);
	free(global.factor);
	free(global.numa_node);
	free(thread_data);
	numa_free(global.prob, global.total_size * sizeof(double));
	free(global.cpu);
	free(id);
	return checksum;
}
/*
int main(int argc, char **argv)
{
	int numa = numa_max_node() + 1;
	int i, threads = cpus(); uint64_t j;
	uint64_t gigs = argc > 1 ? atoi(argv[1]) : 10;
	double theta = argc > 2 ? atof(argv[2]) : 1.0;
	uint64_t tuples = gigs * 1024 * 1024 * 128;
	uint64_t *data[numa], size[numa];
	for (i = 0 ; i != numa ; ++i) {
		size[i] = tuples / numa;
		data[i] = NULL;
	}
	fprintf(stderr, "Size:  %ld GB\n", gigs);
	fprintf(stderr, "Tuples: %ld\n", tuples);
	fprintf(stderr, "Theta: %.2f\n", theta);
	uint64_t bits[65];
	uint64_t t = micro_time();
	zipf(data, size, numa, theta, 1, bits);
	t = micro_time() - t;
	uint64_t sum = 0;
	for (i = 0 ; i != 65 ; ++i) {
		sum += bits[i];
		fprintf(stderr, "%2d bits: %.2f%% (%ld/%ld)\n",
		        i, sum * 100.0 / tuples, sum, tuples);
		if (sum == tuples) break;
	}
	fprintf(stderr, "Time: %ld us\n", t);
	fprintf(stderr, "Rate: %.1f GB / sec\n", gigs * 1000000.0 / t);
	if (argc > 3) {
		FILE *fp = fopen(argv[3], "wb");
		for (i = 0 ; i != numa ; ++i) {
			uint64_t *d = data[i];
			for (j = 0 ; j != size[i] ; ++j)
				fwrite(&d[j], 8, 1, fp);
		}
		fclose(fp);
	}
	return EXIT_SUCCESS;
}
*/
