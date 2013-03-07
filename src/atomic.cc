#include "numadb.h"
#include "atomic.h"
#include <pthread.h>

void mutex_init(Mutex* lock)
{
	pthread_mutex_init(lock, NULL);
}

void mutex_destroy(Mutex* lock)
{
	pthread_mutex_destroy(lock);
}


void mutex_lock(Mutex* lock)
{
	pthread_mutex_lock(lock);
}

void mutex_unlock(Mutex* lock)
{
	pthread_mutex_unlock(lock);
}
