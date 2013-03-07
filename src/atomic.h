#ifndef ATOMIC_H_
#define ATOMIC_H_

#include <stdint.h>
#include <pthread.h>
#include "numadb.h"

typedef pthread_mutex_t Mutex;

void mutex_init(Mutex* lock);
void mutex_destroy(Mutex* lock);
void mutex_lock(Mutex* lock);
void mutex_unlock(Mutex* lock);

inline void atomic_add_32(volatile val_t *des, int32_t src)
{
	__asm__ __volatile__("spin0:\tmovl %0, %%eax\n\t"
		"movl %%eax, %%edx\n\t"
		"addl %1, %%edx\n\t"
		"lock\n\t"
		"cmpxchgl %%edx, %0\n\t"
		"jnz spin0\n\t"
		:"=m"(*des)
		:"r"(src),"m"(*des)
		:"memory","%eax", "%edx"
		);
}

inline void atomic_add_64(volatile val_t *des, int64_t src)
{
	__asm__ __volatile__("spi1:\tmovq %0, %%rax\n\t"
		"movq %%rax, %%rdx\n\t"
		"addq %1, %%rdx\n\t"
		"lock\n\t"
		"cmpxchg %%rdx, %0\n\t"
		"jnz spi1\n\t"
		:"=m"(*des)
		:"r"(src),"m"(*des)
		:"memory","%rax", "%rdx"
		);
}


#endif // ATOMIC_H_
