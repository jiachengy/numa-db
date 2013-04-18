#include <iostream>
#include <pthread.h>
#include <papi.h>
#include <numa.h>

#include "table.h"
#include "types.h"
#include "taskqueue.h"
#include "util.h"

#include "perf.h"

#define PRINT_PER_THREAD

using namespace std;

inline void run_task(Task *task, thread_t *my)
{
  task->Run(my);      
  //  delete task;
}

void steal_local(thread_t *my)
{
  // no bufferd task available
  // is there local pass2 partition / probe work we can steal?
  thread_t **groups = my->node->groups;
  uint32_t steal_cpu, next_cpu;
  for (uint32_t i = 0; i < my->node->nthreads; i++) {
    // use compare and swap to update the stealing
    do {
      steal_cpu = my->node->next_cpu;
      next_cpu = (steal_cpu == my->node->nthreads - 1) ? 0 : steal_cpu + 1;
    } while (!__sync_bool_compare_and_swap(&my->node->next_cpu, steal_cpu, next_cpu));
                 
    thread_t *t = groups[steal_cpu];
    if (t->tid == my->tid) // skip myself
      continue;

    if (t->localtasks) {
      my->stolentasks = t->localtasks; // steal it!
      break; // we can leave now
    }
  }
}

Task * steal_remote(thread_t *my)
{
  node_t *nodes = my->env->nodes();				
  int steal_node, next_node;
  for (int i = 0; i != my->env->nnodes(); i++) {
    do {
      steal_node = my->node->next_node;
      next_node = (steal_node == my->env->nnodes() -1 ) ? 0 : steal_node + 1;
    } while (!__sync_bool_compare_and_swap(&my->node->next_node, steal_node, next_node));

    if (steal_node == my->node_id) // skip my node
      continue;
        
    // steal from node queue first
    node_t * target = &nodes[steal_node];
    Task * task = target->queue->Fetch();
    if (task)
      return task;
    
    // steal from local list
    thread_t **groups = target->groups;
    // check if there is work to steal on this node
    uint32_t steal_cpu, next_cpu;
    for (uint32_t i = 0; i != target->nthreads; i++) {
      // use compare and swap to update the stealing
      do {
        steal_cpu = target->next_cpu;
        next_cpu = (steal_cpu == target->nthreads - 1) ? 0 : steal_cpu + 1;
      } while (!__sync_bool_compare_and_swap(&target->next_cpu, steal_cpu, next_cpu));
                 
      thread_t *t = groups[steal_cpu];
      if (t->localtasks) {
        my->stolentasks = t->localtasks; // steal it!
        break; // we can leave now
      }
    }

    if (my->stolentasks != NULL)
      break;
  }
  return NULL;
}

void* work_thread(void *param)
{
  thread_t *my = (thread_t*)param;

#if PER_CORE==1
  perf_register_thread();
  my->perf = perf_init();
  perf_start(my->perf);
  perf_counter_t before = perf_read(my->perf);

#endif

  int putbacks = 0;
  int cpu = my->cpu;
  cpu_bind(cpu);


  Taskqueue *queue = my->node->queue;

  // busy waiting or sleep?
  while (1) {
    Task *task = NULL;

    // check termination
    if (my->env->done())
      break;
    
    // poll local tasks
    if (my->localtasks) {
      task = my->localtasks->FetchAtomic();
      
      // case 1: local task available
      if (task) {
        my->local++;
        run_task(task, my);
        continue;
      }
      // case 2: local task exhausted
      else if (my->localtasks->exhausted()) {
        my->batch_task = NULL;
        my->localtasks = NULL;
      }
      // case 3: local tasks are not ready yet
      else { 
        ++putbacks;
        queue->Putback(my->batch_task->id(), my->batch_task);
        my->batch_task = NULL;
        my->localtasks = NULL;
      }
    }

    assert(my->localtasks == NULL);

    // poll node shared tasks
    task = queue->Fetch();
    if (task) {
      my->shared++;
      run_task(task, my);
      continue;
    }

    assert(task == NULL);

    // poll buffered stolen task
    // if (my->stolentasks) {
    //   task = my->stolentasks->FetchAtomic();
      
    //   // case 1: we steal a job
    //   if (task) {
    //     my->remote++;
    //     run_task(task, my);
    //     continue;
    //   }
    //   // case 2: the job we trying to steal is empty or has already exhausted
    //   else {
    //     my->stolentasks = NULL;
    //   }
    // }

    // assert(my->stolentasks == NULL);

    // if (!my->stolentasks)
    //   steal_local(my);

    
    // if (!my->stolentasks) {
    //   task = steal_remote(my);
    //   if (task) {
    //     my->remote++;
    //     run_task(task, my);
    //   }
    // }
  }

  logging("Putbacks: %d\n", putbacks);
  logging("Remaining partition: %d\n", my->memm->available());

#if PER_CORE == 1
  perf_stop(my->perf);
  perf_counter_t after = perf_read(my->perf);
  my->total_counter = perf_counter_diff(before, after);

  perf_destroy(my->perf);
  perf_unregister_thread();
#endif

  return NULL;
}

void Run(Environment *env)
{
#if PER_SYSTEM == 1
  perf_t *perf = perf_init();
  perf_start(perf);
  perf_counter_t before = perf_read(perf);
#endif
  pthread_t threads[env->nthreads()];

  long t = PAPI_get_real_usec();

  // start threads
  for (int i = 0; i < env->nthreads(); i++)
    pthread_create(&threads[i], NULL, work_thread, (void*)&env->threads()[i]);

  // join threads
  for (int i = 0; i < env->nthreads(); i++)
    pthread_join(threads[i], NULL);

  t = PAPI_get_real_usec() - t;

  logging("All threads join.\n");

#if PER_SYSTEM == 1
  perf_counter_t after = perf_read(perf);
  perf_counter_t count = perf_counter_diff(before, after);
#elif PER_CORE == 1
  thread_t *args = env->threads();
  perf_counter_t partition_aggr = PERF_COUNTER_INITIALIZER;
  perf_counter_t build_aggr = PERF_COUNTER_INITIALIZER;
  perf_counter_t probe_aggr = PERF_COUNTER_INITIALIZER;
  perf_counter_t total_aggr = PERF_COUNTER_INITIALIZER;

  for (int i = 0; i != env->nthreads(); ++i) {
    perf_counter_aggr(&partition_aggr, args[i].stage_counter[0]);
    perf_counter_aggr(&build_aggr, args[i].stage_counter[1]);
    perf_counter_aggr(&probe_aggr, args[i].stage_counter[2]);
    perf_counter_aggr(&total_aggr, args[i].total_counter);
#ifdef PRINT_PER_THREAD
    logging("Thread[%d] partition: %ld usec\n", i, args[i].stage_counter[0].tick);
    logging("Thread[%d] build: %ld usec\n", i, args[i].stage_counter[1].tick);
    logging("Thread[%d] probe: %ld usec\n", i, args[i].stage_counter[2].tick);
    logging("Thread[%d] time: %ld  usec\n", i, args[i].total_counter.tick);
#endif

  }
  logging("Aggregate partition counter:\n");
  perf_print(partition_aggr);
  logging("Aggregate build counter:\n");
  perf_print(build_aggr);
  logging("Aggregate probe counter:\n");
  perf_print(probe_aggr);
  logging("Aggregate total counter:\n");
  perf_print(total_aggr);

  logging("average partition: %.2f usec\n", 1.0 * partition_aggr.tick / env->nthreads());
  logging("average build: %.2f usec\n", 1.0 * build_aggr.tick / env->nthreads());
  logging("average probe: %.2f usec\n", 1.0 * probe_aggr.tick / env->nthreads());
  logging("average total: %.2f usec\n", 1.0 * total_aggr.tick / env->nthreads());

#endif

#ifdef PRINT_PER_THREAD
  thread_t *infos = env->threads();
  for (int i = 0; i < env->nthreads(); i++) {
    cout << "Thread[" << infos[i].cpu << "]:"
         << infos[i].local << ","
         << infos[i].shared << ","
         << infos[i].remote << endl;
  }
#endif

  logging("Running time: %ld usec\n", t);

  //  Table *tr = env->GetTable(0);
  //  Table *trpass1 = env->GetTable(1);
  //  Table *trpass2 = env->GetTable(2);
  // Table *ts = env->GetTable(3);
  // Table *tspass1 = env->GetTable(4);
  // Table *tspass2 = env->GetTable(5);

  // int shift1 = Params::kOffsetPass1;
  // int mask1 = (((1 << Params::kNumBitsPass1) - 1) << shift1);
  // int shift2 = Params::kNumRadixBits;
  // int mask2 = (1 << Params::kNumRadixBits) - 1;
  
  // long long sum0 = tr->Validate(0, 0);
  // long long sum1 = trpass1->Validate(mask1, shift1);
  // long long sum2 = trpass2->Validate(mask2, shift2);

  // logging("sum: %lld, sum1: %lld, sum2: %lld\n", sum0, sum1, sum2);

  Table *output = env->output_table();
  logging("matches: %ld\n", output->tuples());

#if PER_SYSTEM == 1
  perf_stop(perf);
  perf_destroy(perf);
#endif
}

