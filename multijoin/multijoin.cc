#include <iostream>
#include <pthread.h>
#include <numa.h>

#include "types.h"
#include "taskqueue.h"
#include "util.h"

#include "perf.h"

using namespace std;

inline void run_task(Task *task, thread_t *my)
{
  task->Run(my);      
  //  delete task;
}

void* work_thread(void *param)
{
  thread_t *my = (thread_t*)param;
  int cpu = my->cpu;
  cpu_bind(cpu);

#if PER_CORE==1
  perf_register_thread();
  my->perf = perf_init();
  perf_start(my->perf);
#endif

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
    if (my->stolentasks) {
      task = my->stolentasks->FetchAtomic();
      
      // case 1: we steal a job
      if (task) {
        my->remote++;
        run_task(task, my);
        continue;
      }
      // case 2: the job we trying to steal is empty or has already exhausted
      else {
        my->stolentasks = NULL;
      }
    }

    assert(my->stolentasks == NULL);

    /*    
    if (!my->stolentasks) {
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

    
    if (!my->stolentasks) {
      // there is no local work we can steal
      // is there global partitioning we can steal?
      node_t *nodes = my->env->nodes();				
      int steal_node, next_node;
      for (int i = 0; i < my->env->nnodes(); i++) {
        do {
          steal_node = my->node->next_node;
          next_node = (steal_node == my->env->nnodes() -1 ) ? 0 : steal_node + 1;
        } while (!__sync_bool_compare_and_swap(&my->node->next_node, steal_node, next_node));

        if (steal_node == my->node_id) // skip my node
          continue;

        // TODO: GLOBAL STEAL NOT IMPLEMENTED 
        Tasklist *part_tasks = nodes[steal_node].queue->GetListByType(OpPartition);

        if (part_tasks) {
          my->stolentasks = part_tasks;
          break; // job stolen, we can leave now
        }
      }
    }
    */
  }

#if PER_CORE==1
  perf_stop(my->perf);
#if PERF_ALL == 1
  perf_read(my->perf);
#endif
  //  perf_destroy(my->perf);
  perf_unregister_thread();
#endif
  return NULL;
}

void Run(Environment *env)
{

#ifdef USE_PERF
#if PER_SYSTEM==1
  perf_t *perf = perf_init();
  perf_start(perf);
#endif  
#endif

  long t = micro_time();

  pthread_t threads[env->nthreads()];
  // start threads
  for (int i = 0; i < env->nthreads(); i++) {
    pthread_create(&threads[i], NULL, work_thread, (void*)&env->threads()[i]);
  }

  // join threads
  for (int i = 0; i < env->nthreads(); i++)
    pthread_join(threads[i], NULL);

  t = (micro_time() - t) / 1000;


#ifdef USE_PERF
#if PER_SYSTEM==1
  perf_read(perf);
  perf_stop(perf);
  perf_print(perf);
  //  perf_destroy(perf);
#endif

#if PER_CORE == 1
  thread_t *args = env->threads();
  for (int i = 0; i < env->nthreads(); i++) {
    //    cout << "Thread " << i << ":" << endl;
    //    perf_print(args[i].perf);
    if (i != 0) 
      perf_aggregate(args[0].perf, args[i].perf);
  }
  cout << "Aggregate on thread 0:" << endl;
  perf_print(args[0].perf);
#endif
#endif

  thread_t *infos = env->threads();
  for (int i = 0; i < env->nthreads(); i++) {
    cout << "Thread[" << infos[i].cpu << "]:"
         << infos[i].local << ","
         << infos[i].shared << ","
         << infos[i].remote << endl;
  }

  LOG(INFO) << "Running time: " << t << " msec";

  LOG(INFO) << "All threads join.";

  Table *tb = env->GetTable(0);
  long long sum = 0;
  for (uint32_t node = 0; node < tb->nnodes(); ++node) {
    list<Partition*> &ps = tb->GetPartitionsByNode(node);
    size_t size = 0;
    for (list<Partition*>::iterator it = ps.begin();
         it != ps.end(); ++it) {
      size += (*it)->size();
      tuple_t *tuple = (*it)->tuples();
      for (uint32_t i = 0; i < (*it)->size(); i++) {
        sum += tuple[i].key;
      }
    }
  }
  LOG(INFO) << "sum 0: " << sum;
  
  tb = env->GetTable(1);
  long long sum1 = 0;
  for (uint32_t key = 0; key < tb->nkeys(); key++) {
    list<Partition*> &ps = tb->GetPartitionsByKey(key);
    size_t size = 0;
    for (list<Partition*>::iterator it = ps.begin();
         it != ps.end(); ++it) {
      size += (*it)->size();
      tuple_t *tuple = (*it)->tuples();
      for (uint32_t i = 0; i < (*it)->size(); i++) {
        sum1 += tuple[i].key;
      }
    }
    // LOG(INFO) << "part " << key << " size: " << size;
  }
  LOG(INFO) << "sum 1: " << sum1;
  assert(sum1 == sum);

  
  tb = env->GetTable(2);
  long long sum2= 0;
  for (uint32_t key = 0; key < tb->nkeys(); key++) {
    list<Partition*> &ps = tb->GetPartitionsByKey(key);
    size_t size = 0;
    for (list<Partition*>::iterator it = ps.begin();
         it != ps.end(); ++it) {
      size += (*it)->size();
      tuple_t *tuple = (*it)->tuples();
      for (uint32_t i = 0; i < (*it)->size(); i++)
        sum2 += tuple[i].key;
    }
       // LOG(INFO) << "part " << key << " size: " << size;
  }
  LOG(INFO) << "sum 2: " << sum2;
  assert(sum2 == sum);
}
