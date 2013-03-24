#include <iostream>
#include <pthread.h>
#include <numa.h>

#include "types.h"
#include "taskqueue.h"
#include "util.h"

using namespace std;

inline void run_task(Task *task, thread_t *my)
{
  task->Run(my);      
  delete task;
}

void* work_thread(void *param)
{
  thread_t *my = (thread_t*)param;
  int cpu = my->cpu;
  cpu_bind(cpu);

  Taskqueue *queue = my->node->queue;

  // TODO: change busy waiting to signal
  while (1) {
    Task *task = NULL;

    // check termination
    if (my->env->done())
      break;
    
    // poll local tasks
    if (my->localtasks) {
      task = my->localtasks->FetchAtomic();
      if (task) {
        my->local++;
        run_task(task, my);
        continue;
      }
      else if (my->localtasks->exhausted())
        my->localtasks = NULL;
    }

    // poll node shared tasks
    task = queue->Fetch();
    if (task) {
      my->shared++;
      run_task(task, my);
      continue;
    }

    // poll buffered stolen task
    if (my->stolentasks) {
      task = my->stolentasks->FetchAtomic();
      if (task) {
        my->remote++;
        run_task(task, my);
        continue;
      }
      else if (my->stolentasks->exhausted())
        my->stolentasks = NULL;
    }

    if (!my->stolentasks) {
      // no bufferd task available
      // is there local probe work we can steal?
      thread_t **groups = my->node->groups;
      int next_cpu = my->node->next_cpu;
      for (int i = 0; i < my->node->nthreads; i++) {
        thread_t *t = groups[next_cpu];
        if (t->tid == my->tid) // skip myself
          continue;
        ++next_cpu;
        if (next_cpu == my->node->nthreads)
          next_cpu = 0;

        if (t->localtasks) {
          my->stolentasks = t->localtasks; // steal it!

          pthread_mutex_lock(&my->node->lock);
          my->node->next_cpu = next_cpu;
          pthread_mutex_unlock(&my->node->lock);

          break;
        }
      }
    }


    if (!my->stolentasks) {
      // there is no local probing we can steal
      // is there global partitioning we can steal?
      node_t *nodes = my->env->nodes();				
      int next_node = my->node->next_node;
      for (int i = 0; i < my->env->nnodes(); i++) {
        if (next_node == my->node_id) // skip my node
          continue;

        Tasklist *part_tasks = nodes[next_node].queue->GetListByType(OpPartition);
        ++next_node;
        if (next_node == my->env->nnodes())
          next_node = 0;

        if (part_tasks) {
          my->stolentasks = part_tasks;
          //          LOG(INFO) << "Heyheyhey.";
          break;
        }
      }

      pthread_mutex_lock(&my->node->lock);
      my->node->next_node = next_node;
      pthread_mutex_unlock(&my->node->lock);
    }
  }


  //  LOG(INFO) << "Thread " << my->tid << " is existing.";
  return NULL;
}

// two way hash join
void Hashjoin(Table *relR, Table *relS, int nthreads)
{
  // init the environment
  Environment *env = new Environment(nthreads);
  LOG(INFO) << "Environment set up.";

  // init the task queues
  env->CreateJoinTasks(relR, relS);

  LOG(INFO) << "Task initialized.";

  long t = micro_time();

  pthread_t threads[nthreads];
  // start threads
  for (int i = 0; i < nthreads; i++) {
    pthread_create(&threads[i], NULL, work_thread, (void*)&env->threads()[i]);
  }

  // join threads
  //thread_t *infos = env->threads();
  for (int i = 0; i < nthreads; i++) {
    pthread_join(threads[i], NULL);
    // cout << "Thread[" << i << "]:"
    //      << infos[i].local << ","
    //      << infos[i].shared << ","
    //      << infos[i].remote << endl;
  }

  t = (micro_time() - t) / 1000;
  LOG(INFO) << "Running time: " << t << " msec";

  LOG(INFO) << "All threads join.";

  delete env;
}
