#include <pthread.h>
#include <numa.h>

#include "types.h"
#include "taskqueue.h"
#include "util.h"

void* work_thread(void *param)
{
  thread_t *my = (thread_t*)param;
  int cpu = my->cpu;
  cpu_bind(cpu);

  Taskqueue *queue = my->node->queue;
  Task *task = NULL;

  while (1) {
    task = NULL; // reset task

    // check termination
    if (my->env->done())
      break;

    if (my->localtasks) {
      task = my->localtasks->Fetch();
      if (task == NULL) { // local tasks are exhausted
        delete my->localtasks;
        my->localtasks = NULL;
      }
    }
    LOG(INFO) << "Local not found.";

    if (!task) {
      task = queue->Fetch();
      // if current queue is build or probe
      // we will expand the tasks to our local queue
    }

    LOG(INFO) << "Node queue not found.";


    if (!task) {
      // we have to steal
      // currently, we only steal local probe and remote partition
      // If build cannot be stolen, there is a potential
      // problem of unbalanced load

      // do we have buffered stolen work?
      if (my->stolentasks) {
        task = my->stolentasks->Fetch();
        if (task == NULL)
          my->stolentasks = NULL;
      }
    }

    if (!task) {
      // is there local probe work we can steal?
      thread_t **groups = my->node->groups;
      for (int i = 0; i < my->node->nthreads; i++) {
        thread_t *t = groups[i];
        if (t->tid == my->tid) // skip myself
          continue;
        if (t->localtasks && t->localtasks->type()==OpProbe) { // it has local probing task
          my->stolentasks = t->localtasks; // steal it!
          break;
        }
      }
      
      if (my->stolentasks) {
        LOG(INFO) << "Haha. We have stolen a local probing work.";
      }
      else {
        // there is no local probing we can steal
        // is there global partitioning we can steal?
        node_t *nodes = my->env->nodes();
				
        for (int i = 0; i < my->env->nnodes(); i++) {
          if (i == my->node_id) // skip my node
            continue;
          Tasklist *part_tasks = nodes[i].queue->GetListByType(OpPartition);
          if (part_tasks) {
            my->stolentasks = part_tasks;
            break;
          }
        }
        if (my->stolentasks) {
          LOG(INFO) << "Heyhey, we have stolen a remote partition work.";
        }
      }
      
      if (my->stolentasks) {
        task = my->stolentasks->Fetch();
      }
    }
    
    if (task) {
      LOG(INFO) << "Grab a new task.";
      task->Run(my);
      delete task;
    }
    // otherwise there is no work now, we can sleep
    //    else
    //      sleep(1); // sleep for 1 second
  }


  LOG(INFO) << "Thread " << my->tid << " is existing.";
  return NULL;
}

// two way hash join
void Hashjoin(Table *relR, Table *relS, int nthreads)
{
  // init the environment
  Environment *env = new Environment(nthreads);
  LOG(INFO) << "Environment set up.";

  // init the task queues
  env->TestPartition(relR, relS);

  LOG(INFO) << "Task initialized.";

  pthread_t threads[nthreads];
  // start threads
  for (int i = 0; i < nthreads; i++) {
    pthread_create(&threads[i], NULL, work_thread, (void*)&env->threads()[i]);
  }

  // join threads
  for (int i = 0; i < nthreads; i++) {
    pthread_join(threads[i], NULL);
  }

  LOG(INFO) << "All threads join.";

  delete env;
}
