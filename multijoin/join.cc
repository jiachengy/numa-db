#include <smmintrin.h>

#include "params.h"
#include "hashjoin.h"
#include "perf.h"

inline size_t get_hist_size(size_t tuples) 
{
  NEXT_POW_2(tuples);
  tuples >>= 2;
  if(tuples < 4) tuples = 4; 
  return tuples;
}

partition_t * BuildTask::Build(thread_t * my)
{
  list<partition_t*> &parts = in_->GetPartitionsByKey(radix_);

  size_t total_tuples = 0;
  for(list<partition_t*>::iterator it = parts.begin();
      it != parts.end(); ++it )
    total_tuples += (*it)->tuples;

  size_t partitions = get_hist_size(total_tuples);
  int shift = Params::kNumRadixBits;
  int32_t mask  = (partitions-1) << shift;

  // preallocate the folllowing in thread
  my->hist   = (uint32_t*)realloc(my->hist, partitions * sizeof(uint32_t));
  memset(my->hist, 0x0, partitions * sizeof(uint32_t));
  my->part = (tuple_t**)realloc(my->part, partitions * sizeof(tuple_t*));   // temp output buffer
  
  uint32_t *hist = my->hist;
  tuple_t **part = my->part;

  // dynamic request from memm
  partition_t *htp = my->memm->GetHashtable(total_tuples, partitions + 1);
  assert(htp->hashtable != NULL);
  htp->radix = radix_;
  uint32_t *sum = htp->hashtable->sum;

  for(list<partition_t*>::iterator it = parts.begin();
      it != parts.end(); ++it ) {
    tuple_t * tuple = (*it)->tuple;
    size_t tuples = (*it)->tuples;
    for (uint64_t i = 0; i != tuples ; ++i) {
      uint32_t hash = mhash(tuple[i].key, mask, shift);
      hist[hash]++;
    }
  }

  /* prefix sum on histogram */
  for( uint32_t i = 0; i != partitions; i++ ) {
    sum[i] = i ? sum[i-1] + hist[i-1] : 0;
    part[i] = htp->hashtable->tuple + sum[i];
  }
  sum[partitions] = total_tuples;

  for(list<partition_t*>::iterator it = parts.begin();
      it != parts.end(); ++it ) {
    tuple_t * tuple = (*it)->tuple;
    for (uint64_t i = 0; i < (*it)->tuples; ++i) {
      uint32_t hash = mhash(tuple[i].key, mask, shift);
      *(part[hash]++) = tuple[i];
    }
  }



  /*
  // validate
  long long sum = 0;
  for(list<partition_t*>::iterator it = parts.begin();
  it != parts.end(); ++it ) {
  tuple_t * tuple = (*it)->tuple;
  size_t tuples = (*it)->tuples;
  for (uint64_t i = 0; i != tuples ; ++i) {
  sum += tuple[i].key;
  }
  }
  long long sum1 = 0;
  tuple_t * tuple = outp->tuple;
  for (uint32_t i = 0 ; i != partitions ; ++i) {
  assert(part_end[i] == part_start[i] + hist[i]);
  for (uint32_t j = part_start[i]; j != part_end[i]; ++j) {
  assert(mhash(tuple[j].key, mask, shift) == i);
  sum1 += tuple[j].key;
  }
  }
  assert(sum == sum1);
  */
  return htp;
}


void BuildTask::Run(thread_t *my)
{
  perf_counter_t before = perf_read(my->perf);

  partition_t *htp = Build(my);
  Finish(my, htp);

  perf_counter_t after = perf_read(my->perf);
  perf_counter_t state = perf_counter_diff(before, after);
  perf_counter_aggr(&my->stage_counter[1], state);
}


void BuildTask::Finish(thread_t* my, partition_t *htp)
{
  htp->ready = true;

  // hash table partition
  out_->AddPartition(htp);

  // Commit
  in_->Commit(in_->GetPartitionsByKey(radix_).size());

  // We create a new probe task on this node
  Taskqueue *queue = my->node->queue;
  ProbeTask **probetask = my->env->GetProbeTaskByTable(probe_->id());
  queue->AddTask(probe_->id(), probetask[radix_]);

  // check if I am the last one to finish?
  if (!in_->done())
    return;

  // set the output hashtable table to ready
  out_->set_ready();

  if (out_->type() == OpNone) 
    my->env->commit();

  if (my->env->queries() == 0)
    my->env->set_done();
}

#ifdef BUCKET_CHAINING
// NOT IMPLEMENTED
hashtable_t* BuildTask::Build(thread_t *my)
{
}
#endif

void UnitProbeTask::DoJoin(hashtable_t *hashtable, thread_t *my)
{
  size_t partitions = hashtable->partitions;
  int shift = Params::kNumRadixBits;
  int32_t mask  = (partitions-1) << shift;

  // check buffer compatibility
  if (!buffer_compatible(my->buffer, out_, input_->radix)) {
    pthread_mutex_lock(&my->lock);
    // flush old buffers
    if (my->buffer) {
      partition_t **buffer = my->buffer->partition;
      for (int i = 0; i != my->buffer->partitions; ++i)
        if (buffer[i]->tuples != 0)
          FlushBuffer(my->buffer->table, buffer[i], my->env);
      buffer_destroy(my->buffer);
    }
    // we only need one buffer
    my->buffer = buffer_init(out_, input_->radix, 1);
    pthread_mutex_unlock(&my->lock);
  }

  if (my->buffer->partition[0] == NULL) {
    my->buffer->partition[0] = my->memm->GetPartition();
    my->buffer->partition[0]->radix = input_->radix;
  }    

  partition_t *outbuf = my->buffer->partition[0];

  tuple_t * stuple = input_->tuple;
  tuple_t * rtuple = hashtable->tuple;
  uint32_t * sum = hashtable->sum;

  //  long matches = 0;
  // start from what is remaining
  tuple_t * output = outbuf->tuple + outbuf->tuples;
  tuple_t * outend = outbuf->tuple + Params::kMaxTuples;
  for (uint64_t i = 0; i != input_->tuples; ++i) {
    uint32_t idx = mhash(stuple[i].key, mask, shift);
    uint64_t j = sum[idx], end = sum[idx+1];
    for (; j != end; ++j) {
      if (stuple[i].key == rtuple[j].key) {
        output->key = (intkey_t)stuple[i].payload;
        output->payload = (intkey_t)rtuple[i].payload;
        output++;
        if (output == outend) {
          outbuf->tuples = Params::kMaxTuples;
          FlushBuffer(my->buffer->table, outbuf, my->env);
          
          outbuf = my->memm->GetPartition();
          outbuf->radix = input_->radix;
          output = outbuf->tuple;
          outend = outbuf->tuple + Params::kMaxTuples;
          my->buffer->partition[0] = outbuf;
        }
        //        matches++;
      }
    }
  }

  // set buffer size
  outbuf->tuples = output - outbuf->tuple;
 //  outbuf->tuples = matches;
}


// First write a version without output
void UnitProbeTask::Run(thread_t *my)
{
  perf_counter_t before = perf_read(my->perf);

  int radix = input_->radix;
  Table *buildtable = my->env->build_table();
  list<partition_t*> &htps = buildtable->GetPartitionsByKey(radix);
  hashtable_t *hashtable = htps.front()->hashtable;

  DoJoin(hashtable, my);

  Finish(my);

  perf_counter_t after = perf_read(my->perf);
  perf_counter_t state = perf_counter_diff(before, after);
  perf_counter_aggr(&my->stage_counter[2], state);

}

void UnitProbeTask::Finish(thread_t* my)
{
  in_->Commit();

  // check if I am the last one to finish?
  if (!in_->done())
    return;

  // Flush buffers
  // force flush only if we are the last operation
  thread_t * thread = my->env->threads();
  for (int tid = 0; tid != my->env->nthreads(); ++tid) {
    pthread_mutex_lock(&thread[tid].lock);
    buffer_t *buffer = thread[tid].buffer; // NEED a latch to do this work
    // force flush all remaining buffers
    if (buffer && buffer->table == out_) {
      for (int i = 0; i != buffer->partitions; ++i)
        if (buffer->partition[i]->tuples != 0)
          FlushBuffer(buffer->table, buffer->partition[i], my->env);
      buffer_destroy(buffer);
      thread[tid].buffer = NULL;
    }
    pthread_mutex_unlock(&thread[tid].lock);
  }

  // set output table to ready
  out_->set_ready();

  // check if there is no more work
  if (out_->type() == OpNone)
    my->env->commit();

  if (my->env->queries() == 0)
    my->env->set_done();

}
