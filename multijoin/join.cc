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

#ifdef BUCKET_CHAINING
// NOT IMPLEMENTED
partition_t* BuildTask::Build(thread_t *my)
{
  size_t tuples = 0;
  for (int node = 0; node != my->env->nnodes(); ++node) {
    vector<partition_t*> &parts = in_->GetPartitionsByKey(node, radix_);
    for(vector<partition_t*>::iterator it = parts.begin();
        it != parts.end(); ++it )
      tuples += (*it)->tuples;
  }

  size_t buckets = NEXT_POW_2(tuples / Params::kLoadFactor);
  int shift = Params::kNumRadixBits;
  int32_t mask  = (buckets - 1) << shift;

  partition_t *htp = my->memm[2]->GetHashtable(buckets);
  htp->radix = radix_;
  htp->buckets = buckets;
  htp->tuples = tuples;

  int * bucket = htp->hashtable->bucket;
  entry_t * entry = htp->hashtable->entry;

  uint64_t idx = 0;

  for (int node = 0; node != my->env->nnodes(); ++node) {
    vector<partition_t*> &parts = in_->GetPartitionsByKey(node, radix_);
    for(vector<partition_t*>::iterator it = parts.begin();
        it != parts.end(); ++it ) {
      intkey_t * key = (*it)->key;
      value_t * value = (*it)->value;
      for (uint64_t i = 0; i < (*it)->tuples; ++i) {
        uint32_t hash = mhash(key[i], mask, shift);
        entry[idx].next = bucket[hash];
        entry[idx].key = key[i];
        entry[idx].value = value[i];
        bucket[hash] = ++idx;
      }
    }
  }
  return htp;
}
#else
partition_t * BuildTask::Build(thread_t * my)
{
  size_t total_tuples = 0;
  for (int node = 0; node != my->env->nnodes(); ++node) {
    vector<partition_t*> &parts = in_->GetPartitionsByKey(node, radix_);
    for(vector<partition_t*>::iterator it = parts.begin();
        it != parts.end(); ++it )
      total_tuples += (*it)->tuples;
  }

  size_t partitions = get_hist_size(total_tuples);
  int shift = Params::kNumRadixBits;
  int32_t mask  = (partitions-1) << shift;

  // preallocate the folllowing in thread
  my->hist   = (uint32_t*)realloc(my->hist, partitions * sizeof(uint32_t));
  memset(my->hist, 0x0, partitions * sizeof(uint32_t));
#ifdef COLUMN_WISE
  my->part_key = (intkey_t**)realloc(my->part_key, partitions * sizeof(intkey_t*));   // temp output buffer
  my->part_value = (value_t**)realloc(my->part_value, partitions * sizeof(value_t*));   // temp output buffer
#else
  my->part = (tuple_t**)realloc(my->part, partitions * sizeof(tuple_t*));   // temp output buffer
#endif
  
  uint32_t *hist = my->hist;
#ifdef COLUMN_WISE
  intkey_t **part_key = my->part_key;
  value_t **part_value = my->part_value;
#else
  tuple_t **part = my->part;
#endif
  // dynamic request from memm
  partition_t *htp = my->memm[1]->GetHashtable(total_tuples, partitions + 1);
  assert(htp->hashtable != NULL);
  htp->radix = radix_;
  uint32_t *sum = htp->hashtable->sum;

  for (int node = 0; node != my->env->nnodes(); ++node) {
    vector<partition_t*> &parts = in_->GetPartitionsByKey(node, radix_);
    for(vector<partition_t*>::iterator it = parts.begin();
        it != parts.end(); ++it ) {
#ifdef COLUMN_WISE
      intkey_t * key = (*it)->key;
#else
      tuple_t * tuple = (*it)->tuple;
#endif
      size_t tuples = (*it)->tuples;
      for (uint64_t i = 0; i != tuples ; ++i) {
#ifdef COLUMN_WISE
        uint32_t hash = mhash(key[i], mask, shift);
#else
        uint32_t hash = mhash(tuple[i].key, mask, shift);
#endif
        hist[hash]++;
      }
    }
  }

  /* prefix sum on histogram */
  for( uint32_t i = 0; i != partitions; i++ ) {
    sum[i] = i ? sum[i-1] + hist[i-1] : 0;
#ifdef COLUMN_WISE
    part_key[i] = htp->hashtable->key + sum[i];
    part_value[i] = htp->hashtable->value + sum[i];
#else
    part[i] = htp->hashtable->tuple + sum[i];
#endif
  }
  sum[partitions] = total_tuples;

  for (int node = 0; node != my->env->nnodes(); ++node) {
    vector<partition_t*> &parts = in_->GetPartitionsByKey(node, radix_);
    for(vector<partition_t*>::iterator it = parts.begin();
        it != parts.end(); ++it ) {
      partition_t * p = *it;

#ifdef COLUMN_WISE
      intkey_t * key = p->key;
      value_t * value = p->value;
#else
      tuple_t * tuple = p->tuple;
#endif
      for (uint64_t i = 0; i != p->tuples; ++i) {
#ifdef COLUMN_WISE
        uint32_t hash = mhash(key[i], mask, shift);
        *(part_key[hash]++) = key[i];
        *(part_value[hash]++) = value[i];
#else
        uint32_t hash = mhash(tuple[i].key, mask, shift);
        *(part[hash]++) = tuple[i];
#endif
      }

      if (p->memm)
        p->memm->Recycle(p);
    }
  }  

  return htp;
}
#endif

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

  // We create a new probe task on this node
  Taskqueue *queue = my->node->queue;
  ProbeTask **probetask = my->env->GetProbeTaskByTable(probe_->id());
  queue->AddTask(probe_->id(), probetask[radix_]);
  probetask[radix_]->set_schedule(true);

  node_t *nodes = my->env->nodes();

  int parts = 0;
  for (int node = 0; node < my->env->nnodes(); ++node) {
    nodes[node].queue->UnblockNext(in_->id());
    parts += in_->GetPartitionsByKey(node, radix_).size();
  }

  // commit and check if I am the last one to finish?

  if (!in_->Commit(parts))
    return;

  // set the output hashtable table to ready
  out_->set_ready();

  if (out_->type() == OpNone) 
    my->env->commit();

  if (my->env->queries() == 0)
    my->env->set_done();
}

#ifdef BUCKET_CHAINING
void UnitProbeTask::DoJoin(hashtable_t *hashtable, thread_t *my)
{
  size_t buckets = hashtable->buckets;
  int shift = Params::kNumRadixBits;
  int32_t mask  = (buckets-1) << shift;

  Memory * memm = my->memm[1]; // use the smaller partition size

  pthread_mutex_lock(&my->lock);
  // check buffer compatibility
  if (!buffer_compatible(my->buffer, out_, input_->radix)) {
    // flush old buffers
    if (my->buffer) {
      FlushEntireBuffer(my->buffer, my->node_id, my->env);
      buffer_destroy(my->buffer);
    }
    // we only need one buffer
    my->buffer = buffer_init(out_, input_->radix, shift, 1);
  }
  pthread_mutex_unlock(&my->lock);

  if (my->buffer->partition[0] == NULL) {
    my->buffer->partition[0] = memm->GetPartition();
    my->buffer->partition[0]->radix = input_->radix;
  }    

  partition_t *outbuf = my->buffer->partition[0];

  intkey_t * skey = input_->key;
  value_t * svalue = input_->value;

  intkey_t * key_out = outbuf->key + outbuf->tuples;
  intkey_t * key_end = outbuf->key + outbuf->capacity;
  value_t * value_out = outbuf->value + outbuf->tuples;

  size_t matches = 0;
  for (uint64_t i = 0; i != input_->tuples; ++i) {
	uint32_t idx = mhash(skey[i], mask, shift);
    entry_t e;
    for (int hit = bucket[idx]; hit > 0; hit = e.next) {
      e = entry[hit-1];

      if (skey[i] == e.key) {
        matches++;
        *(key_out++) = svalue[i];
        *(value_out++) = e.value;

        if (key_out == key_end) {
          outbuf->tuples = outbuf->capacity;
          FlushBuffer(my->buffer, outbuf, my->env);
          
          outbuf = memm->GetPartition();
          outbuf->radix = input_->radix;
		  
          key_out = outbuf->key;
          key_end = outbuf->key + outbuf->capacity;;
          value_out = outbuf->value;
          my->buffer->partition[0] = outbuf;
        }

      }
    }
  }  
}
#else
void UnitProbeTask::DoJoin(hashtable_t *hashtable, thread_t *my)
{
  size_t partitions = hashtable->partitions;
  int shift = Params::kNumRadixBits;
  int32_t mask  = (partitions-1) << shift;

  Memory * memm = my->memm[1]; // use the smaller partition size

  pthread_mutex_lock(&my->lock);
  // check buffer compatibility
  if (!buffer_compatible(my->buffer, out_, input_->radix)) {
    // flush old buffers
    if (my->buffer) {
      FlushEntireBuffer(my->buffer, my->node_id, my->env);
      buffer_destroy(my->buffer);
    }
    // we only need one buffer
    my->buffer = buffer_init(out_, input_->radix, shift, 1);
  }
  pthread_mutex_unlock(&my->lock);

  if (my->buffer->partition[0] == NULL) {
    my->buffer->partition[0] = memm->GetPartition();
    my->buffer->partition[0]->radix = input_->radix;
  }    

  partition_t *outbuf = my->buffer->partition[0];

#ifdef COLUMN_WISE
  intkey_t * skey = input_->key;
  intkey_t * rkey = hashtable->key;
  value_t * svalue = input_->value;
  value_t * rvalue = hashtable->value;
#else
  tuple_t * stuple = input_->tuple;
  tuple_t * rtuple = hashtable->tuple;
#endif
  uint32_t * sum = hashtable->sum;

  //  long matches = 0;
  // start from what is remaining
#ifdef COLUMN_WISE
  intkey_t * key_out = outbuf->key + outbuf->tuples;
  intkey_t * key_end = outbuf->key + outbuf->capacity;
  value_t * value_out = outbuf->value + outbuf->tuples;
  //  value_t * value_end = outbuf->value + outbuf->capacity;
#else
  tuple_t * output = outbuf->tuple + outbuf->tuples;
  tuple_t * outend = outbuf->tuple + outbuf->capacity;
#endif
  for (uint64_t i = 0; i != input_->tuples; ++i) {
#ifdef COLUMN_WISE
	uint32_t idx = mhash(skey[i], mask, shift);
    uint64_t j = sum[idx], end = sum[idx+1];
    for (; j != end; ++j) {

	  if (skey[i] == rkey[j]) {
        *(key_out++) = svalue[i];
        *(value_out++) = rvalue[i];
        if (key_out == key_end) {
          outbuf->tuples = outbuf->capacity;
          FlushBuffer(my->buffer, outbuf, my->env);
          
          outbuf = memm->GetPartition();
          outbuf->radix = input_->radix;
		  
          key_out = outbuf->key;
          key_end = outbuf->key + outbuf->capacity;;
          value_out = outbuf->value;
		  //          value_end = outbuf->value + outbuf->capacity;;
          my->buffer->partition[0] = outbuf;
        }
      }
    }
  }
  // set buffer size
  outbuf->tuples = key_out - outbuf->key;
#else
    uint32_t idx = mhash(stuple[i].key, mask, shift);
    uint64_t j = sum[idx], end = sum[idx+1];
    for (; j != end; ++j) {
      if (stuple[i].key == rtuple[j].key) {
        output->key = (intkey_t)stuple[i].payload;
        output->payload = (intkey_t)rtuple[i].payload;
        output++;
        if (output == outend) {
          outbuf->tuples = outbuf->capacity;
          FlushBuffer(my->buffer->table, outbuf, my->env);
          
          outbuf = memm->GetPartition();
          outbuf->radix = input_->radix;
          output = outbuf->tuple;
          outend = outbuf->tuple + outbuf->capacity;
          my->buffer->partition[0] = outbuf;
        }
        //        matches++;
      }
    }
  }
  // set buffer size
  outbuf->tuples = output - outbuf->tuple;
 //  outbuf->tuples = matches;
#endif
}
#endif

// First write a version without output
void UnitProbeTask::Run(thread_t *my)
{
  perf_counter_t before = perf_read(my->perf);

  int radix = input_->radix;
  Table *buildtable = my->env->build_table();

  BlockKeyIterator it = buildtable->GetBlocksByKey(radix);
  assert(it.hasNext());
  hashtable_t *hashtable = it.getNext()->hashtable;
  
  DoJoin(hashtable, my);

  Finish(my);

  perf_counter_t after = perf_read(my->perf);
  perf_counter_t state = perf_counter_diff(before, after);
  perf_counter_aggr(&my->stage_counter[2], state);

}

void UnitProbeTask::Finish(thread_t* my)
{
  if (input_->memm)
    input_->memm->Recycle(input_);
  
  // check if I am the last one to finish?
  if (!in_->Commit())
    return;

  // Flush buffers
  // force flush only if we are the last operation
  thread_t * thread = my->env->threads();
  for (int tid = 0; tid != my->env->nthreads(); ++tid) {
    pthread_mutex_lock(&thread[tid].lock);
    buffer_t *buffer = thread[tid].buffer; // NEED a latch to do this work
    // force flush all remaining buffers
    if (buffer && buffer->table == out_) {
      FlushEntireBuffer(buffer, thread[tid].node_id, my->env);
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
