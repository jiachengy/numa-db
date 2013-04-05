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

hashtable_t * BuildTask::Build(thread_t * my)
{
  tuple_t * output = my->memm->baseptr();

  list<partition_t*> &parts = in_->GetPartitionsByKey(radix_);

  size_t total_tuples = 0;
  for(list<partition_t*>::iterator it = parts.begin();
      it != parts.end(); ++it )
    total_tuples += (*it)->tuples;

  size_t partitions = get_hist_size(total_tuples);
  int shift = Params::kNumRadixBits;
  int32_t mask  = (partitions-1) << shift;

  logging("tuples: %ld, partitions: %ld\n", total_tuples, partitions);
  
  uint64_t *hist   = (uint64_t*) calloc(partitions, sizeof(uint64_t));
  for(list<partition_t*>::iterator it = parts.begin();
      it != parts.end(); ++it ) {
    tuple_t * tuple = (*it)->tuple;
    size_t tuples = (*it)->tuples;
    for (uint64_t i = 0; i != tuples ; ++i) {
      uint32_t hash = mhash(tuple[i].key, mask, shift);
      hist[hash]++;
    }
  }
  
  // temp output buffer holder
  tuple_t **part = (tuple_t**)malloc(partitions * sizeof(tuple_t*));
  // index start, index end
  uint32_t *part_start = (uint32_t*)malloc(partitions * sizeof(uint32_t));
  uint32_t *part_end = (uint32_t*)malloc(partitions * sizeof(uint32_t));

  // TODO: consider overflow hash tables
  assert(total_tuples <= Params::kMaxTuples);
  partition_t * outp = my->memm->GetPartition();
  outp->tuples = total_tuples;

  /* prefix sum on histogram */
  for( uint32_t i = 0; i != partitions; i++ ) {
    part[i] = i ? part[i-1] + hist[i-1] : outp->tuple;
    part_start[i] = i ? part_start[i-1] + hist[i-1] : 0;
    part_end[i] = part_start[i] + hist[i];
  }

  cache_line_t *wc_buf = (cache_line_t*)alloc(partitions * sizeof(cache_line_t));
  uint32_t *wc_count = (uint32_t*)calloc(partitions, sizeof(uint32_t));

  for(list<partition_t*>::iterator it = parts.begin();
      it != parts.end(); ++it ) {
    tuple_t * tuple_ptr = (*it)->tuple;
    tuple_t * tuple_end = &(*it)->tuple[(*it)->tuples];

    if (it == parts.begin()) {
      // alignment only at the first loop
      uint64_t i = partitions; 
      do {
		tuple_t row = *tuple_ptr++;
		uint32_t hash = mhash(row.key, mask, shift);
		*part[hash]++ = row;
		if (++wc_count[hash] == 7 && !--i) break;
      } while (tuple_ptr != tuple_end);

      for (uint64_t i = 0 ; i != partitions ; ++i) {
        uint64_t o, off = part[i] - output;
        wc_buf[i].data[7] = off;
        off &= 7;
        for (o = 0 ; o != off ; ++o)
          wc_buf[i].data[o] = ((uint64_t*)part[i])[o - off];
      }
    }

	// main loop
	if (tuple_ptr != tuple_end)
      do {
		// read and hash row
		tuple_t row = *tuple_ptr;
		uint32_t hash = mhash(row.key, mask, shift);
		// offset in wc buffer (loads cache line)
		uint64_t index = wc_buf[hash].data[7]++;
		uint64_t mod_index = index & 7;
		// write in wc buffer
		wc_buf[hash].data[mod_index] = *(uint64_t*)tuple_ptr;
		// cache line is full
		if (mod_index == 7) {
          // use 128-bit registers by default
          __m128i *src = (__m128i*) wc_buf[hash].data;
          __m128i *dest = (__m128i*) &output[index - 7];
          // load cache line from cache to 4 registers
          __m128i r0 = _mm_load_si128(&src[0]);
          __m128i r1 = _mm_load_si128(&src[1]);
          __m128i r2 = _mm_load_si128(&src[2]);
          __m128i r3 = _mm_load_si128(&src[3]);
          // store cache line from registers to memory
          _mm_stream_si128(&dest[0], r0);
          _mm_stream_si128(&dest[1], r1);
          _mm_stream_si128(&dest[2], r2);
          _mm_stream_si128(&dest[3], r3);
          // restore overwritten pointer
          wc_buf[hash].data[7] = index + 1;
		}
      } while (++tuple_ptr != tuple_end);
  }
  
  // send remaining items from buffers to output
  // only when we finish processing all
  for (uint64_t i = 0 ; i != partitions ; ++i) {
    uint64_t j, p = wc_buf[i].data[7];
    part[i] = &output[p];
    p &= 7;
    for (j = 0 ; j != p ; ++j)
      ((uint64_t*)part[i])[j - p] = wc_buf[i].data[j];
  }

  hashtable_t * ht = (hashtable_t*)malloc(sizeof(hashtable_t));
  ht->data = outp;
  ht->tuples = total_tuples;
  ht->start = part_start;
  ht->end = part_end;
  ht->partitions = partitions;


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

  free(wc_count);
  free(wc_buf);
  free(part);
  free(hist);
  
  return ht;
}

#ifdef BUCKET_CHAINING
// NOT IMPLEMENTED
hashtable_t* BuildTask::Build(thread_t *my)
{
  /* count number of tuples */
  size_t tuples = 0;
  list<partition_t*> &parts = in_->GetPartitionsByKey(key_);
  for (list<partition_t*>::iterator it = parts.begin(); it != parts.end(); it++)
    tuples += (*it)->tuples;

  partition_t *htp = my->memm->GetPartition();
  hashtable_t *ht = htp->hashtable;
  htp->radix = key_;

  int *bucket = ht->bucket;
  entry_t *next = ht->next;
  const uint32_t MASK = (ht->nbuckets-1) << (Params::kNumRadixBits);

  // Begin to build the hash table
  uint32_t i = 0;
  for (list<partition_t*>::iterator it = parts.begin();
       it != parts.end(); ++it) {

    tuple_t *tuple = (*it)->tuple;

    for (uint32_t j = 0; j < (*it)->tuples; j++) {
      uint32_t idx = mhash(tuple->key, MASK, Params::kNumRadixBits);
      next[i].next = bucket[idx];
      next[i].tuple = *tuple;

      bucket[idx] = ++i; // pos starts from 1 instead of 0
      tuple++;
    }
  }
}
#endif

void BuildTask::Run(thread_t *my)
{
  perf_counter_t before = perf_read(my->perf);

  hashtable_t *ht = Build(my);
  Finish(my, ht);

  perf_counter_t after = perf_read(my->perf);
  perf_counter_t state = perf_counter_diff(before, after);

  perf_counter_aggr(&my->stage_counter[1], state);
}


void BuildTask::Finish(thread_t* my, hashtable_t *ht)
{
  partition_t * htp = partition_init(my->node_id);
  htp->hashtable = ht;
  htp->radix = radix_;
  htp->ready = true;

  // hash table partition
  out_->AddPartition(htp);

  // Commit
  in_->Commit(in_->GetPartitionsByKey(radix_).size());


  // We create a new probe task on this node
  // Taskqueue *queue = my->node->queue;
  // Task *task = new ProbeTask(OpProbe, radix_);
  // queue->AddTask(probe_->id(), task);

  // check if I am the last one to finish?
  if (!in_->done())
    return;

  // set the output hashtable table to ready
  out_->set_ready();

  if (out_->type() == OpNone) {
    my->env->set_done();
    return;
  }
}





/*
  void UnitProbeTask::ProbeBlock(thread_t *my, block_t block, hashtable_t *ht)
  {
  const uint32_t MASK = (ht->nbuckets-1) << (Params::kNumRadixBits);
  int *bucket = ht->bucket;
  entry_t *next = ht->next;  

  // output buffer
  int buffer_id = my->tid * Params::kFanoutPass1 + part_->key();
  Partition *outp = out_->GetBuffer(buffer_id);

  tuple_t *tuple = block.tuples;
  for(uint32_t i = 0; i < block.size; i++, tuple++){
  uint32_t idx = HASH_BIT_MODULO(tuple->key, MASK, Params::kNumRadixBits);
  for(int hit = bucket[idx]; hit > 0; hit = next[hit-1].next){

  if(tuple->key == next[hit-1].tuple.key){
  // V1: check output for every match

  // there is no space
  if (!outp || Params::kPartitionSize == outp->size()) {
  // if the buffer is full
  if (outp)
  out_->AddPartition(outp);

  // create a new buffer
  // switch the output buffer to the new one
  #ifdef PRE_ALLOC
  Partition *np = my->recycler->GetEmptyPartition();
  np->set_key(part_->key());
  #else
  Partition *np = new Partition(my->node_id, part_->key());
  np->Alloc();
  #endif

  out_->SetBuffer(buffer_id, np);
  outp = np;
  }
  tuple_t jr;
  jr.key = tuple->payload;
  jr.payload = next[hit-1].tuple.payload;
  outp->Append(jr);

  // tuple_t *t = &outp->tuples()[outp->size()];
  //   t->key = tuple->payload;
  //   t->payload = next[hit-1].tuple.payload;
  //   outp->set_size(outp->size() + 1);
  }      
  }
  }
  }
*/

// TODO: unrolling the loop
void UnitProbeTask::Run(thread_t *my)
{
  // int key = part_->key();

  // hashtable_t *ht = build_->GetPartitionsByKey(key).front()->hashtable();


  // // process in blocks
  // while (!part_->done()) {
  //   block_t block = part_->NextBlock();
  //   ProbeBlock(my, block, ht);    
  // }

  // Finish(my);
}

void UnitProbeTask::Finish(thread_t* my)
{
  in_->Commit();

  // check if I am the last one to finish?
  if (!in_->done())
    return;

  // Finish all remaining buffered partitions
  // for (int node = 0; node < my->env->nnodes(); node++) {
  //   for (int i = 0; i < out_->nbuffers(); i++) {
  //     partition_t *outp = out_->GetBuffer(node, i);
  //     if (outp)
  //       out_->AddPartition(outp);
  //   }
  // }

  // set output table to ready
  out_->set_ready();

  // check if there is no more work
  if (out_->type() == OpNone) {
    my->env->set_done();
    return;
  }

  // Unblock next operator
  // Only used in multi joins
  node_t *nodes = my->env->nodes();
  for (int node = 0; node < my->env->nnodes(); node++)
    nodes[node].queue->Unblock(out_->id());
}


void ProbeTask::Run(thread_t *my)
{
  //  Tasklist *probes = my->env->probes()[key_];
  my->batch_task = this;
}
