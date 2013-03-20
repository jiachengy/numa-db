#include <glog/logging.h>

#include "hashjoin.h"

#define HASH_BIT_MODULO(K, MASK, NBITS) (((K) & MASK) >> NBITS)


// add the partition
// schedule the next task
void FinishPartition(Partition *p, Table *table)
{
  p->set_ready(); // ready
  table->AddPartition(p);
}


void PartitionTask::ProcessBlock(thread_t *args, block_t block, uint32_t mask, uint32_t fanout, uint32_t hist[], tuple_t *dst[]) {

  // histogram
  memset(hist, 0, fanout * sizeof(uint32_t)); // clear histogram

  LOG(INFO) << "Start histogram...";

  tuple_t *tuple = block.tuples;
  for (uint32_t i = 0; i < block.size; i++) {
    uint32_t idx = HASH_BIT_MODULO((tuple++)->key, mask, offset_);
    hist[idx]++;
  }

  LOG(INFO) << "Checking output buffer...";
	
  // check output buffer
  for (uint32_t idx = 0; idx < fanout; idx++) {
    int buffer_id = args->tid * fanout + idx;
    Partition *outp = out_->GetBuffer(buffer_id);

    // if buffer does not exists
    // or if not enough space
    if (!outp || Partition::kPartitionSize - outp->size() < hist[idx]) {
      // if the buffer is full
      if (outp) {
        FinishPartition(outp, out_);
      }

      // create a new buffer
      // switch the output buffer to the new one
      Partition *np = new Partition(args->node_id, idx);
      out_->SetBuffer(buffer_id, np);
      outp = np;

      LOG(INFO) << "Create a new buffer.";
    }
    dst[idx] = &outp->tuples()[outp->size()];
    outp->set_size(outp->size() + hist[idx]); // set size at once
  }

  LOG(INFO) << "Start scattering...";
  // second scan, partition and scatter
  tuple = block.tuples;
  for (uint32_t i = 0; i < block.size; i++) {
    uint32_t idx = HASH_BIT_MODULO(tuple->key, mask, offset_);
    *(dst[idx]++) = *(tuple++);
  }
}

void PartitionTask::Finish(thread_t* args)
{
  in_->Commit();

  // check if I am the last one to finish?
  if (!in_->done())
    return;

  LOG(INFO) << "Put buffers.";

  // Finish all remaining buffered partitions
  for (int i = 0; i < out_->nbuffers(); i++) {
    Partition *outp = out_->GetBuffer(i);
    if (outp) {
      FinishPartition(outp, out_);
    }
  }

  LOG(INFO) << "Put buffers done.";

  // set output table to ready
  out_->set_ready();


  // Unblock next operator
  // node_t *nodes = args->env->nodes();
  // for (int node = 0; node < args->env->nnodes(); node++) {
  //   nodes[node].queue->Unblock(out_->id());
  // }


  // BECAUSE WE ARE TESTING PARTITIONING ONLY
  // AFTER WE ARE DONE
  args->env->set_done();
}


void PartitionTask::Run(thread_t *args)
{
  uint32_t mask = ((1 << nbits_) - 1) << offset_;
  uint32_t fanout = 1 << nbits_;
  uint32_t hist[fanout];
  tuple_t *dst[fanout];

  LOG(INFO) << "Thread " << args->tid << " starts a partition.";

  // process the partition in blocks  
  while (!part_->done()) {
    block_t block = part_->NextBlock();
    assert(block.size > 0 && block.size <= Partition::kBlockSize);
    ProcessBlock(args, block, mask, fanout, hist, dst);    
  }

  LOG(INFO) << "Finish all blocks.";
  Finish(args);
  LOG(INFO) << "Thread " << args->tid << " finishes partition.";  
}
