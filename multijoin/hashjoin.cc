#include <glog/logging.h>

#include "hashjoin.h"

#define HASH_BIT_MODULO(K, MASK, NBITS) (((K) & MASK) >> NBITS)

void PartitionTask::ProcessBlock(thread_t *args, block_t block) {
	uint32_t mask = ((1 << nbits_) - 1) << offset_;
	uint32_t fanout = 1 << nbits_;

	uint32_t hist[fanout];
	memset(hist, 0, fanout * sizeof(uint32_t));

	// histogram
	tuple_t *tuple = part_[block.offset].tuples_;
	for (uint32_t i = 0; i < block.size; i++, tuple++) {
		uint32_t idx = HASH_BIT_MODULO(tuple->key, mask, offset_);
		hist[idx]++;
	}


	Partition *dst[fanout];
	
	// check current output buffer
	for (uint32_t i = 0; i < fanout; i++) {
		int buffer_id = args->tid * fanout + i;
		Partition *p = out_->buffers_[buffer_id];

		// if buffer does not exists
		// or if not enough space
		if (!p || PARTITION_SIZE - p->size_ < hist[i]) {
			// if the buffer is full
			if (p) {
				out_->AddPartition(p);

				Task *ntask = NULL;
				if (out_->type() == OpBuild) {
					ntask = new BuildTask();
				}
				else if (out_->type() == OpProbe) {
					ntask = new ProbeTask();
				}
				else {
					LOG(INFO) << "Unexpected op type";
				}
				args->queue->Add(out_->id_, ntask);
			}

			// create a new buffer
			// switch the output buffer to the new one
			Partition *np = new Partition();
			out_->buffers_[buffer_id] = np;
			p = np;
		}
		dst[i] = p;
	}

	// second scan, partition and scatter
	tuple = &part_->tuples_[block.offset];
	for (uint32_t i = 0; i < block.size; i++, tuple++) {
		uint32_t idx = HASH_BIT_MODULO(tuple->key, mask, offset_);
		dst[idx]->tuples_[dst[idx]->size_] = *tuple;
		dst[idx]->size_++;
		
	}
}

void PartitionTask::Unblock(thread_t *threads, int nthreads) {
	for (int i = 0; i < nthreads; i++) {
		threads[i].queue->promote(out_->id_);
	}
}


void PartitionTask::Run(thread_t *args) {
	// process the partition in blocks
	uint32_t offset = 0;
	while (offset < part_->size_) {
		size_t sz = BLOCK_SIZE;
		if (part_->size_ - offset < BLOCK_SIZE)
			sz = part_->size_ - offset;
		block_t block;
		block.offset = offset;
		block.size = sz;
		ProcessBlock(args, block);
			
		offset += sz;
	}

	part_->done_ = true;

	// commit to the input table
	// ask it if we are the last?
	bool islast = in_->Commit();

	// check if I am the last one to finish?
	// if yes, promote all other threads
	// if not, do nothing
	if (islast) {
		Unblock(args, args->global->nthreads);
	}
}
