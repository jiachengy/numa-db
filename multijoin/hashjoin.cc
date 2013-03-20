#include <glog/logging.h>

#include "hashjoin.h"

#define HASH_BIT_MODULO(K, MASK, NBITS) (((K) & MASK) >> NBITS)

void PartitionTask::ProcessBlock(thread_t *args, block_t block) {
	uint32_t mask = ((1 << nbits_) - 1) << offset_;
	uint32_t fanout = 1 << nbits_;

	uint32_t hist[fanout];
	memset(hist, 0, fanout * sizeof(uint32_t));

	// histogram
	tuple_t *tuple = part_[block.offset].tuples();
	for (uint32_t i = 0; i < block.size; i++, tuple++) {
		uint32_t idx = HASH_BIT_MODULO(tuple->key, mask, offset_);
		hist[idx]++;
	}


	tuple_t *dst[fanout];
	
	// check current output buffer
	for (uint32_t index = 0; index < fanout; index++) {
		int buffer_id = args->tid * fanout + index;
		Partition *p = out_->GetBuffer(buffer_id);

		// if buffer does not exists
		// or if not enough space
		if (!p || Partition::kPartitionSize - p->size() < hist[index]) {
			// if the buffer is full
			if (p) {
				out_->AddPartition(p);

				//				Task *ntask = NULL;
				// if (out_->type() == OpBuild) {
				// 	ntask = new BuildTask();
				// }
				// else if (out_->type() == OpProbe) {
				// 	ntask = new ProbeTask();
				// }
				// else {
				// 	LOG(INFO) << "Unexpected op type";
				// }
				// args->queue->Add(out_->id_, ntask);
			}

			// create a new buffer
			// switch the output buffer to the new one
			Partition *np = new Partition(args->node_id, index);
			out_->SetBuffer(buffer_id, np);
			p = np;
		}
		dst[index] = &p->tuples()[p->size()];
		p->set_size(p->size() + hist[index]);
	}

	// second scan, partition and scatter
	tuple = &(part_->tuples()[block.offset]);
	for (uint32_t i = 0; i < block.size; i++, tuple++) {
		uint32_t idx = HASH_BIT_MODULO(tuple->key, mask, offset_);
		*(dst[idx]++) = *(tuple++);
	}
}

void PartitionTask::Unblock(thread_t* args)
{
	node_t *nodes = args->env->nodes();
	for (int node = 0; node < args->env->nnodes(); node++) {
		nodes[node].queue->Unblock(out_->id());
	}
}


void PartitionTask::Run(thread_t *args)
{
	// process the partition in blocks
	uint32_t offset = 0;
	while (offset < part_->size()) {
		size_t sz = Partition::kBlockSize;
		if (part_->size() - offset < Partition::kBlockSize)
			sz = part_->size() - offset;
		block_t block;
		block.offset = offset;
		block.size = sz;
		ProcessBlock(args, block);
			
		offset += sz;
	}

	part_->set_done();

	in_->Commit();

	// check if I am the last one to finish?
	// if yes, promote all other threads
	// if not, do nothing
	if (in_->done()) {
		Unblock(args);
	}
}
