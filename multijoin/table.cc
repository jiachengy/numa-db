#include "table.h"

Table::Table(uint32_t nnodes, uint32_t nkeys) {
	id_ = kInvalidId;
	type_ = OpNone;

	nkeys_ = nkeys;
	if (nkeys_)
		pkeys_.resize(nkeys);

	nnodes_ = nnodes;
	pnodes_.resize(nnodes);
		
	nbuffers_ = 0;
	done_count_ = 0;
	done_ = false;
	ready_ = false;
	buffers_ = NULL;
}


Table::Table(int id, OpType type, uint32_t nnodes, uint32_t nkeys, size_t nbuffers) {
	id_ = id;
	type_ = type;

	nkeys_ = nkeys;
	if (nkeys_)
		pkeys_.resize(nkeys);

	nnodes_ = nnodes;
	pnodes_.resize(nnodes);
		
	nbuffers_ = nbuffers;
	done_count_ = 0;
	done_ = false;
	ready_ = false;
	buffers_ = (Partition**)malloc(sizeof(Partition*) * nbuffers);
	memset(buffers_, 0, sizeof(Partition*) * nbuffers);
}


Table::~Table() {
	if (buffers_)
		free(buffers_);
	for (uint32_t node = 0; node < nnodes_; node++) {
		list<Partition*> pnode = pnodes_[node];
		for (list<Partition*>::iterator it = pnode.begin();
			 it != pnode.end(); it++) {
			delete *it;
		}
	}
}

