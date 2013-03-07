#include "glog/logging.h"
#include "hashtable.h"
#include "partition.h"

#define CARDINALITY 100

void LocalAggregation(Block block, Partition *out)
{
	HTPartition *htout = (HTPartition*)out;
	
	if (htout->hashtable == NULL) {
		htout->hashtable = new LocalAggrTable(CARDINALITY);
		LOG(INFO) << "Hash table built.";
	}

	LocalAggrTable *ht = (LocalAggrTable*)htout->hashtable;

    data_t *keys = block.data;
	rid_t *rids = block.rids;
	for (unsigned int i = 0; i < block.size; i++) {
		ht->Aggregate(keys[i], rids[i]);
	}
}


void GlobalAggregation(Block block, Partition *out)
{
	HTPartition *htout = (HTPartition*)out;
	
	if (htout->hashtable == NULL) {
		htout->hashtable = new GlobalAggrTable(CARDINALITY);
		LOG(INFO) << "Hash table built.";
	}

	GlobalAggrTable *ht = (GlobalAggrTable*)htout->hashtable;

    data_t *keys = block.data;
	rid_t *rids = block.rids;
	for (unsigned int i = 0; i < block.size; i++) {
		ht->Aggregate(keys[i], rids[i]);
	}
}
