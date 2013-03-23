#include "hashtable.h"
#include "util.h"

void hashtable_free(hashtable_t *ht)
{
  dealloc(ht->next, sizeof(entry_t) * ht->ntuples);
  dealloc(ht->bucket, sizeof(int) * ht->nbuckets);
  free(ht);
}

hashtable_t *hashtable_init(int ntuples, int nbuckets)
{
  hashtable_t *ht = (hashtable_t*)malloc(sizeof(hashtable_t));
  ht->next   = (entry_t*)alloc(sizeof(entry_t) * ntuples);
  ht->bucket = (int*)alloc(nbuckets * sizeof(int));
  ht->nbuckets = nbuckets;
  ht->ntuples = ntuples;
  return ht;
}

