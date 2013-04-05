#include "params.h"
#include "hashtable.h"
#include "util.h"

#ifdef BUCKET_CHAINING
void hashtable_free(hashtable_t *ht)
{
  if (ht->next)
    dealloc(ht->next, sizeof(entry_t) * ht->ntuples);
  if (ht->bucket)
    dealloc(ht->bucket, sizeof(int) * ht->nbuckets);
  free(ht);
}

hashtable_t *hashtable_init(int ntuples)
{
  uint32_t nbuckets = ntuples;
  NEXT_POW_2(nbuckets);
  nbuckets = nbuckets;

  hashtable_t *ht = (hashtable_t*)malloc(sizeof(hashtable_t));
  ht->next   = (entry_t*)alloc(sizeof(entry_t) * ntuples);
  ht->bucket = (int*)alloc(sizeof(int) * nbuckets);
  
  // actually, we only need to reset ht->bucket
  memset(ht->next, 0, sizeof(entry_t) * ntuples);
  memset(ht->bucket, 0, sizeof(int) * nbuckets);

  ht->nbuckets = nbuckets;
  ht->ntuples = ntuples;
  return ht;
}

hashtable_t *hashtable_init_noalloc(int ntuples)
{
  int nbuckets = (int)(ntuples);
  hashtable_t *ht = (hashtable_t*)malloc(sizeof(hashtable_t));
  
  ht->nbuckets = nbuckets;
  ht->ntuples = ntuples;
  return ht;
}


void hashtable_reset(hashtable_t *ht)
{
  memset(ht->bucket, 0, sizeof(int) * ht->nbuckets);
}
#endif

