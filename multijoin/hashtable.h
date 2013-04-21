#ifndef HASHTABLE_H_
#define HASHTABLE_H_

typedef struct hashtable_t hashtable_t;

#include "types.h"
#include "table.h"

#ifdef BUCKET_CHAINING
struct entry_t {
  tuple_t tuple;
  int next;
};
struct hashtable_t {
  entry_t *next;
  int *bucket;
  uint32_t nbuckets;
  uint32_t ntuples;
};
#else
struct hashtable_t {
#ifdef COLUMN_WISE
  intkey_t * key;
  value_t * value;
#else
  tuple_t * tuple;
#endif
  size_t tuples;
  uint32_t * sum;
  size_t partitions;
};
#endif

#ifdef BUCKET_CHAINING
void hashtable_free(hashtable_t *ht);
hashtable_t *hashtable_init(int ntuples);
hashtable_t *hashtable_init_noalloc(int ntuples);
void hashtable_reset(hashtable_t *ht);
#endif

#endif // HASHTABLE_H_
