#ifndef HASHTABLE_H_
#define HASHTABLE_H_

#include "types.h"

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

void hashtable_free(hashtable_t *ht);
hashtable_t *hashtable_init(int ntuples, int nbuckets);


#endif // HASHTABLE_H_
