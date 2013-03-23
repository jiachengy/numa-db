#ifndef HASHTABLE_H_
#define HASHTABLE_H_

#include "types.h"

struct entry_t {
  tuple_t tuple;
  int next;
};

struct hashtable_t {
  entry_t *next = NULL;
  int *bucket = NULL;
  uint32_t nbuckets = 0;
  uint32_t ntuples = 0;
};

void hashtable_free(hashtable_t *ht);
hashtable_t *hashtable_init(int ntuples, int nbuckets);


#endif // HASHTABLE_H_
