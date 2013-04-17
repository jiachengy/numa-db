#ifndef PARAMS_H_
#define PARAMS_H_

#include <unistd.h>

#include "types.h"

#define CACHE_LINE_SIZE 64

#define STAGES 4

struct config_t
{
  size_t mem_per_thread;
};

extern config_t gConfig;

class Params
{
 public:
  //  static size_t kMaxHtTuples;
  static const size_t kPartitionSize = 1024 * 1024; // 128K tuples = 1M
  static const size_t kMaxTuples = kPartitionSize / sizeof(tuple_t);

  static const int kTuplesPerCacheLine = CACHE_LINE_SIZE / sizeof(tuple_t);

  //  static const int kHtInflateRate = 1; // can only be 2^k

  static const int kNumRadixBits = 12;
  static const int kNumPasses = 2; 

  static const int kNumBitsPass1 = kNumRadixBits / kNumPasses;
  static const int kNumBitsPass2 = kNumRadixBits - kNumBitsPass1;

  static const int kOffsetPass1 = 0;
  static const int kOffsetPass2 = kNumBitsPass1;

  static const int kFanoutPass1 = 1 << kNumBitsPass1;
  static const int kFanoutPass2 = 1 << kNumBitsPass2;
  static const int kFanoutTotal = 1 << kNumRadixBits;
};

#endif // PARAMS_H_
