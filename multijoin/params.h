#ifndef PARAMS_H_
#define PARAMS_H_

#include <unistd.h>

#include "types.h"

#define CACHE_LINE_SIZE 64

#define STAGES 6

#define TWO_PASSES

struct config_t
{
  size_t large_mem_per_thread;
  size_t small_mem_per_thread;
  size_t hashtable_per_thread;
};

extern config_t gConfig;

class Params
{
 public:
  //  static size_t kMaxHtTuples;
  static const size_t kPartitionSize = 1024 * 1024; // 1M
  static const size_t kSmallPartitionSize = 1024 * 64; // 256K: L2 size



  static const size_t kMaxTuples = kPartitionSize / sizeof(tuple_t);
  static const size_t kSmallMaxTuples = kSmallPartitionSize / sizeof(tuple_t);
  static const int kTuplesPerCacheLine = CACHE_LINE_SIZE / sizeof(tuple_t);
  static const size_t kPaddingTuples = 0;


  //  static const int kHtInflateRate = 1; // can only be 2^k

  static const int kNumRadixBits = 16;
  static const int kNumPasses = 2; 

  static const int kNumBitsPass1 = kNumRadixBits / kNumPasses;
  static const int kNumBitsPass2 = kNumRadixBits - kNumBitsPass1;

  static const int kOffsetPass1 = 0;
  static const int kOffsetPass2 = kNumBitsPass1;

  static const int kFanoutPass1 = 1 << kNumBitsPass1;
  static const int kFanoutPass2 = 1 << kNumBitsPass2;
  static const int kFanoutTotal = 1 << kNumRadixBits;

  static const size_t kScheduleThreshold = kSmallPartitionSize * kFanoutPass2 / kPartitionSize * 1.5;
};

#endif // PARAMS_H_
