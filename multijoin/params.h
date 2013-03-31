#ifndef PARAMS_H_
#define PARAMS_H_

#include <unistd.h>

#ifndef PRE_ALLOC
#define PRE_ALLOC
#endif

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64
#endif


class Params
{
 public:
  static size_t kMaxHtTuples;

  static const size_t kPartitionSize = 1024 * 1024; // 256K
  static const size_t kBlockSize = 1024 * 256; // 32KB

  static const int kHtInflateRate = 1; // can only be 2^k

  static const int kNumRadixBits = 14;
  static const int kNumPasses = 2; 

  static const int kNumBitsPass1 = kNumRadixBits / kNumPasses;
  static const int kNumBitsPass2 = kNumRadixBits-(kNumRadixBits/kNumPasses);

  static const int kOffsetPass1 = 0;
  static const int kOffsetPass2 = kNumBitsPass1;

  static const int kFanoutPass1 = 1 << kNumBitsPass1;
  static const int kFanoutPass2 = 1 << kNumBitsPass2;
  static const int kFanoutTotal = 1 << kNumRadixBits;
};

#endif // PARAMS_H_
