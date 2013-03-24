#ifndef PARAMS_H_
#define PARAMS_H_

#include <unistd.h>

class Params
{
 public:
  static const size_t kBlockSize = 4096; // 32KB
  static const size_t kPartitionSize = 32768 * 4; // 1M

  static const int kNumRadixBits = 7;
  static const int kNumPasses = 1; 

  static const int kNumBitsPass1 = kNumRadixBits / kNumPasses;
  static const int kFanoutPass1 = (1 << (kNumRadixBits/kNumPasses));
  static const int kFanoutPass2 = (1 << (kNumRadixBits-(kNumRadixBits/kNumPasses)));
	
};

#endif // PARAMS_H_
