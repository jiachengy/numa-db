#ifndef PARAMS_H_
#define PARAMS_H_

class Params
{
 public:
	static const int kNumRadixBits = 14;
	static const int kNumPasses = 1; 

	static const int kNumBitsPass1 = kNumRadixBits / kNumPasses;
	static const int kFanoutPass1 = (1 << (kNumRadixBits/kNumPasses));
	static const int kFanoutPass2 = (1 << (kNumRadixBits-(kNumRadixBits/kNumPasses)));
	
};

#endif // PARAMS_H_
