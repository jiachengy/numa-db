#include "hash.h"

/* 32-bit MurmurHash 3 */
/* http://code.google.com/p/smhasher/wiki/MurmurHash3 */

uint32_t hash32(uint32_t key)
{
    int h = key;
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
}

