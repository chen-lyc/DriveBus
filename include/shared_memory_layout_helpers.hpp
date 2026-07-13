#include "shared_memory_layout.hpp"

inline int find_size_class(uint32_t size) {
    for (int i = 0; i < kClassCount; ++i) {
        if (size <= kClassSizeBytes[i]) {
            return i;
        }
    }
    return -1;
}