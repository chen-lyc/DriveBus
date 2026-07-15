#pragma once

#include <atomic>
#include <cstdint>
#include <limits>
#include <pthread.h>

inline constexpr uint32_t kDescriptorSlotCount = 16;
inline constexpr size_t kMaxSubscribers = 2;
inline constexpr uint32_t kInvalidIndex = std::numeric_limits<uint32_t>::max();

inline constexpr int kClassCount = 5;
inline constexpr uint32_t kClassSizeBytes[] = {16, 128, 256, 512, 1024};

inline constexpr uint32_t kChunkCountBySizeClass[] = {64, 32, 16, 8, 8};
inline constexpr uint32_t kTotalChunkCount =
    kChunkCountBySizeClass[0] +
    kChunkCountBySizeClass[1] +
    kChunkCountBySizeClass[2] +
    kChunkCountBySizeClass[3] +
    kChunkCountBySizeClass[4];

inline constexpr uint32_t kChunkIndexBaseBySizeClass[] = {0, 64, 96, 112, 120};

inline constexpr int kMaxChunkCountPerSizeClass = 64;

inline constexpr int kPayloadPoolSizeBytes = 1024 * 21;

inline constexpr uint32_t kInvalidOffset = std::numeric_limits<uint32_t>::max();

inline constexpr int kMagic = 21354532;

inline constexpr uint32_t kFirstOffset[] = {
    0,
    16 * 64,
    16 * 64 + 128 * 32,
    16 * 64 + 128 * 32 + 256 * 16,
    16 * 64 + 128 * 32 + 256 * 16 + 512 * 8};
inline constexpr uint32_t kLastOffset[] = {
    kFirstOffset[1] - kClassSizeBytes[0],
    kFirstOffset[2] - kClassSizeBytes[1],
    kFirstOffset[3] - kClassSizeBytes[2],
    kFirstOffset[4] - kClassSizeBytes[3],
    kPayloadPoolSizeBytes - kClassSizeBytes[4],
};

struct MessageDescriptor {
    uint32_t offset;
    uint32_t len;
};

struct FreeListHeads {
    std::atomic<uint32_t> offset[kClassCount];
};

struct FreeListTails {
    std::atomic<uint32_t> offset[kClassCount];
};

struct ChunkUsageTracker {
    pthread_mutex_t mutex;
    bool is_in_use[kClassCount][kMaxChunkCountPerSizeClass];
};

struct SharedData {
    std::atomic<uint32_t> descriptor_read_indexs[kMaxSubscribers]{}; // 还未读的第一个偏移量位置
    std::atomic<uint32_t> descriptor_write_index = 0;                // 还未写的第一个偏移量位置
    // 同环次数已写区间 [rd, wr), 单位是描述符个数
    MessageDescriptor desc_ring[kDescriptorSlotCount];
    FreeListHeads head;
    FreeListTails tail;
    std::atomic<uint8_t> chunk_reference_counts[kTotalChunkCount]{};
    char data[1024 * 21];

#ifdef ENABLE_DEBUG_CHECKS
    ChunkUsageTracker chunk_usage_tracker;
#endif
};