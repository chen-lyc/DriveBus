#include "include/logger.h"
#include <atomic>
#include <cstring>
#include <iostream>
#include <random>
#include <unistd.h>
#include <stdint.h>
using namespace std;

int get_msg_size() {
    random_device rd;
    mt19937 gen(rd());

    const int K = 1;
    const int M = 1 * 2;

    uniform_int_distribution<int> choose(0, 1);

    uniform_int_distribution<int> small_dist(8 * K, 16 * K);
    uniform_int_distribution<int> big_dist(64 * M, 512 * M);

    return small_dist(gen);
    if (choose(gen) == 0) {
    } else {
        return big_dist(gen);
    }
}

struct MessageDesc {
    int offset;
    int len;
};

struct FreeListHeads {
    atomic<int> offset_16;
    int offset_128;
    int offset_256;
    int offset_512;
    int offset_1024;
};

struct FreeListTails {
    std::atomic<int> offset_16;
    std::atomic<int> offset_128;
    std::atomic<int> offset_256;
    std::atomic<int> offset_512;
    std::atomic<int> offset_1024;
};

const int kDescNum = 16;

struct SharedData {
    atomic<int> offset_read = 0;  // 还未读的第一个偏移量位置
    atomic<int> offset_write = 0; // 还未写的第一个偏移量位置
    // 同环次数已写区间 [rd, wr), 单位是描述符个数
    MessageDesc desc_ring[kDescNum];
    FreeListHeads head;
    FreeListTails tail;
    char data[1024 * 21];
};

const int kClassNum = 5;
const int kClassSize[] = {16, 128, 256, 512, 1024};

const int kMemorySize_16 = 64;
const int kMemorySize_128 = 32;
const int kMemorySize_256 = 16;
const int kMemorySize_512 = 8;
const int kMemorySize_1024 = 8;

const int kInvalidOffset = -1;

const int kMagic = 21354532;

void read_data(SharedData *p, MessageDesc desc_ring[], int n) {
    for (int i = 0; i < n; i++) {
        int offset = desc_ring[i].offset;
        int len = desc_ring[i].len;

        int idx = 0;
        while (idx < kClassNum && len > kClassSize[idx]) ++idx;
        if (idx > kClassNum) {
            // cout << "offset is " << offset << " len is " << len << ", over size" << endl;
            continue;
        }

        int magic, seq;
        memcpy(&magic, p->data + offset, sizeof(int));
        memcpy(&seq, p->data + offset + sizeof(int), sizeof(int));
        // cout << "read " << len << " byte, offset is " << offset << ", seq is " << seq << endl;
        if (magic != kMagic) {
            cout << "error magic: " << magic << ", seq is " << seq << endl;
        }
        if (len > 2 * sizeof(int)) {
            write(1, p->data + offset + 2 * sizeof(int), len - 2 * sizeof(int));
        }
        // cout << endl;

        int last_tail_off_16 = p->tail.offset_16.load(memory_order_relaxed);
        memcpy(p->data + last_tail_off_16, &offset, sizeof(int));
        p->tail.offset_16.store(offset, std::memory_order_release);
    }
}

void shm_init(SharedData &shm) {
    shm.offset_read.store(0, memory_order_relaxed);
    shm.offset_write.store(0, memory_order_relaxed);

    shm.head.offset_16.store(0, memory_order_relaxed);
    shm.head.offset_128 = shm.head.offset_16.load(memory_order_relaxed) + 16 * kMemorySize_16;
    shm.head.offset_256 = shm.head.offset_128 + 128 * kMemorySize_128;
    shm.head.offset_512 = shm.head.offset_256 + 256 * kMemorySize_256;
    shm.head.offset_1024 = shm.head.offset_512 + 512 * kMemorySize_512;

    shm.tail.offset_16.store(16 * (kMemorySize_16 - 1), memory_order_relaxed);

    auto init = [&](int size, const int kMemorySize, int offset) {
        for (int i = 0; i < kMemorySize; i++) {
            int next_offset = offset + size;
            memcpy(shm.data + offset, &next_offset, sizeof(int));
            offset = next_offset;
        }
        memcpy(shm.data + offset, &kInvalidOffset, sizeof(int));
    };
    init(16, kMemorySize_16, 0);
    init(128, kMemorySize_128, shm.head.offset_128);
    init(256, kMemorySize_256, shm.head.offset_256);
    init(512, kMemorySize_512, shm.head.offset_512);
    init(1024, kMemorySize_1024, shm.head.offset_1024);
}

void run_a(SharedData *p) {
    int consumed = 0;
    while (consumed < 26 * 100) {
        int rd = p->offset_read.load(memory_order_relaxed);
        int wr = p->offset_write.load(std::memory_order_acquire);
        if (rd == wr) {
            continue;
        }

        if (rd > wr) {
            int num = kDescNum - rd;
            MessageDesc desc_ring[num];
            copy(p->desc_ring + rd, p->desc_ring + kDescNum, desc_ring);
            read_data(p, desc_ring, num);
            consumed += num;

            p->offset_read.store(0, std::memory_order_release);
        }

        wr = p->offset_write.load(std::memory_order_acquire);
        rd = p->offset_read.load(memory_order_relaxed);
        if (wr > rd) {
            int num = wr - rd;
            MessageDesc desc_ring[num];
            copy(p->desc_ring + rd, p->desc_ring + wr, desc_ring);
            read_data(p, desc_ring, num);
            consumed += num;

            rd = wr;
            p->offset_read.store(wr, std::memory_order_release);
            wr = p->offset_write.load(std::memory_order_acquire);
        }
        // cout << "read over" << endl;
    }

    write(1, p->data, sizeof(p->data));
    cout << endl;
}

void run_b(SharedData *p) {
    int task_num = 26 * 100;
    int seq = 0;
    for (char c = 'a'; task_num; c++) {
        int len = get_msg_size();
        // int len = 16;

        int rd = p->offset_read.load(std::memory_order_acquire);
        int wr = p->offset_write.load(memory_order_relaxed);
        while ((rd > wr && rd <= wr + 1) || wr + 1 >= rd + kDescNum) {
            rd = p->offset_read.load(std::memory_order_acquire);
            wr = p->offset_write.load(memory_order_relaxed);
        }

        int head_off_16 = p->head.offset_16.load(memory_order_relaxed);
        int tail_off_16 = p->tail.offset_16.load(std::memory_order_acquire);
        while (head_off_16 == tail_off_16) {
            tail_off_16 = p->tail.offset_16.load(std::memory_order_acquire);
        }
        int next_head_off_16;
        memcpy(&next_head_off_16, p->data + head_off_16, sizeof(int));
        p->head.offset_16.store(next_head_off_16, memory_order_relaxed);

        MessageDesc desc{head_off_16, len};
        memcpy(p->desc_ring + wr, &desc, sizeof(MessageDesc));

        memcpy(p->data + head_off_16, &kMagic, sizeof(int));
        memcpy(p->data + head_off_16 + sizeof(int), &seq, sizeof(int));
        ++seq;
        if (len > 2 * sizeof(int)) {
            memset(p->data + head_off_16 + 2 * sizeof(int), c, len - 2 * sizeof(int));
        }

        int candidate = wr + 1;
        if (candidate >= kDescNum) p->offset_write.store(candidate % kDescNum, std::memory_order_release);
        else p->offset_write.store(candidate, std::memory_order_release);

        // cout << "write " << len << " byte " << c << endl;
        --task_num;
        // cout << "data: ";
        // write(1, p->data, sizeof(p->data));
        // cout << endl;
        if (c == 'z') c = 'a' - 1;
    }
    cout << "all write over" << endl;
}

int main() {
    SharedData shm{};
    shm_init(shm);

    thread a_thread(run_a, &shm);
    thread b_thread(run_b, &shm);

    b_thread.join();
    a_thread.join();

    return 0;
}
