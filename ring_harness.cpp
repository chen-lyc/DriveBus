#include <algorithm>
#include <atomic>
#include <cstring>
#include <iostream>
#include <thread>

using namespace std;

struct MessageDesc {
    int offset;
    int len;
};

const int kDescNum = 16;
const int kTaskNum = 26 * 10;

struct SharedData {
    atomic<int> offset_read = 0;
    atomic<int> offset_write = 0;
    MessageDesc desc_ring[kDescNum];
};

void read_data(MessageDesc desc_ring[], int n) {
    for (int i = 0; i < n; i++) {
        cout << "offset is " << desc_ring[i].offset << ", len is " << desc_ring[i].len << endl;
    }
}

void run_a(SharedData *p) {
    int consumed = 0;
    while (consumed < kTaskNum) {
        int rd = p->offset_read.load(memory_order_relaxed);
        int wr = p->offset_write.load(memory_order_acquire);
        if (rd == wr) {
            continue;
        }

        if (rd > wr) {
            int num = kDescNum - rd;
            MessageDesc desc_ring[num];
            copy(p->desc_ring + rd, p->desc_ring + kDescNum, desc_ring);
            read_data(desc_ring, num);
            consumed += num;

            p->offset_read.store(0, memory_order_release);
        }

        wr = p->offset_write.load(memory_order_acquire);
        rd = p->offset_read.load(memory_order_relaxed);
        while (wr > rd) {
            int num = wr - rd;
            MessageDesc desc_ring[num];
            copy(p->desc_ring + rd, p->desc_ring + wr, desc_ring);
            read_data(desc_ring, num);
            consumed += num;

            rd = wr;
            p->offset_read.store(wr, memory_order_release);
            wr = p->offset_write.load(memory_order_acquire);
        }
    }
}

void run_b(SharedData *p) {
    int task_num = kTaskNum;
    int seq = 0;
    while (task_num) {
        int rd = p->offset_read.load(memory_order_acquire);
        int wr = p->offset_write.load(memory_order_relaxed);
        while ((rd > wr && rd <= wr + 1) || wr + 1 >= rd + kDescNum) {
            rd = p->offset_read.load(memory_order_acquire);
            wr = p->offset_write.load(memory_order_relaxed);
        }

        MessageDesc desc{seq, seq};
        memcpy(p->desc_ring + wr, &desc, sizeof(MessageDesc));
        ++seq;

        int candidate = wr + 1;
        if (candidate >= kDescNum) p->offset_write.store(candidate % kDescNum, memory_order_release);
        else p->offset_write.store(candidate, memory_order_release);

        --task_num;
    }
}

int main() {
    SharedData shm{};
    thread a_thread(run_a, &shm);
    thread b_thread(run_b, &shm);

    b_thread.join();
    a_thread.join();

    return 0;
}
