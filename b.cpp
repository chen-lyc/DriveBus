#include "include/logger.h"
#include <atomic>
#include <cstring>
#include <iostream>
#include <random>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <fcntl.h>
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

void send_fd(int sock, int fd) {
    struct msghdr msg{};

    char dummy = 'F';
    struct iovec iov;
    iov.iov_base = &dummy;
    iov.iov_len = sizeof(dummy);

    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;

    char control[CMSG_SPACE(sizeof(int))];
    memset(control, 0, sizeof(control));

    msg.msg_control = control;
    msg.msg_controllen = sizeof(control);

    struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(sizeof(int));

    *reinterpret_cast<int *>(CMSG_DATA(cmsg)) = fd;
    sendmsg(sock, &msg, 0);
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

const int kClassNum = 5;
constexpr int kClassSize[] = {16, 128, 256, 512, 1024};

const int kMemorySize_16 = 64;
const int kMemorySize_128 = 32;
const int kMemorySize_256 = 16;
const int kMemorySize_512 = 8;
const int kMemorySize_1024 = 8;

const int kInvalidOffset = -1;

const int kMagic = 21354532;

const int kFirstOffset16 = 0;
const int kLastOffset16 = 16 * (kMemorySize_16 - 1);

struct Outstanding {
    pthread_mutex_t mutex;
    bool chunk_16[kMemorySize_16];
};

struct SharedData {
    atomic<int> offset_read = 0;  // 还未读的第一个偏移量位置
    atomic<int> offset_write = 0; // 还未写的第一个偏移量位置
    // 同环次数已写区间 [rd, wr), 单位是描述符个数
    MessageDesc desc_ring[kDescNum];
    FreeListHeads head;
    FreeListTails tail;
    char data[1024 * 21];

    Outstanding outstanding;
};

void outstanding_init(Outstanding &outstanding) {
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
    pthread_mutex_init(&outstanding.mutex, &attr);
    pthread_mutexattr_destroy(&attr);

    memset(outstanding.chunk_16, 0, kMemorySize_16);
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
        for (int i = 0; i < kMemorySize; ++i) {
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

int main() {
    shm_unlink("/shm");
    int fd = shm_open("/shm", O_CREAT | O_RDWR, 0666);
    ftruncate(fd, sizeof(SharedData));
    SharedData *p = static_cast<SharedData *>(mmap(nullptr, sizeof(SharedData), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));

    shm_init(*p);
    outstanding_init(p->outstanding);

    const string path = "/tmp/demo.sock";

    int sock = socket(AF_UNIX, SOCK_STREAM, 0);

    struct sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, path.c_str());

    int ret = connect(sock, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
    int fail_num = 0;
    while (ret < 0 && fail_num < 3) {
        sleep(1);
        ++fail_num;
        ret = connect(sock, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
    }
    if (ret < 0) return -1;

    int efd = eventfd(0, EFD_NONBLOCK);
    send_fd(sock, efd);

    int task_num = 26 * 3000;
    int seq = 0;
    for (char c = 'a'; task_num; ++c) {
        int len = get_msg_size();
        // int len = 16;

        int rd = p->offset_read.load(std::memory_order_acquire);
        int wr = p->offset_write.load(memory_order_relaxed);
        while ((rd > wr && rd <= wr + 1) || wr + 1 >= rd + kDescNum) {
            this_thread::sleep_for(chrono::milliseconds(1));
            rd = p->offset_read.load(std::memory_order_acquire);
            wr = p->offset_write.load(memory_order_relaxed);
        }

        int head_off_16 = p->head.offset_16.load(memory_order_relaxed);
        int tail_off_16 = p->tail.offset_16.load(std::memory_order_acquire);
        while (head_off_16 == tail_off_16) {
            this_thread::sleep_for(chrono::milliseconds(1));
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

        {
            if (head_off_16 < kFirstOffset16 || head_off_16 > kLastOffset16) {
                cout << "offset is out of range: head_off_16 is " << head_off_16 << endl;
                return -1;
            }

            pthread_mutex_lock(&p->outstanding.mutex);

            int idx = head_off_16 / 16;
            if (p->outstanding.chunk_16[idx] == true) {
                cout << "the " << idx << " chunk error use again" << endl;
                pthread_mutex_unlock(&p->outstanding.mutex);
                return 1;
            } else {
                // cout << "the " << idx << " chunk using" << endl;
                p->outstanding.chunk_16[idx] = true;
            }

            pthread_mutex_unlock(&p->outstanding.mutex);
        }

        int candidate = wr + 1;
        if (candidate >= kDescNum) p->offset_write.store(candidate % kDescNum, std::memory_order_release);
        else p->offset_write.store(candidate, std::memory_order_release);

        uint64_t val = 1;
        write(efd, &val, sizeof(val));
        cout << "write " << len << " byte " << c << endl;
        --task_num;
        if (c == 'z') c = 'a' - 1;
    }
    uint64_t val = 1;
    write(efd, &val, sizeof(val));
    cout << "all write over" << endl;

    munmap(p, sizeof(SharedData));
    close(fd);

    return 0;
}