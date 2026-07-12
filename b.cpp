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
    const int M = 1;

    uniform_int_distribution<int> choose(0, 1);

    uniform_int_distribution<int> small_dist(8 * K, 16 * K);
    uniform_int_distribution<int> big_dist(128 * M, 1024 * M);

    if (choose(gen) == 0) {
        return small_dist(gen);
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

const int kDescNum = 16;

const int kClassNum = 5;
constexpr int kClassSize[] = {16, 128, 256, 512, 1024};

const int kMemorySize[] = {64, 32, 16, 8, 8};

const int kMemorySize_16 = 64;
const int kMemorySize_128 = 32;
const int kMemorySize_256 = 16;
const int kMemorySize_512 = 8;
const int kMemorySize_1024 = 8;

const int kDataSize = 1024 * 21;

const int kInvalidOffset = -1;

const int kMagic = 21354532;

const int kFirstOffset[] = {
    0,
    16 * 64,
    16 * 64 + 128 * 32,
    16 * 64 + 128 * 32 + 256 * 16,
    16 * 64 + 128 * 32 + 256 * 16 + 512 * 8};
const int kLastOffset[] = {
    kFirstOffset[1] - kClassSize[0],
    kFirstOffset[2] - kClassSize[1],
    kFirstOffset[3] - kClassSize[2],
    kFirstOffset[4] - kClassSize[3],
    kDataSize - kClassSize[4],
};

int size_class_for(int size) {
    for (int i = 0; i < kClassNum; ++i) {
        if (size <= kClassSize[i]) {
            return i;
        }
    }
    return -1;
}

struct MessageDesc {
    int offset;
    int len;
};

struct FreeListHeads {
    std::atomic<int> offset[kClassNum];
};

struct FreeListTails {
    std::atomic<int> offset[kClassNum];
};

struct Outstanding {
    pthread_mutex_t mutex;
    bool chunk[kClassNum][1024 * kMemorySize_1024];
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

    memset(outstanding.chunk, 0, sizeof(outstanding.chunk));
}

void shm_init(SharedData &shm) {
    shm.offset_read.store(0, memory_order_relaxed);
    shm.offset_write.store(0, memory_order_relaxed);

    shm.head.offset[0].store(0, memory_order_relaxed);
    for (int i = 1; i < kClassNum; ++i) {
        shm.head.offset[i].store(shm.head.offset[i - 1].load(memory_order_relaxed) + kClassSize[i - 1] * kMemorySize[i - 1], memory_order_relaxed);
    }

    for (int i = 0; i < kClassNum - 1; ++i) {
        shm.tail.offset[i].store(shm.head.offset[i + 1].load(memory_order_relaxed) - kClassSize[i], memory_order_relaxed);
    }
    shm.tail.offset[kClassNum - 1].store(sizeof(shm.data) - kClassSize[kClassNum - 1], memory_order_relaxed);

    auto init = [&](int size, const int kMemorySize, int offset) {
        for (int i = 0; i < kMemorySize - 1; ++i) {
            int next_offset = offset + size;
            memcpy(shm.data + offset, &next_offset, sizeof(int));
            offset = next_offset;
        }
        memcpy(shm.data + offset, &kInvalidOffset, sizeof(int));
    };
    for (int i = 0; i < kClassNum; ++i) {
        init(kClassSize[i], kMemorySize[i], shm.head.offset[i]);
    }
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
        int size_class = size_class_for(len);
        if (size_class == -1) {
            cout << "seq is " << seq << " size is " << len << ": size is too big" << endl;
            return 1;
        }

        int rd = p->offset_read.load(std::memory_order_acquire);
        int wr = p->offset_write.load(memory_order_relaxed);
        while ((rd > wr && rd <= wr + 1) || wr + 1 >= rd + kDescNum) {
            this_thread::sleep_for(chrono::milliseconds(1));
            rd = p->offset_read.load(std::memory_order_acquire);
            wr = p->offset_write.load(memory_order_relaxed);
        }

        int head_off = p->head.offset[size_class].load(memory_order_relaxed);
        int tail_off = p->tail.offset[size_class].load(std::memory_order_acquire);
        while (head_off == tail_off) {
            this_thread::sleep_for(chrono::milliseconds(1));
            tail_off = p->tail.offset[size_class].load(std::memory_order_acquire);
        }
        int next_head_off;
        memcpy(&next_head_off, p->data + head_off, sizeof(int));
        p->head.offset[size_class].store(next_head_off, memory_order_relaxed);

        MessageDesc desc{head_off, len};
        memcpy(p->desc_ring + wr, &desc, sizeof(MessageDesc));

        memcpy(p->data + head_off, &kMagic, sizeof(int));
        memcpy(p->data + head_off + sizeof(int), &seq, sizeof(int));
        ++seq;
        if (len > 2 * sizeof(int)) {
            memset(p->data + head_off + 2 * sizeof(int), c, len - 2 * sizeof(int));
        }

        {
            if (head_off < kFirstOffset[size_class] || head_off > kLastOffset[size_class]) {
                cout << "size_class " << size_class << " offset is out of range: head_off is " << head_off << endl;
                return -1;
            }

            pthread_mutex_lock(&p->outstanding.mutex);

            int idx = head_off / kClassSize[size_class];
            if (p->outstanding.chunk[size_class][idx] == true) {
                cout << "size_class " << size_class << " the " << idx << " chunk error use again" << endl;
                pthread_mutex_unlock(&p->outstanding.mutex);
                return 1;
            } else {
                // cout << "size_class " << size_class <<"the " << idx << " chunk using" << endl;
                p->outstanding.chunk[size_class][idx] = true;
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