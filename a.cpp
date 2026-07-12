#include "include/logger.h"
#include <atomic>
#include <cstring>
#include <iostream>
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

int recv_fd(int sock) {
    struct msghdr msg{};

    char dummy;
    struct iovec iov;
    iov.iov_base = &dummy;
    iov.iov_len = sizeof(dummy);

    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;

    char control[CMSG_SPACE(sizeof(int))];
    memset(control, 0, sizeof(control));

    msg.msg_control = control;
    msg.msg_controllen = sizeof(control);

    int ret = recvmsg(sock, &msg, 0);
    if (ret < 0) return -1;

    struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    if (cmsg == nullptr) return -1;
    if (cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SCM_RIGHTS) return -1;

    int fd = *reinterpret_cast<int *>(CMSG_DATA(cmsg));
    return fd;
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

int expected_seq = 0;

void read_data(SharedData *p, MessageDesc desc_ring[], int n) {
    for (int i = 0; i < n; ++i) {
        int offset = desc_ring[i].offset;
        int len = desc_ring[i].len;
        int size_class = size_class_for(len);

        int idx = 0;
        while (idx < kClassNum && len > kClassSize[idx]) ++idx;
        if (idx > kClassNum) {
            cout << "offset is " << offset << " len is " << len << ", over size" << endl;
            continue;
        }

        int magic, seq;
        memcpy(&magic, p->data + offset, sizeof(int));
        memcpy(&seq, p->data + offset + sizeof(int), sizeof(int));
        cout << "read " << len << " byte, offset is " << offset << ", seq is " << seq << endl;
        if (magic != kMagic) {
            cout << "error magic: expected is " << kMagic << ", actual is " << magic << endl;
            exit(1);
        }
        if (seq != expected_seq) {
            cout << "error seq: " << " expected is " << expected_seq << ", actual is " << seq << endl;
            exit(1);
        }
        if (len > 2 * sizeof(int)) {
            write(1, p->data + offset + 2 * sizeof(int), len - 2 * sizeof(int));
        }
        cout << endl;

        ++expected_seq;

        {
            if (offset < kFirstOffset[size_class] || offset > kLastOffset[size_class]) {
                cout << "size_class " << size_class << " offset is out of range: head_off is " << offset << endl;
                exit(1);
            }

            pthread_mutex_lock(&p->outstanding.mutex);

            int idx = offset / kClassSize[size_class];
            // cout << "the " << idx << " chunk free" << endl;
            if (p->outstanding.chunk[size_class][idx] == false) {
                cout << "size_class " << size_class << " the " << idx << " chunk free twice" << endl;
                exit(1);
            }
            p->outstanding.chunk[size_class][idx] = false;

            pthread_mutex_unlock(&p->outstanding.mutex);
        }

        int last_tail_off = p->tail.offset[size_class].load(memory_order_relaxed);
        memcpy(p->data + last_tail_off, &offset, sizeof(int));
        p->tail.offset[size_class].store(offset, std::memory_order_release);
    }
}

int main() {
    const string path = "/tmp/demo.sock";
    unlink(path.c_str());

    int listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, path.c_str());

    int ret = bind(listen_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
    if (ret < 0) return -1;

    ret = listen(listen_fd, 1);
    if (ret < 0) return -1;

    cout << "receiver listening...\n";

    int conn_fd = accept(listen_fd, nullptr, nullptr);

    int efd = recv_fd(conn_fd);

    int epollfd = epoll_create1(0);
    epoll_event event;
    event.data.fd = efd;
    event.events = EPOLLET | EPOLLIN;
    epoll_ctl(epollfd, EPOLL_CTL_ADD, efd, &event);

    int fd = shm_open("/shm", O_RDWR, 0666);
    if (fd == -1) {
        perror("shm_open");
        close(fd);
        return 1;
    }
    SharedData *p = static_cast<SharedData *>(mmap(nullptr, sizeof(SharedData), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));

    uint64_t val;
    ssize_t n = read(efd, &val, sizeof(val));
    if (n > 0) {
        int rd = p->offset_read.load(memory_order_relaxed);
        int wr = p->offset_write.load(std::memory_order_acquire);
        while (wr > rd) {
            int num = wr - rd;
            MessageDesc desc_ring[num];
            copy(p->desc_ring + rd, p->desc_ring + wr, desc_ring);
            read_data(p, desc_ring, num);

            rd = wr;
            p->offset_read.store(wr, std::memory_order_release);
            wr = p->offset_write.load(std::memory_order_acquire);
        }
        cout << "read over" << endl;
    }
    // 防止 b 进程在 epoll_wait 之前触发时间，导致 ET 模式无法收到

    int maxevents = 1024;
    epoll_event events[maxevents];
    while (true) {
        int num = epoll_wait(epollfd, events, maxevents, 1000);
        cout << "num = " << num << endl;
        if (num == 0) {
            cout << "time out" << endl;
            break;
        }

        for (int i = 0; i < num; ++i) {
            int fd = events[i].data.fd;
            if (fd == efd) {
                uint64_t val;
                read(efd, &val, sizeof(val));

                int rd = p->offset_read.load(memory_order_relaxed);
                int wr = p->offset_write.load(std::memory_order_acquire);
                if (rd > wr) {
                    int num = kDescNum - rd;
                    MessageDesc desc_ring[num];
                    copy(p->desc_ring + rd, p->desc_ring + kDescNum, desc_ring);
                    read_data(p, desc_ring, num);

                    p->offset_read.store(0, std::memory_order_release);
                }

                wr = p->offset_write.load(std::memory_order_acquire);
                rd = p->offset_read.load(memory_order_relaxed);
                while (wr > rd) {
                    int num = wr - rd;
                    MessageDesc desc_ring[num];
                    copy(p->desc_ring + rd, p->desc_ring + wr, desc_ring);
                    read_data(p, desc_ring, num);

                    rd = wr;
                    p->offset_read.store(wr, std::memory_order_release);
                    wr = p->offset_write.load(std::memory_order_acquire);
                }
                cout << "read over" << endl;
            }
        }
        this_thread::sleep_for(chrono::milliseconds(1));
    }

    write(1, p->data, sizeof(p->data));
    cout << endl;

    {
        pthread_mutex_lock(&p->outstanding.mutex);

        for (int i = 0; i < kClassNum; ++i) {
            for (int j = 0; j < kMemorySize_16; ++j) {
                if (p->outstanding.chunk[i][j] == true) {
                    cout << "class " << i << " the " << i << " chunk not freee" << endl;
                }
            }
        }

        pthread_mutex_unlock(&p->outstanding.mutex);
    }
    pthread_mutex_destroy(&p->outstanding.mutex);

    munmap(p, sizeof(SharedData));
    shm_unlink("/shm");
    close(fd);

    return 0;
}