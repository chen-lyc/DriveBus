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

void read_data(SharedData *p, MessageDesc desc_ring[], int n) {
    for (int i = 0; i < n; i++) {
        int offset = desc_ring[i].offset;
        int len = desc_ring[i].len;

        int idx = 0;
        while (idx < kClassNum && len > kClassSize[idx]) ++idx;
        if (idx > kClassNum) {
            cout << "offset is " << offset << " len is " << len << ", over size" << endl;
            continue;
        }

        cout << "read " << len << " byte, offset is " << offset << endl;
        write(1, p->data + offset, len);
        cout << endl;

        int off_16 = p->tail.offset_16.load(memory_order_relaxed);
        memcpy(p->data + off_16, &offset, sizeof(int));
        p->tail.offset_16.store(offset, std::memory_order_release);
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

        for (int i = 0; i < num; i++) {
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

    munmap(p, sizeof(SharedData));
    shm_unlink("/shm");
    close(fd);

    return 0;
}