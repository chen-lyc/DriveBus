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

struct SharedData {
    atomic<int> offset_read = 0;
    atomic<int> offset_write = 0;
    char data[64 * 26];
};

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
            write(1, p->data + rd, wr - rd);
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
                    write(1, p->data + rd, sizeof(p->data) - rd);
                    p->offset_read.store(0, std::memory_order_release);
                }

                wr = p->offset_write.load(std::memory_order_acquire);
                rd = p->offset_read.load(memory_order_relaxed);
                while (wr > rd) {
                    write(1, p->data + rd, wr - rd);
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