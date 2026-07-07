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

struct SharedData {
    atomic<int> offset_read = 0;
    atomic<int> offset_write = 0;
    char data[64 * 26];
};

int main() {
    shm_unlink("/shm");
    int fd = shm_open("/shm", O_CREAT | O_RDWR, 0666);
    SharedData data;
    ftruncate(fd, sizeof(data));
    SharedData *p = static_cast<SharedData *>(mmap(nullptr, sizeof(data), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));

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

    int task_num = 26 * 2;
    for (char c = 'a'; task_num; c++) {
        char temp[64];
        memset(temp, c, sizeof(temp));

        int rd = p->offset_read.load(std::memory_order_acquire);
        int wr = p->offset_write.load(memory_order_relaxed);
        while ((rd > wr && rd <= wr + sizeof(temp)) || wr + sizeof(temp) >= rd + sizeof(p->data)) {
            this_thread::sleep_for(chrono::milliseconds(1));
            rd = p->offset_read.load(std::memory_order_acquire);
            wr = p->offset_write.load(memory_order_relaxed);
        }

        wr = p->offset_write.load(memory_order_relaxed);
        memcpy(p->data + wr, temp, sizeof(temp));
        int candidate = wr + sizeof(temp);
        if (candidate >= sizeof(p->data)) p->offset_write.store(candidate % sizeof(p->data), std::memory_order_release);
        else p->offset_write.store(candidate, std::memory_order_release);

        uint64_t val = 1;
        write(efd, &val, sizeof(val));
        cout << c << " write over" << endl;
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