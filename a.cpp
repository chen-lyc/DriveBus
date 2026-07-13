#include "include/logger.h"
#include "include/shared_memory_layout.hpp"
#include "include/shared_memory_layout_helpers.hpp"
#include <atomic>
#include <cstring>
#include <cstddef>
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

    ssize_t ret = recvmsg(sock, &msg, 0);
    if (ret < 0) return -1;

    struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    if (cmsg == nullptr) return -1;
    if (cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SCM_RIGHTS) return -1;

    int fd = *reinterpret_cast<int *>(CMSG_DATA(cmsg));
    return fd;
}

int expected_seq = 0;

void read_data(SharedData *p, MessageDescriptor desc_ring[], const size_t descriptor_count) {
    for (size_t descriptor_index = 0; descriptor_index < descriptor_count; ++descriptor_index) {
        uint32_t offset = desc_ring[descriptor_index].offset;
        uint32_t message_size_bytes = desc_ring[descriptor_index].len;

        int size_class = find_size_class(message_size_bytes);
        if (size_class < 0) {
            cout << "expected_seq is " << expected_seq << " size is " << message_size_bytes << ": size is too big" << endl;
            exit(1);
        }

        int magic, seq;
        memcpy(&magic, p->data + offset, sizeof(int));
        memcpy(&seq, p->data + offset + sizeof(int), sizeof(int));
        cout << "read " << message_size_bytes << " byte, offset is " << offset << ", seq is " << seq << endl;
        if (magic != kMagic) {
            cout << "error magic: expected is " << kMagic << ", actual is " << magic << endl;
            exit(1);
        }
        if (seq != expected_seq) {
            cout << "error seq: " << " expected is " << expected_seq << ", actual is " << seq << endl;
            exit(1);
        }
        if (message_size_bytes > 2 * sizeof(int)) {
            write(1, p->data + offset + 2 * sizeof(int), message_size_bytes - 2 * sizeof(int));
        }
        cout << endl;

        ++expected_seq;

        {
            if (offset < kFirstOffset[size_class] || offset > kLastOffset[size_class]) {
                cout << "size_class " << size_class << " offset is out of range: head_off is " << offset << endl;
                exit(1);
            }

            pthread_mutex_lock(&p->chunk_usage_tracker.mutex);

            size_t block_index = offset / kClassSizeBytes[size_class];
            // cout << "the " << idx << " chunk free" << endl;
            if (p->chunk_usage_tracker.is_in_use[size_class][block_index] == false) {
                cout << "size_class " << size_class << " the " << block_index << " chunk free twice" << endl;
                exit(1);
            }
            p->chunk_usage_tracker.is_in_use[size_class][block_index] = false;

            pthread_mutex_unlock(&p->chunk_usage_tracker.mutex);
        }

        uint32_t last_tail_off = p->tail.offset[size_class].load(memory_order_relaxed);
        memcpy(p->data + last_tail_off, &offset, sizeof(uint32_t));
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
        uint32_t rd = p->descriptor_read_index.load(memory_order_relaxed);
        uint32_t wr = p->descriptor_write_index.load(std::memory_order_acquire);
        while (wr > rd) {
            const size_t descriptor_count = static_cast<size_t>(wr - rd);
            MessageDescriptor desc_ring[descriptor_count];
            copy(p->desc_ring + rd, p->desc_ring + wr, desc_ring);

            rd = wr;
            p->descriptor_read_index.store(wr, std::memory_order_release);
            wr = p->descriptor_write_index.load(std::memory_order_acquire);

            read_data(p, desc_ring, descriptor_count);
        }
        cout << "read over" << endl;
    }
    // 防止 b 进程在 epoll_wait 之前触发时间，导致 ET 模式无法收到

    int maxevents = 1024;
    epoll_event events[maxevents];
    while (true) {
        int event_count = epoll_wait(epollfd, events, maxevents, 1000);
        cout << "event_cout = " << event_count << endl;
        if (event_count == 0) {
            cout << "time out" << endl;
            break;
        }

        for (int i = 0; i < event_count; ++i) {
            int fd = events[i].data.fd;
            if (fd == efd) {
                uint64_t val;
                read(efd, &val, sizeof(val));

                uint32_t rd = p->descriptor_read_index.load(memory_order_relaxed);
                uint32_t wr = p->descriptor_write_index.load(std::memory_order_acquire);
                if (rd > wr) {
                    const size_t descriptor_count = static_cast<size_t>(kDescriptorSlotCount - rd);
                    MessageDescriptor desc_ring[descriptor_count];
                    copy(p->desc_ring + rd, p->desc_ring + kDescriptorSlotCount, desc_ring);

                    p->descriptor_read_index.store(0, std::memory_order_release);

                    read_data(p, desc_ring, descriptor_count);
                }

                wr = p->descriptor_write_index.load(std::memory_order_acquire);
                rd = p->descriptor_read_index.load(memory_order_relaxed);
                while (wr > rd) {
                    const size_t descriptor_count = static_cast<size_t>(wr - rd);
                    MessageDescriptor desc_ring[descriptor_count];
                    copy(p->desc_ring + rd, p->desc_ring + wr, desc_ring);

                    rd = wr;
                    p->descriptor_read_index.store(wr, std::memory_order_release);
                    wr = p->descriptor_write_index.load(std::memory_order_acquire);

                    read_data(p, desc_ring, descriptor_count);
                }
                cout << "read over" << endl;
            }
        }
        this_thread::sleep_for(chrono::milliseconds(1));
    }

    write(1, p->data, sizeof(p->data));
    cout << endl;

    {
        pthread_mutex_lock(&p->chunk_usage_tracker.mutex);

        for (int i = 0; i < kClassCount; ++i) {
            for (int j = 0; j < kBlockCountBySizeClass_16; ++j) {
                if (p->chunk_usage_tracker.is_in_use[i][j] == true) {
                    cout << "class " << i << " the " << i << " chunk not freee" << endl;
                }
            }
        }

        pthread_mutex_unlock(&p->chunk_usage_tracker.mutex);
    }
    pthread_mutex_destroy(&p->chunk_usage_tracker.mutex);

    munmap(p, sizeof(SharedData));
    shm_unlink("/shm");
    close(fd);

    return 0;
}