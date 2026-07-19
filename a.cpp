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

int expected_seq = 0;

uint32_t subscriber_read_index = 0;
size_t subscriber_slot_index;

int recv_fd(int sock) {
    struct msghdr msg{};

    uint32_t recvived_subscriber_slot_index;
    struct iovec iov;
    iov.iov_base = &recvived_subscriber_slot_index;
    iov.iov_len = sizeof(recvived_subscriber_slot_index);

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
    subscriber_slot_index = recvived_subscriber_slot_index;
    return fd;
}

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

#ifdef ENABLE_DEBUG_CHECKS
        if (offset < kFirstOffset[size_class] || offset > kLastOffset[size_class]) {
            cout << "size_class " << size_class << " offset is out of range: head_off is " << offset << endl;
            pthread_mutex_unlock(&p->chunk_usage_tracker.mutex);
            exit(1);
        }
#endif

        uint32_t local_chunk_index = (offset - kFirstOffset[size_class]) / kClassSizeBytes[size_class];
        uint32_t chunk_index = kChunkIndexBaseBySizeClass[size_class] + local_chunk_index;
        uint8_t subscriber_reference_bit = 1 << subscriber_slot_index;
        uint8_t previous_reference_count = p->chunk_reference_counts[chunk_index].fetch_and(~subscriber_reference_bit, std::memory_order_acq_rel);

        if (++subscriber_read_index >= kDescriptorSlotCount) subscriber_read_index %= kDescriptorSlotCount;
        p->descriptor_read_indices[subscriber_slot_index].store(subscriber_read_index, std::memory_order_release);
        cout << "subscriber_read_index is " << subscriber_read_index << endl;

        if (previous_reference_count == subscriber_reference_bit) {
#ifdef ENABLE_DEBUG_CHECKS
            {
                pthread_mutex_lock(&p->chunk_usage_tracker.mutex);

                size_t chunk_index = (offset - kFirstOffset[size_class]) / kClassSizeBytes[size_class];
                cout << "the " << chunk_index << " chunk free" << endl;
                if (p->chunk_usage_tracker.is_in_use[size_class][chunk_index] == false) {
                    cout << "size_class " << size_class << " the " << chunk_index << " chunk free twice" << endl;
                    pthread_mutex_unlock(&p->chunk_usage_tracker.mutex);
                    exit(1);
                }
                p->chunk_usage_tracker.is_in_use[size_class][chunk_index] = false;

                pthread_mutex_unlock(&p->chunk_usage_tracker.mutex);
            }
#endif

            uint32_t last_tail_off = p->tail.offset[size_class].load(memory_order_relaxed);
            memcpy(p->data + last_tail_off, &offset, sizeof(uint32_t));
            p->tail.offset[size_class].store(offset, std::memory_order_release);
        }
    }
}

void consume_contiguous_messages(SharedData *p) {
    uint32_t wr = p->descriptor_write_index.load(std::memory_order_acquire);
    while (wr > subscriber_read_index) {
        const size_t descriptor_count = static_cast<size_t>(wr - subscriber_read_index);
        MessageDescriptor desc_ring[descriptor_count];
        copy(p->desc_ring + subscriber_read_index, p->desc_ring + wr, desc_ring);

        read_data(p, desc_ring, descriptor_count);
        wr = p->descriptor_write_index.load(std::memory_order_acquire);
    }
}

int main() {
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

    int efd = recv_fd(sock);

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
    p->descriptor_read_indices[subscriber_slot_index].store(0, memory_order_relaxed);

    uint64_t val;
    ssize_t n = read(efd, &val, sizeof(val));
    if (n > 0) {
        consume_contiguous_messages(p);
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

                uint32_t wr = p->descriptor_write_index.load(std::memory_order_acquire);
                cout << "wr is " << wr << ", rd is " << subscriber_read_index << endl;
                if (subscriber_read_index > wr) {
                    const size_t descriptor_count = static_cast<size_t>(kDescriptorSlotCount - subscriber_read_index);
                    MessageDescriptor desc_ring[descriptor_count];
                    copy(p->desc_ring + subscriber_read_index, p->desc_ring + kDescriptorSlotCount, desc_ring);

                    read_data(p, desc_ring, descriptor_count);
                    wr = p->descriptor_write_index.load(std::memory_order_acquire);
                }

                consume_contiguous_messages(p);
                cout << "read over" << endl;
            }
        }
        this_thread::sleep_for(chrono::milliseconds(1));
    }

    // write(1, p->data, sizeof(p->data));
    // cout << endl;

#ifdef ENABLE_DEBUG_CHECKS
    {
        pthread_mutex_lock(&p->chunk_usage_tracker.mutex);

        for (int i = 0; i < kClassCount; ++i) {
            for (int j = 0; j < kMaxChunkCountPerSizeClass; ++j) {
                if (p->chunk_usage_tracker.is_in_use[i][j] == true) {
                    cout << "class " << i << " the " << j << " chunk not free" << endl;
                }
            }
        }

        pthread_mutex_unlock(&p->chunk_usage_tracker.mutex);
        pthread_mutex_destroy(&p->chunk_usage_tracker.mutex);
    }
#endif

    munmap(p, sizeof(SharedData));
    shm_unlink("/shm");
    close(fd);

    return 0;
}