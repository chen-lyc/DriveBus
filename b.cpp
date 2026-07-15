#include "include/logger.h"
#include "include/shared_memory_layout.hpp"
#include "include/shared_memory_layout_helpers.hpp"
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

uint32_t get_msg_size() {
    random_device rd;
    mt19937 gen(rd());

    const uint32_t K = 1;
    const uint32_t M = 1;

    uniform_int_distribution<uint32_t> choose(0, 1);

    uniform_int_distribution<uint32_t> small_dist(8 * K, 16 * K);
    uniform_int_distribution<uint32_t> big_dist(128 * M, 1024 * M);

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

void Chunk_usage_tracker_init(ChunkUsageTracker &tracker) {
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
    pthread_mutex_init(&tracker.mutex, &attr);
    pthread_mutexattr_destroy(&attr);

    memset(tracker.is_in_use, 0, sizeof(tracker.is_in_use));
}

void shm_init(SharedData &shm) {
    for (int i = 0; i < kMaxSubscribers; ++i) {
        shm.descriptor_read_indexs[i].store(kInvalidIndex, memory_order_relaxed);
    }
    shm.descriptor_write_index.store(0, memory_order_relaxed);

    shm.head.offset[0].store(0, memory_order_relaxed);
    for (int i = 1; i < kClassCount; ++i) {
        shm.head.offset[i].store(shm.head.offset[i - 1].load(memory_order_relaxed) + kClassSizeBytes[i - 1] * kChunkCountBySizeClass[i - 1], memory_order_relaxed);
    }

    for (int i = 0; i < kClassCount - 1; ++i) {
        shm.tail.offset[i].store(shm.head.offset[i + 1].load(memory_order_relaxed) - kClassSizeBytes[i], memory_order_relaxed);
    }
    shm.tail.offset[kClassCount - 1].store(sizeof(shm.data) - kClassSizeBytes[kClassCount - 1], memory_order_relaxed);

    for (int i = 0; i < kTotalChunkCount; ++i) {
        shm.chunk_reference_counts[i].store(0, memory_order_relaxed);
    }

    auto init = [&](uint32_t chunk_size_bytes, const uint32_t chunk_count, uint32_t offset) {
        for (uint32_t i = 0; i < chunk_count - 1; ++i) {
            uint32_t next_offset = offset + chunk_size_bytes;
            memcpy(shm.data + offset, &next_offset, sizeof(uint32_t));
            offset = next_offset;
        }
        memcpy(shm.data + offset, &kInvalidOffset, sizeof(uint32_t));
    };
    for (int i = 0; i < kClassCount; ++i) {
        init(kClassSizeBytes[i], kChunkCountBySizeClass[i], shm.head.offset[i]);
    }
}

constexpr size_t kMaxEvents = 1024;

uint32_t find_slowest_read_index(const SharedData *p) {
    uint32_t min_read_index_before_write = kInvalidIndex;
    uint32_t min_read_index_after_write = kInvalidIndex;
    uint32_t descriptor_write_index = p->descriptor_write_index.load(memory_order_relaxed);
    for (size_t subsrciber_index = 0; subsrciber_index < kMaxSubscribers; ++subsrciber_index) {
        uint32_t descriptor_read_index = p->descriptor_read_indexs[subsrciber_index].load(std::memory_order_acquire);
        if (descriptor_read_index == kInvalidIndex) continue;

        if (descriptor_read_index <= descriptor_write_index)
            min_read_index_after_write = min(min_read_index_after_write, descriptor_read_index);
        else
            min_read_index_before_write = min(min_read_index_before_write, descriptor_read_index);
    }

    if (min_read_index_before_write != std::numeric_limits<uint32_t>::max())
        return min_read_index_before_write;

    return min_read_index_after_write;
}

int main() {
    shm_unlink("/shm");
    int fd = shm_open("/shm", O_CREAT | O_RDWR, 0666);
    ftruncate(fd, sizeof(SharedData));
    SharedData *p = static_cast<SharedData *>(mmap(nullptr, sizeof(SharedData), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));

    shm_init(*p);
#ifdef ENABLE_DEBUG_CHECKS
    Chunk_usage_tracker_init(p->chunk_usage_tracker);
#endif

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

    int conn_fd1 = accept(listen_fd, nullptr, nullptr);
    int conn_fd2 = accept(listen_fd, nullptr, nullptr);

    int efd1 = eventfd(0, EFD_NONBLOCK);
    send_fd(conn_fd1, efd1);
    int efd2 = eventfd(0, EFD_NONBLOCK);
    send_fd(conn_fd2, efd2);

    int task_num = 26 * 3000;
    int seq = 0;
    uint32_t min_descriptor_read_index = kInvalidIndex;
    for (char c = 'a'; task_num; ++c) {
        uint32_t message_size_bytes = get_msg_size();
        int size_class = find_size_class(message_size_bytes);
        if (size_class < 0) {
            cout << "seq is " << seq << " size is " << message_size_bytes << ": size is too big" << endl;
            return 1;
        }

        uint32_t wr = p->descriptor_write_index.load(memory_order_relaxed);
        while (min_descriptor_read_index == kInvalidIndex ||
            (min_descriptor_read_index > wr && min_descriptor_read_index <= wr + 1) ||
            wr + 1 >= min_descriptor_read_index + kDescriptorSlotCount) {
            this_thread::sleep_for(chrono::milliseconds(1));
            min_descriptor_read_index = find_slowest_read_index(p);
            wr = p->descriptor_write_index.load(memory_order_relaxed);
        }
        cout << "min_descriptor_read_index is " << min_descriptor_read_index << endl;
        cout << "idx " << 1 << " read_index is " << p->descriptor_read_indexs[0].load(memory_order_relaxed) << endl;
        cout << "idx " << 2 << " read_index is " << p->descriptor_read_indexs[1].load(memory_order_relaxed) << endl;

        uint32_t head_off = p->head.offset[size_class].load(memory_order_relaxed);
        uint32_t tail_off = p->tail.offset[size_class].load(std::memory_order_acquire);
        while (head_off == tail_off) {
            this_thread::sleep_for(chrono::milliseconds(1));
            tail_off = p->tail.offset[size_class].load(std::memory_order_acquire);
        }

#ifdef ENABLE_DEBUG_CHECKS
        {
            if (head_off < kFirstOffset[size_class] || head_off > kLastOffset[size_class]) {
                cout << "size_class " << size_class << " offset is out of range: head_off is " << head_off << endl;
                return -1;
            }

            pthread_mutex_lock(&p->chunk_usage_tracker.mutex);

            size_t chunk_index = (head_off - kFirstOffset[size_class]) / kClassSizeBytes[size_class];
            if (p->chunk_usage_tracker.is_in_use[size_class][chunk_index] == true) {
                cout << "size_class " << size_class << " the " << chunk_index << " chunk error use again" << endl;
                pthread_mutex_unlock(&p->chunk_usage_tracker.mutex);
                return 1;
            } else {
                // cout << "size_class " << size_class <<"the " << idx << " chunk using" << endl;
                p->chunk_usage_tracker.is_in_use[size_class][chunk_index] = true;
            }

            pthread_mutex_unlock(&p->chunk_usage_tracker.mutex);
        }
#endif

        uint32_t next_head_off;
        memcpy(&next_head_off, p->data + head_off, sizeof(uint32_t));
        p->head.offset[size_class].store(next_head_off, memory_order_relaxed);

        MessageDescriptor desc{head_off, message_size_bytes};
        memcpy(p->desc_ring + wr, &desc, sizeof(MessageDescriptor));

        memcpy(p->data + head_off, &kMagic, sizeof(int));
        memcpy(p->data + head_off + sizeof(int), &seq, sizeof(int));
        ++seq;
        if (message_size_bytes > 2 * sizeof(int)) {
            memset(p->data + head_off + 2 * sizeof(int), c, message_size_bytes - 2 * sizeof(int));
        }

        uint32_t local_chunk_index = (head_off - kFirstOffset[size_class]) / kClassSizeBytes[size_class];
        uint32_t chunk_index = kChunkIndexBaseBySizeClass[size_class] + local_chunk_index;
        p->chunk_reference_counts[chunk_index].fetch_add(2, memory_order_relaxed);

        uint32_t candidate = wr + 1;
        if (candidate >= kDescriptorSlotCount) p->descriptor_write_index.store(candidate % kDescriptorSlotCount, std::memory_order_release);
        else p->descriptor_write_index.store(candidate, std::memory_order_release);
        cout << "candidate is " << candidate << endl;

        uint64_t val = 1;
        write(efd1, &val, sizeof(val));
        write(efd2, &val, sizeof(val));
        cout << "write " << message_size_bytes << " byte " << c << ", seq is " << seq - 1 << endl;
        --task_num;
        if (c == 'z') c = 'a' - 1;
    }
    uint64_t val = 1;
    write(efd1, &val, sizeof(val));
    write(efd2, &val, sizeof(val));
    cout << "all write over" << endl;

    munmap(p, sizeof(SharedData));
    close(fd);

    return 0;
}