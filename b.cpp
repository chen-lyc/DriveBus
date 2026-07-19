#include "include/logger.h"
#include "include/shared_memory_layout.hpp"
#include "include/shared_memory_layout_helpers.hpp"
#include <algorithm>
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
#include <vector>
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

void send_fd(int sock, int fd, uint32_t send_subscriber_slot_index) {
    struct msghdr msg{};

    struct iovec iov;
    iov.iov_base = &send_subscriber_slot_index;
    iov.iov_len = sizeof(send_subscriber_slot_index);

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

bool is_peer_alive(int conn_fd) {
    char byte;
    while (true) {
        const ssize_t n = recv(conn_fd, &byte, sizeof(byte), MSG_PEEK | MSG_DONTWAIT);

        if (n == 0) {
            return false;
        }

        if (n > 0) {
            return true;
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return true;
        }

        if (errno == EINTR) {
            continue;
        }
    }
}

void release_chunk_references_in_descriptor_range(SharedData *p, size_t start, size_t end, uint8_t subscriber_reference_bit) {
    while (start < end) {
        uint32_t offset = p->desc_ring[start].offset;
        uint32_t len = p->desc_ring[start].len;
        int size_class = find_size_class(len);

        uint32_t local_chunk_index = (offset - kFirstOffset[size_class]) / kClassSizeBytes[size_class];
        uint32_t chunk_index = kChunkIndexBaseBySizeClass[size_class] + local_chunk_index;
        uint8_t previous_reference_count = p->chunk_reference_counts[chunk_index].fetch_and(~subscriber_reference_bit, std::memory_order_acq_rel);

        if (previous_reference_count == subscriber_reference_bit) {
#ifdef ENABLE_DEBUG_CHECKS
            {
                pthread_mutex_lock(&p->chunk_usage_tracker.mutex);

                size_t chunk_index = (offset - kFirstOffset[size_class]) / kClassSizeBytes[size_class];
                // cout << "the " << chunk_index << " chunk free" << endl;
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
        ++start;
    }
}

size_t subscriber_slot_index = 0;
unordered_map<int, size_t> subscriber_slot_index_by_conn_fd{};

vector<int> conn_fds{};

void reap_dead_subscribers(SharedData *p, uint32_t wr) {
    vector<size_t> dead_subscriber_slot_indices;
    conn_fds.erase(remove_if(conn_fds.begin(), conn_fds.end(), [&dead_subscriber_slot_indices](int conn_fd) {
        if (is_peer_alive(conn_fd)) {
            return false;
        }

        dead_subscriber_slot_indices.emplace_back(subscriber_slot_index_by_conn_fd[conn_fd]);
        subscriber_slot_index_by_conn_fd.erase(conn_fd);
        close(conn_fd);
        return true;
    }),
        conn_fds.end());

    for (size_t subscriber_slot_index : dead_subscriber_slot_indices) {
        uint32_t rd = p->descriptor_read_indices[subscriber_slot_index].load(std::memory_order_acquire);
        uint8_t subscriber_reference_bit = 1 << subscriber_slot_index;
        if (rd > wr) {
            release_chunk_references_in_descriptor_range(p, rd, kDescriptorSlotCount, subscriber_reference_bit);
            rd = 0;
        }

        release_chunk_references_in_descriptor_range(p, rd, wr, subscriber_reference_bit);
        p->descriptor_read_indices[subscriber_slot_index].store(kInvalidIndex, memory_order_relaxed);
    }
}

void chunk_usage_tracker_init(ChunkUsageTracker &tracker) {
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
    pthread_mutex_init(&tracker.mutex, &attr);
    pthread_mutexattr_destroy(&attr);

    memset(tracker.is_in_use, 0, sizeof(tracker.is_in_use));
}

void shm_init(SharedData &shm) {
    for (int i = 0; i < kMaxSubscribers; ++i) {
        shm.descriptor_read_indices[i].store(kInvalidIndex, memory_order_relaxed);
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
        uint32_t descriptor_read_index = p->descriptor_read_indices[subsrciber_index].load(std::memory_order_acquire);
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
    chunk_usage_tracker_init(p->chunk_usage_tracker);
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
    conn_fds.emplace_back(conn_fd1);
    int conn_fd2 = accept(listen_fd, nullptr, nullptr);
    conn_fds.emplace_back(conn_fd2);

    int efd1 = eventfd(0, EFD_NONBLOCK);
    send_fd(conn_fd1, efd1, subscriber_slot_index);
    subscriber_slot_index_by_conn_fd.insert({conn_fd1, subscriber_slot_index});
    ++subscriber_slot_index;
    int efd2 = eventfd(0, EFD_NONBLOCK);
    send_fd(conn_fd2, efd2, subscriber_slot_index);
    subscriber_slot_index_by_conn_fd.insert({conn_fd2, subscriber_slot_index});
    ++subscriber_slot_index;

    int task_num = 26 * 30000;
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
            reap_dead_subscribers(p, wr);
            min_descriptor_read_index = find_slowest_read_index(p);
            wr = p->descriptor_write_index.load(memory_order_relaxed);
            // cout << "min_read_index == wr" << endl;
        }
        cout << "min_descriptor_read_index is " << min_descriptor_read_index << endl;
        cout << "idx " << 1 << " read_index is " << p->descriptor_read_indices[0].load(memory_order_relaxed) << endl;
        cout << "idx " << 2 << " read_index is " << p->descriptor_read_indices[1].load(memory_order_relaxed) << endl;

        uint32_t head_off = p->head.offset[size_class].load(memory_order_relaxed);
        uint32_t tail_off = p->tail.offset[size_class].load(std::memory_order_acquire);
        while (head_off == tail_off) {
            reap_dead_subscribers(p, wr);
            tail_off = p->tail.offset[size_class].load(std::memory_order_acquire);
            cout << "no node free" << endl;
            // 单写端不需要更新 head_off
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
        p->head.offset[size_class].store(next_head_off, memory_order_relaxed); // 单写端 relaxed

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

        uint8_t chunk_reference_mask = 0;
        for (size_t conn_fd : conn_fds) {
            auto it = subscriber_slot_index_by_conn_fd.find(conn_fd);
            if (it == subscriber_slot_index_by_conn_fd.end()) {
                cout << "error: invalid conn_fd" << endl;
                exit(1);
            }
            chunk_reference_mask += 1 << subscriber_slot_index_by_conn_fd[conn_fd];
        }
        p->chunk_reference_counts[chunk_index].fetch_add(chunk_reference_mask, memory_order_relaxed); // 1 默认只是int,后续需要修改

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