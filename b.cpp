#include "include/broker_protocol.hpp"
#include "include/shared_memory_layout.hpp"
#include "include/shared_memory_layout_helpers.hpp"
#include "include/fd_helpers.hpp"
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <iostream>
#include <optional>
#include <random>
#include <thread>
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

struct SubscriberRegistration {
    int event_fd;
    uint32_t slot_index;
};

optional<SubscriberRegistration> receive_subscriber_registration(int broker_fd) {
    struct msghdr msg{};

    uint32_t received_subscriber_slot_index;
    struct iovec iov{};
    iov.iov_base = &received_subscriber_slot_index;
    iov.iov_len = sizeof(received_subscriber_slot_index);

    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;

    char control[CMSG_SPACE(sizeof(int))];
    memset(control, 0, sizeof(control));

    msg.msg_control = control;
    msg.msg_controllen = sizeof(control);

    ssize_t ret = recvmsg(broker_fd, &msg, 0);
    if (ret < 0) {
        cerr << "[b] Failed to receive subscriber registration: recvmsg: " << strerror(errno) << endl;
        return nullopt;
    }

    struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    if (cmsg == nullptr) {
        cerr << "[b] Failed to receive subscriber registration: no SCM_RIGHTS file descriptor in message" << endl;
        return nullopt;
    }
    if (cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SCM_RIGHTS) {
        cerr << "[b] Failed to receive subscriber registration: ancillary message is not SCM_RIGHTS" << endl;
        return nullopt;
    }

    int fd = *reinterpret_cast<int *>(CMSG_DATA(cmsg));
    return SubscriberRegistration{fd, received_subscriber_slot_index};
}

vector<int> event_fds{};

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

int broker_fd;

vector<uint32_t> drain_disconnected_subscriber_slots() {
    vector<uint32_t> slot_indices;
    while (true) {
        char packet[kSubscriberDisconnectedMessageSize];
        const ssize_t n = recv(broker_fd, &packet, kSubscriberDisconnectedMessageSize, 0);

        if (n == 0) {
            cerr << "[b] Failed to receive subscriber disconnect notification: broker closed the connection" << endl;
            exit(1);
        } else if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            } else {
                cerr << "[b] Failed to receive subscriber disconnect notification: " << strerror(errno) << endl;
                exit(1);
            }
        }

        if (n > 0) {
            BrokerMessageType type = static_cast<BrokerMessageType>(packet[0]);
            if (type != BrokerMessageType::SubscriberDisconnected) {
                cerr << "[b] Unexpected subscriber disconnect message type: " << static_cast<int>(packet[0]) << endl;
                exit(1);
            }

            size_t body_len = n - 1;
            if (body_len % sizeof(int) != 0) {
                cerr << "[b] Invalid subscriber disconnect payload length: " << body_len << endl;
                exit(1);
            }

            int slot_index;
            memcpy(&slot_index, packet + 1, body_len);
            slot_indices.emplace_back(slot_index);
        }
    }
    return slot_indices;
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

std::unordered_map<int, size_t> subscriber_slot_index_by_event_fd{};
std::array<int, kMaxSubscribers> event_fd_by_subscriber_slot_index{};

void reap_dead_subscribers(SharedData *p) {
    vector<uint32_t> slot_indices = drain_disconnected_subscriber_slots();

    for (uint32_t slot_index : slot_indices) {
        const int event_fd = event_fd_by_subscriber_slot_index[slot_index];
        subscriber_slot_index_by_event_fd.erase(event_fd);
        erase(event_fds, event_fd);

        uint8_t subscriber_reference_bit = 1 << slot_index;
        uint32_t rd = p->descriptor_read_indices[slot_index].load(std::memory_order_acquire);
        uint32_t wr = p->descriptor_write_index.load(std::memory_order_relaxed);
        if (rd > wr) {
            release_chunk_references_in_descriptor_range(p, rd, kDescriptorSlotCount, subscriber_reference_bit);
            rd = 0;
        }

        release_chunk_references_in_descriptor_range(p, rd, wr, subscriber_reference_bit);
        p->descriptor_read_indices[slot_index].store(kInvalidIndex, memory_order_relaxed);
    }
}

int main() {
    const string path = "/tmp/broker.sock";

    broker_fd = socket(AF_UNIX, SOCK_SEQPACKET, 0);

    struct sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, path.c_str());

    int ret = connect(broker_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
    int fail_num = 0;
    while (ret < 0 && fail_num < 3) {
        sleep(1);
        ++fail_num;
        ret = connect(broker_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
    }
    if (ret < 0) {
        cerr << "[b] Failed to connect to broker after " << fail_num << " attempts: " << strerror(errno) << endl;
        return -1;
    }

    BrokerMessageType role = BrokerMessageType::PublisherRegistration;
    send(broker_fd, &role, sizeof(role), 0);

    uint32_t subscriber_count;
    {
        ssize_t n = recv(broker_fd, &subscriber_count, sizeof(subscriber_count), 0);
        if (n < 0) {
            cerr << "[b] Failed to receive subscriber count: " << strerror(errno) << endl;
            return 1;
        }
        cout << "subscriber_count is " << subscriber_count << endl;
    }

    for (size_t i = 0; i < subscriber_count; ++i) {
        auto registration = receive_subscriber_registration(broker_fd);
        if (!registration) {
            cerr << "[b] Failed to receive registration for subscriber " << i << endl;
            return 1;
        }

        const int event_fd = registration->event_fd;
        const uint32_t subscriber_slot_index = registration->slot_index;
        subscriber_slot_index_by_event_fd.insert({event_fd, subscriber_slot_index});
        event_fd_by_subscriber_slot_index[subscriber_slot_index] = event_fd;
        event_fds.emplace_back(event_fd);

        cout << "event_fd is " << event_fd << ", subscriber_slot_index is " << subscriber_slot_index << endl;
    }

    set_fd_nonblocking(broker_fd);

    int fd = shm_open("/shm", O_RDWR, 0666);
    if (fd == -1) {
        cerr << "[b] Failed to open shared memory /shm" << endl;
        perror("shm_open");
        close(fd);
        return 1;
    }
    SharedData *p = static_cast<SharedData *>(mmap(nullptr, sizeof(SharedData), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));

    int task_num = 26 * 3000;
    int seq = 0;
    uint32_t min_descriptor_read_index = find_slowest_read_index(p);
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
            this_thread::sleep_for(chrono::milliseconds(50));
            reap_dead_subscribers(p);
            min_descriptor_read_index = find_slowest_read_index(p);
            wr = p->descriptor_write_index.load(memory_order_relaxed);
            cout << "min_read_index == wr: min_read_index is " << min_descriptor_read_index << "  idx " << 1 << " read_index is " << p->descriptor_read_indices[0].load(memory_order_relaxed) << "   idx " << 2 << " read_index is " << p->descriptor_read_indices[1].load(memory_order_relaxed) << endl;
        }
        cout << "wr is " << wr << endl;
        cout << "min_descriptor_read_index is " << min_descriptor_read_index << endl;
        cout << "idx " << 1 << " read_index is " << p->descriptor_read_indices[0].load(memory_order_relaxed) << endl;
        cout << "idx " << 2 << " read_index is " << p->descriptor_read_indices[1].load(memory_order_relaxed) << endl;

        uint32_t head_off = p->head.offset[size_class].load(memory_order_relaxed);
        uint32_t tail_off = p->tail.offset[size_class].load(std::memory_order_acquire);
        while (head_off == tail_off) {
            this_thread::sleep_for(chrono::milliseconds(50));
            reap_dead_subscribers(p);
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
        for (size_t event_fd : event_fds) {
            auto it = subscriber_slot_index_by_event_fd.find(event_fd);
            if (it == subscriber_slot_index_by_event_fd.end()) {
                cout << "error: invalid event_fd" << endl;
                exit(1);
            }
            chunk_reference_mask += 1 << subscriber_slot_index_by_event_fd[event_fd];
        }
        cout << "chunk_reference_mask is " << static_cast<int>(chunk_reference_mask) << endl;
        p->chunk_reference_counts[chunk_index].fetch_add(chunk_reference_mask, memory_order_relaxed); // 1 默认只是int,后续需要修改

        uint32_t candidate = wr + 1;
        if (candidate >= kDescriptorSlotCount) p->descriptor_write_index.store(candidate % kDescriptorSlotCount, std::memory_order_release);
        else p->descriptor_write_index.store(candidate, std::memory_order_release);

        uint64_t val = 1;
        for (int event_fd : event_fds) {
            write(event_fd, &val, sizeof(val));
        }
        cout << "write " << message_size_bytes << " byte " << c << ", seq is " << seq - 1 << endl;
        --task_num;
        if (c == 'z') c = 'a' - 1;
    }
    uint64_t val = 1;
    for (int event_fd : event_fds) {
        write(event_fd, &val, sizeof(val));
    }
    cout << "all write over" << endl;

    munmap(p, sizeof(SharedData));
    close(fd);

    return 0;
}
