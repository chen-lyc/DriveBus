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

    int fd;
    char packet[kSubscriberEventFdAndSlotMessageSize];
    ssize_t ret = receive_packet_with_fd(broker_fd, packet, sizeof(packet), fd);
    if (ret < 0) return nullopt;

    BrokerMessageType message_type = static_cast<BrokerMessageType>(packet[0]);
    if (message_type != BrokerMessageType::SubscriberEventFdAndSlot) {
        cerr << "InitialSubscriberRegistrationTypeError: expected="
                       << static_cast<unsigned>(BrokerMessageType::SubscriberEventFdAndSlot)
                       << ", actual=" << static_cast<unsigned>(static_cast<unsigned char>(packet[0]))
                       << ", bytes=" << ret << ", broker_fd=" << broker_fd << endl;
        exit(1);
    }
    uint32_t received_subscriber_slot_index;
    memcpy(&received_subscriber_slot_index, packet + 1, sizeof(received_subscriber_slot_index));

    return SubscriberRegistration{fd, received_subscriber_slot_index};
}

void init_chunk_usage_tracker(ChunkUsageTracker &tracker) {
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
    pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
    pthread_mutex_init(&tracker.mutex, &attr);
    pthread_mutexattr_destroy(&attr);

    memset(tracker.is_in_use, 0, sizeof(tracker.is_in_use));
}

void init_shm(SharedData &shm) {
    for (int i = 0; i < kMaxSubscribers; ++i) {
        shm.descriptor_read_indices[i].store(kInvalidIndex, std::memory_order_relaxed);
    }
    shm.descriptor_write_index.store(0, std::memory_order_relaxed);

    shm.head.offset[0].store(0, std::memory_order_relaxed);
    for (int i = 1; i < kClassCount; ++i) {
        shm.head.offset[i].store(shm.head.offset[i - 1].load(std::memory_order_relaxed) + kClassSizeBytes[i - 1] * kChunkCountBySizeClass[i - 1], std::memory_order_relaxed);
    }

    for (int i = 0; i < kClassCount - 1; ++i) {
        shm.tail.offset[i].store(shm.head.offset[i + 1].load(std::memory_order_relaxed) - kClassSizeBytes[i], std::memory_order_relaxed);
    }
    shm.tail.offset[kClassCount - 1].store(sizeof(shm.data) - kClassSizeBytes[kClassCount - 1], std::memory_order_relaxed);

    for (int i = 0; i < kTotalChunkCount; ++i) {
        shm.chunk_reference_counts[i].store(0, std::memory_order_relaxed);
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

vector<int> event_fds{};

constexpr size_t kMaxEvents = 1024;

SharedData *p;

uint32_t find_slowest_read_index() {
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

void release_chunk_references_in_descriptor_range(size_t start, size_t end, uint32_t subscriber_reference_bit) {
    while (start < end) {
        uint32_t offset = p->desc_ring[start].offset;
        uint32_t len = p->desc_ring[start].len;
        int size_class = find_size_class(len);

        uint32_t local_chunk_index = (offset - kFirstOffset[size_class]) / kClassSizeBytes[size_class];
        uint32_t chunk_index = kChunkIndexBaseBySizeClass[size_class] + local_chunk_index;
        uint32_t previous_reference_count = p->chunk_reference_counts[chunk_index].fetch_and(~subscriber_reference_bit, std::memory_order_acq_rel);

        if (previous_reference_count == subscriber_reference_bit) {
#ifdef ENABLE_DEBUG_CHECKS
            {
                pthread_mutex_lock(&p->chunk_usage_tracker.mutex);

                size_t chunk_index = (offset - kFirstOffset[size_class]) / kClassSizeBytes[size_class];
                // cout << "the " << chunk_index << " chunk free" << endl;
                if (p->chunk_usage_tracker.is_in_use[size_class][chunk_index] == false) {
                    cerr << "PublisherChunkDoubleFreeError: class=" << size_class
                                   << ", index=" << chunk_index << ", offset=" << offset << endl;
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

void reap_dead_subscribers(uint32_t slot_index) {
    uint32_t subscriber_reference_bit = 1 << slot_index;
    uint32_t rd = p->descriptor_read_indices[slot_index].load(std::memory_order_acquire);
    uint32_t wr = p->descriptor_write_index.load(std::memory_order_relaxed);
    if (rd > wr) {
        release_chunk_references_in_descriptor_range(rd, kDescriptorSlotCount, subscriber_reference_bit);
        rd = 0;
    }

    release_chunk_references_in_descriptor_range(rd, wr, subscriber_reference_bit);
    p->descriptor_read_indices[slot_index].store(kInvalidIndex, memory_order_relaxed);

    const int event_fd = event_fd_by_subscriber_slot_index[slot_index];
    subscriber_slot_index_by_event_fd.erase(event_fd);
    erase(event_fds, event_fd);
    close(event_fd);
}

void track_subscriber(SubscriberRegistration registration) {
    if (registration.slot_index >= kMaxSubscribers) {
        cerr << "PublisherSubscriberSlotRangeError: slot=" << registration.slot_index
                       << ", max=" << kMaxSubscribers << ", event_fd=" << registration.event_fd << endl;
        exit(1);
    }

    subscriber_slot_index_by_event_fd.insert({registration.event_fd, registration.slot_index});
    event_fd_by_subscriber_slot_index[registration.slot_index] = registration.event_fd;
    event_fds.emplace_back(registration.event_fd);

    p->descriptor_read_indices[registration.slot_index].store(p->descriptor_write_index.load(memory_order_relaxed), memory_order_relaxed);
}

int broker_fd;

void process_broker_contorl_messages() {
    char packet[kMaxMessageSize];
    char control[CMSG_SPACE(sizeof(int))];

    struct iovec iov{};
    iov.iov_base = packet;
    iov.iov_len = kMaxMessageSize;

    struct msghdr msg{};
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control;

    while (true) {
        msg.msg_controllen = sizeof(control);

        const ssize_t received_size = recvmsg(broker_fd, &msg, 0);

        if (received_size == 0) {
            cerr << "PublisherBrokerClosed: broker_fd=" << broker_fd << endl;
            break;
        } else if (received_size < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            } else {
                cerr << "PublisherControlRecvError: broker_fd=" << broker_fd
                               << ", errno=" << errno << " (" << strerror(errno) << ')' << endl;
                exit(1);
            }
        }

        size_t body_len = received_size - 1;
        auto message_type = static_cast<BrokerMessageType>(packet[0]);

        switch (message_type) {
            case BrokerMessageType::SubscriberDisconnected: {
                if (body_len != sizeof(uint32_t)) {
                    cerr << "SubscriberDisconnectPayloadSizeError: expected=" << sizeof(uint32_t)
                                   << ", actual=" << body_len << ", packet=" << received_size
                                   << ", broker_fd=" << broker_fd << endl;
                    exit(1);
                }

                uint32_t subscriber_slot_index;
                memcpy(&subscriber_slot_index, packet + 1, body_len);
                reap_dead_subscribers(subscriber_slot_index);
                break;
            }
            case BrokerMessageType::SubscriberEventFdAndSlot: {
                if (body_len != sizeof(uint32_t)) {
                    cerr << "SubscriberRegisteredPayloadSizeError: expected=" << sizeof(uint32_t)
                                   << ", actual=" << body_len << ", packet=" << received_size
                                   << ", broker_fd=" << broker_fd << endl;
                    exit(1);
                }

                uint32_t subscriber_slot_index;
                memcpy(&subscriber_slot_index, packet + 1, sizeof(subscriber_slot_index));

                struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);

                if (cmsg == nullptr) {
                    cerr << "SubscriberRegisteredEventFdMissingError: broker_fd=" << broker_fd << endl;
                    exit(1);
                }
                if (cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SCM_RIGHTS) {
                    cerr << "SubscriberRegisteredCmsgError: level=" << cmsg->cmsg_level
                                   << ", type=" << cmsg->cmsg_type
                                   << ", broker_fd=" << broker_fd << endl;
                    exit(1);
                }

                const int event_fd = *reinterpret_cast<int *>(CMSG_DATA(cmsg));
                track_subscriber({event_fd, subscriber_slot_index});
                break;
            }
            default: {
                cerr << "UnexpectedBrokerMessageTypeError: type="
                               << static_cast<unsigned>(static_cast<unsigned char>(packet[0]))
                               << ", bytes=" << received_size << ", broker_fd=" << broker_fd << endl;
                exit(1);
            }
        }
    }
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        cerr << "PublisherArgumentsError: usage=" << argv[0]
                       << " <topic_id> <shm_name>" << endl;
        return 1;
    }
    
    TopicId topic_id = static_cast<TopicId>(stoul(argv[1]));
    const char *shm_name = argv[2];
    shm_unlink(shm_name);

    int shm_fd = shm_open(shm_name, O_CREAT | O_RDWR, 0666);
    ftruncate(shm_fd, sizeof(SharedData));
    p = reinterpret_cast<SharedData *>(mmap(nullptr, sizeof(SharedData), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0));

    init_shm(*p);
#ifdef ENABLE_DEBUG_CHECKS
    init_chunk_usage_tracker(p->chunk_usage_tracker);
#endif

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
        cerr << "PublisherBrokerConnectError: attempts=" << fail_num
                       << ", errno=" << errno << " (" << strerror(errno) << ')' << endl;
        return -1;
    }

    char packet[kPublisherTopicRegistrationMessageSize];
    packet[0] = static_cast<char>(BrokerMessageType::PublisherTopicRegistration);
    
    memcpy(packet + sizeof(BrokerMessageType) / sizeof(char), &topic_id, sizeof(topic_id));

    send_packet_with_fd(broker_fd, packet, sizeof(packet), shm_fd);

    uint32_t subscriber_count;
    {
        ssize_t n = recv(broker_fd, &subscriber_count, sizeof(subscriber_count), 0);
        if (n < 0) {
            cerr << "InitialSubscriberCountReceiveError: broker_fd=" << broker_fd
                           << ", errno=" << errno << " (" << strerror(errno) << ')' << endl;
            return 1;
        }
        cout << "subscriber_count is " << subscriber_count << endl;
    }

    for (size_t i = 0; i < subscriber_count; ++i) {
        auto registration = receive_subscriber_registration(broker_fd);
        if (!registration) {
            cerr << "ExistingSubscriberRegistrationReceiveError: index=" << i
                           << ", broker_fd=" << broker_fd << endl;
            return 1;
        }

        track_subscriber(registration.value());
    }

    set_fd_nonblocking(broker_fd);

    int task_num = 26 * 3000;
    int seq = 0;
    uint32_t min_descriptor_read_index = find_slowest_read_index();
    for (char c = 'a'; task_num; ++c) {
        uint32_t message_size_bytes = get_msg_size();
        int size_class = find_size_class(message_size_bytes);
        if (size_class < 0) {
            cerr << "PublisherMessageSizeClassError: seq=" << seq
                           << ", size=" << message_size_bytes << endl;
            return 1;
        }

        uint32_t wr = p->descriptor_write_index.load(memory_order_relaxed);
        while (min_descriptor_read_index == kInvalidIndex ||
            (min_descriptor_read_index > wr && min_descriptor_read_index <= wr + 1) ||
            wr + 1 >= min_descriptor_read_index + kDescriptorSlotCount) {
            this_thread::sleep_for(chrono::milliseconds(50));
            process_broker_contorl_messages();
            min_descriptor_read_index = find_slowest_read_index();
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
            process_broker_contorl_messages();
            tail_off = p->tail.offset[size_class].load(std::memory_order_acquire);
            cout << "no node free" << endl;
            // 单写端不需要更新 head_off
        }

#ifdef ENABLE_DEBUG_CHECKS
        {
            if (head_off < kFirstOffset[size_class] || head_off > kLastOffset[size_class]) {
                cerr << "PublisherChunkOffsetError: class=" << size_class
                               << ", offset=" << head_off << endl;
                return -1;
            }

            pthread_mutex_lock(&p->chunk_usage_tracker.mutex);

            size_t chunk_index = (head_off - kFirstOffset[size_class]) / kClassSizeBytes[size_class];
            if (p->chunk_usage_tracker.is_in_use[size_class][chunk_index] == true) {
                cerr << "PublisherChunkAlreadyInUseError: class=" << size_class
                               << ", index=" << chunk_index << ", offset=" << head_off << endl;
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

        process_broker_contorl_messages();

        uint32_t chunk_reference_mask = 0;
        for (size_t event_fd : event_fds) {
            auto it = subscriber_slot_index_by_event_fd.find(event_fd);
            if (it == subscriber_slot_index_by_event_fd.end()) {
                cerr << "PublisherEventFdSlotMapMissingError: event_fd=" << event_fd << endl;
                exit(1);
            }
            chunk_reference_mask += 1 << subscriber_slot_index_by_event_fd[event_fd];
        }
        cout << "chunk_reference_mask is " << static_cast<int>(chunk_reference_mask) << endl;
        p->chunk_reference_counts[chunk_index].fetch_add(chunk_reference_mask, memory_order_relaxed);

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
    close(shm_fd);

    return 0;
}
