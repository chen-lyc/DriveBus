#include "include/logger.h"
#include "include/broker_protocol.hpp"
#include "include/shared_memory_layout.hpp"
#include "include/shared_memory_layout_helpers.hpp"
#include "include/fd_helpers.hpp"
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
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

std::filesystem::path publisher_log_path() {
    const char *run_id_env = std::getenv("DRIVEBUS_RUN_ID");
    const string run_id = run_id_env != nullptr && *run_id_env != '\0' ? run_id_env : "standalone";
    const auto start_us = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now().time_since_epoch()).count();

    return std::filesystem::path("logs") /
        run_id /
        "publisher" /
        ("pid-" + to_string(getpid()) + "-start-" + to_string(start_us) + ".log");
}

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
    int data_available_event_fd;
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
        LOG_FATAL("InitialSubscriberRegistrationTypeError: expected=%u actual=%u bytes=%zd broker_fd=%d",
            static_cast<unsigned>(BrokerMessageType::SubscriberEventFdAndSlot),
            static_cast<unsigned>(static_cast<unsigned char>(packet[0])),
            ret,
            broker_fd);
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

vector<int> data_available_event_fds{};

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
                    LOG_FATAL("PublisherChunkDoubleFreeError: class=%d index=%zu offset=%u",
                        size_class,
                        chunk_index,
                        offset);
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

std::unordered_map<int, size_t> subscriber_slot_index_by_data_available_event_fd{};
std::array<int, kMaxSubscribers> data_available_event_fd_by_subscriber_slot_index{};

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

    const int data_available_event_fd = data_available_event_fd_by_subscriber_slot_index[slot_index];
    subscriber_slot_index_by_data_available_event_fd.erase(data_available_event_fd);
    erase(data_available_event_fds, data_available_event_fd);
    close(data_available_event_fd);
}

void track_subscriber(SubscriberRegistration registration) {
    if (registration.slot_index >= kMaxSubscribers) {
        LOG_FATAL("PublisherSubscriberSlotRangeError: slot=%u max=%zu data_available_event_fd=%d",
            registration.slot_index,
            kMaxSubscribers,
            registration.data_available_event_fd);
        exit(1);
    }

    subscriber_slot_index_by_data_available_event_fd.insert({registration.data_available_event_fd, registration.slot_index});
    data_available_event_fd_by_subscriber_slot_index[registration.slot_index] = registration.data_available_event_fd;
    data_available_event_fds.emplace_back(registration.data_available_event_fd);

    p->descriptor_read_indices[registration.slot_index].store(p->descriptor_write_index.load(memory_order_relaxed), memory_order_relaxed);
}

int broker_fd;

void process_broker_control_messages() {
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
        msg.msg_flags = 0;

        const ssize_t received_size = recvmsg(broker_fd, &msg, 0);

        if (received_size == 0) {
            LOG_ERROR("PublisherBrokerClosed: broker_fd=%d", broker_fd);
            break;
        } else if (received_size < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            } else {
                LOG_ERROR("PublisherControlRecvError: broker_fd=%d errno=%d (%s)",
                    broker_fd,
                    errno,
                    strerror(errno));
                exit(1);
            }
        } else if (msg.msg_flags & MSG_CTRUNC) {
            LOG_FATAL("MsgTruncatedError");
            exit(1);
        }

        size_t body_len = received_size - sizeof(BrokerMessageType);
        BrokerMessageType message_type;
        memcpy(&message_type, packet, sizeof(BrokerMessageType));

        switch (message_type) {
            case BrokerMessageType::SubscriberDisconnected: {
                if (body_len != sizeof(uint32_t)) {
                    LOG_FATAL("SubscriberDisconnectPayloadSizeError: expected=%zu actual=%zu packet=%zd broker_fd=%d",
                        sizeof(uint32_t),
                        body_len,
                        received_size,
                        broker_fd);
                    exit(1);
                }

                uint32_t subscriber_slot_index;
                memcpy(&subscriber_slot_index, packet + 1, body_len);
                reap_dead_subscribers(subscriber_slot_index);
                break;
            }
            case BrokerMessageType::SubscriberEventFdAndSlot: {
                if (body_len != sizeof(uint32_t)) {
                    LOG_FATAL("SubscriberRegisteredPayloadSizeError: expected=%zu actual=%zu packet=%zd broker_fd=%d",
                        sizeof(uint32_t),
                        body_len,
                        received_size,
                        broker_fd);
                    exit(1);
                }

                uint32_t subscriber_slot_index;
                memcpy(&subscriber_slot_index, packet + 1, sizeof(subscriber_slot_index));

                struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);

                if (cmsg == nullptr) {
                    LOG_FATAL("SubscriberRegisteredEventFdMissingError: broker_fd=%d", broker_fd);
                    exit(1);
                }
                if (cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SCM_RIGHTS) {
                    LOG_FATAL("SubscriberRegisteredCmsgError: level=%d type=%d broker_fd=%d",
                        cmsg->cmsg_level,
                        cmsg->cmsg_type,
                        broker_fd);
                    exit(1);
                }

                const int data_available_event_fd = *reinterpret_cast<int *>(CMSG_DATA(cmsg));
                track_subscriber({data_available_event_fd, subscriber_slot_index});
                break;
            }
            default: {
                LOG_FATAL("UnexpectedBrokerMessageTypeError: type=%u bytes=%zd broker_fd=%d",
                    static_cast<unsigned>(static_cast<unsigned char>(packet[0])),
                    received_size,
                    broker_fd);
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

    Logger::init(publisher_log_path());

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
        LOG_FATAL("PublisherBrokerConnectError: attempts=%d errno=%d (%s)",
            fail_num,
            errno,
            strerror(errno));
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
            LOG_ERROR("InitialSubscriberCountReceiveError: broker_fd=%d errno=%d (%s)",
                broker_fd,
                errno,
                strerror(errno));
            return 1;
        }
        LOG_INFO("publisher registered: initial_subscriber_count=%u", subscriber_count);
    }

    for (size_t i = 0; i < subscriber_count; ++i) {
        auto registration = receive_subscriber_registration(broker_fd);
        if (!registration) {
            LOG_ERROR("ExistingSubscriberRegistrationReceiveError: index=%zu broker_fd=%d", i, broker_fd);
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
            LOG_FATAL("PublisherMessageSizeClassError: seq=%d size=%u", seq, message_size_bytes);
            return 1;
        }

        uint32_t wr = p->descriptor_write_index.load(memory_order_relaxed);
        while (min_descriptor_read_index == kInvalidIndex ||
            (min_descriptor_read_index > wr && min_descriptor_read_index <= wr + 1) ||
            wr + 1 >= min_descriptor_read_index + kDescriptorSlotCount) {
            this_thread::sleep_for(chrono::milliseconds(50));
            process_broker_control_messages();
            min_descriptor_read_index = find_slowest_read_index();
            LOG_DEBUG("publisher backpressure: min_rd=%u wr=%u rd0=%u rd1=%u",
                min_descriptor_read_index,
                wr,
                p->descriptor_read_indices[0].load(memory_order_relaxed),
                p->descriptor_read_indices[1].load(memory_order_relaxed));
        }
        LOG_DEBUG("publisher descriptor state: wr=%u min_rd=%u rd0=%u rd1=%u",
            wr,
            min_descriptor_read_index,
            p->descriptor_read_indices[0].load(memory_order_relaxed),
            p->descriptor_read_indices[1].load(memory_order_relaxed));

        uint32_t head_off = p->head.offset[size_class].load(memory_order_relaxed);
        uint32_t tail_off = p->tail.offset[size_class].load(std::memory_order_acquire);

        while (head_off == tail_off) {
            this_thread::sleep_for(chrono::milliseconds(50));
            process_broker_control_messages();
            tail_off = p->tail.offset[size_class].load(std::memory_order_acquire);
            LOG_DEBUG("publisher free-list empty: size_class=%d", size_class);
            // 单写端不需要更新 head_off
        }

#ifdef ENABLE_DEBUG_CHECKS
        {
            if (head_off < kFirstOffset[size_class] || head_off > kLastOffset[size_class]) {
                LOG_FATAL("PublisherChunkOffsetError: class=%d offset=%u", size_class, head_off);
                return -1;
            }

            pthread_mutex_lock(&p->chunk_usage_tracker.mutex);

            size_t chunk_index = (head_off - kFirstOffset[size_class]) / kClassSizeBytes[size_class];
            if (p->chunk_usage_tracker.is_in_use[size_class][chunk_index] == true) {
                LOG_FATAL("PublisherChunkAlreadyInUseError: class=%d index=%zu offset=%u",
                    size_class,
                    chunk_index,
                    head_off);
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

        process_broker_control_messages();

        uint32_t chunk_reference_mask = 0;
        for (size_t data_available_event_fd : data_available_event_fds) {
            auto it = subscriber_slot_index_by_data_available_event_fd.find(data_available_event_fd);
            if (it == subscriber_slot_index_by_data_available_event_fd.end()) {
                LOG_FATAL("PublisherEventFdSlotMapMissingError: data_available_event_fd=%d", data_available_event_fd);
                exit(1);
            }
            chunk_reference_mask += 1 << subscriber_slot_index_by_data_available_event_fd[data_available_event_fd];
        }
        LOG_DEBUG("publisher chunk_reference_mask=%u", chunk_reference_mask);
        p->chunk_reference_counts[chunk_index].fetch_add(chunk_reference_mask, memory_order_relaxed);

        uint32_t candidate = wr + 1;
        if (candidate >= kDescriptorSlotCount) p->descriptor_write_index.store(candidate % kDescriptorSlotCount, std::memory_order_release);
        else p->descriptor_write_index.store(candidate, std::memory_order_release);

        uint64_t val = 1;
        for (int data_available_event_fd : data_available_event_fds) {
            write(data_available_event_fd, &val, sizeof(val));
        }
        LOG_DEBUG("publisher wrote message: bytes=%u fill=%d seq=%d",
            message_size_bytes,
            static_cast<int>(c),
            seq - 1);
        --task_num;
        if (c == 'z') c = 'a' - 1;
    }
    uint64_t val = 1;
    for (int data_available_event_fd : data_available_event_fds) {
        write(data_available_event_fd, &val, sizeof(val));
    }
    LOG_INFO("publisher completed message production");

    munmap(p, sizeof(SharedData));
    close(shm_fd);
    shm_unlink(shm_name);

    return 0;
}
