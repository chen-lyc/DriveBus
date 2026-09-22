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

constexpr size_t kMaxEvents = 1024;

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
    int fd;
    char packet[kSubscriberEventFdAndSlotMessageSize];
    ssize_t ret = receive_packet_with_fd(broker_fd, packet, sizeof(packet), fd);
    if (ret != static_cast<ssize_t>(sizeof(packet))) {
        if (ret < 0) {
            LOG_ERROR("InitialSubscriberRegistrationReceiveError: broker_fd=%d errno=%d (%s)",
                broker_fd,
                errno,
                strerror(errno));
        } else {
            LOG_ERROR("InitialSubscriberRegistrationReceiveSizeError: broker_fd=%d expected=%zu actual=%zd",
                broker_fd,
                sizeof(packet),
                ret);
        }
        return nullopt;
    }
    if (fd < 0) {
        LOG_FATAL("InitialSubscriberRegistrationEventFdMissingError: broker_fd=%d", broker_fd);
        return nullopt;
    }

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
    if (slot_index >= kMaxSubscribers) {
        LOG_FATAL("PublisherDeadSubscriberSlotRangeError: slot=%u max=%zu",
            slot_index,
            kMaxSubscribers);
        exit(1);
    }

    uint32_t subscriber_reference_bit = 1 << slot_index;
    uint32_t rd = p->descriptor_read_indices[slot_index].load(std::memory_order_acquire);
    if (rd == kInvalidIndex) {
        LOG_FATAL("PublisherDeadSubscriberNotTrackedError: slot=%u", slot_index);
        exit(1);
    }
    uint32_t wr = p->descriptor_write_index.load(std::memory_order_relaxed);
    if (rd > wr) {
        release_chunk_references_in_descriptor_range(rd, kDescriptorSlotCount, subscriber_reference_bit);
        rd = 0;
    }

    release_chunk_references_in_descriptor_range(rd, wr, subscriber_reference_bit);
    p->descriptor_read_indices[slot_index].store(kInvalidIndex, memory_order_relaxed);

    const int data_available_event_fd = data_available_event_fd_by_subscriber_slot_index[slot_index];
    auto data_available_event_fd_it =
        subscriber_slot_index_by_data_available_event_fd.find(data_available_event_fd);
    if (data_available_event_fd_it == subscriber_slot_index_by_data_available_event_fd.end() ||
        data_available_event_fd_it->second != slot_index) {
        LOG_FATAL("PublisherDeadSubscriberEventFdMissingError: slot=%u data_available_event_fd=%d",
            slot_index,
            data_available_event_fd);
        exit(1);
    }
    subscriber_slot_index_by_data_available_event_fd.erase(data_available_event_fd_it);
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
    if (p->descriptor_read_indices[registration.slot_index].load(memory_order_relaxed) != kInvalidIndex) {
        LOG_FATAL("PublisherSubscriberSlotAlreadyTrackedError: slot=%u data_available_event_fd=%d",
            registration.slot_index,
            registration.data_available_event_fd);
        exit(1);
    }

    const bool data_available_event_fd_inserted =
        subscriber_slot_index_by_data_available_event_fd
            .insert({registration.data_available_event_fd, registration.slot_index})
            .second;
    if (!data_available_event_fd_inserted) {
        LOG_FATAL("PublisherDataAvailableEventFdAlreadyTrackedError: event_fd=%d slot=%u",
            registration.data_available_event_fd,
            registration.slot_index);
        exit(1);
    }
    data_available_event_fd_by_subscriber_slot_index[registration.slot_index] = registration.data_available_event_fd;
    data_available_event_fds.emplace_back(registration.data_available_event_fd);

    p->descriptor_read_indices[registration.slot_index].store(p->descriptor_write_index.load(memory_order_relaxed), memory_order_relaxed);
}

int broker_fd = -1;
int epoll_fd = -1;

void process_broker_control_messages() {
    static char packet[kMaxMessageSize];
    static char control[CMSG_SPACE(sizeof(int))];

    static iovec iov{};
    iov.iov_base = packet;
    iov.iov_len = kMaxMessageSize;

    static msghdr msg{};
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control;

    while (true) {
        msg.msg_controllen = sizeof(control);
        msg.msg_flags = 0;

        const ssize_t received_size = recvmsg(broker_fd, &msg, 0);

        if (received_size == 0) {
            LOG_ERROR("PublisherBrokerClosed: broker_fd=%d", broker_fd);
            epoll_ctl(epoll_fd, EPOLL_CTL_DEL, broker_fd, nullptr);
            close(broker_fd);
            broker_fd = -1;

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
                if (cmsg->cmsg_level != SOL_SOCKET ||
                    cmsg->cmsg_type != SCM_RIGHTS ||
                    cmsg->cmsg_len != CMSG_LEN(sizeof(int))) {
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

enum class PublishAttempt {
    Succeeded,
    Blocked,
    Failed,
};

int task_num = 26 * 3000;
int seq = 0;

uint32_t next_message_size_bytes = get_msg_size();

bool can_publish_next_message() {
    uint32_t min_descriptor_read_index = find_slowest_read_index();

    uint32_t message_size_bytes = next_message_size_bytes;
    int size_class = find_size_class(message_size_bytes);
    if (size_class < 0) {
        LOG_FATAL("PublisherMessageSizeClassError: seq=%d size=%u", seq, message_size_bytes);
        return false;
    }

    uint32_t wr = p->descriptor_write_index.load(memory_order_relaxed);
    if (min_descriptor_read_index == kInvalidIndex ||
        (min_descriptor_read_index > wr && min_descriptor_read_index <= wr + 1) ||
        wr + 1 >= min_descriptor_read_index + kDescriptorSlotCount) {
        LOG_DEBUG("publisher backpressure: min_rd=%u wr=%u rd0=%u rd1=%u",
            min_descriptor_read_index,
            wr,
            p->descriptor_read_indices[0].load(memory_order_relaxed),
            p->descriptor_read_indices[1].load(memory_order_relaxed));

        return false;
    }

    LOG_DEBUG("publisher descriptor state: wr=%u min_rd=%u rd0=%u rd1=%u",
        wr,
        min_descriptor_read_index,
        p->descriptor_read_indices[0].load(memory_order_relaxed),
        p->descriptor_read_indices[1].load(memory_order_relaxed));

    uint32_t head_off = p->head.offset[size_class].load(memory_order_relaxed);
    uint32_t tail_off = p->tail.offset[size_class].load(std::memory_order_acquire);

    if (head_off == tail_off) {
        LOG_DEBUG("publisher free-list empty: size_class=%d", size_class);
        return false;
    }

    return true;
}

PublishAttempt try_publish_one() {
    uint32_t min_descriptor_read_index = find_slowest_read_index();

    static char c = 'a';

    uint32_t message_size_bytes = next_message_size_bytes;
    int size_class = find_size_class(message_size_bytes);
    if (size_class < 0) {
        LOG_FATAL("PublisherMessageSizeClassError: seq=%d size=%u", seq, message_size_bytes);
        return PublishAttempt::Failed;
    }

    uint32_t wr = p->descriptor_write_index.load(memory_order_relaxed);
    if (min_descriptor_read_index == kInvalidIndex ||
        (min_descriptor_read_index > wr && min_descriptor_read_index <= wr + 1) ||
        wr + 1 >= min_descriptor_read_index + kDescriptorSlotCount) {
        LOG_DEBUG("publisher backpressure: min_rd=%u wr=%u rd0=%u rd1=%u",
            min_descriptor_read_index,
            wr,
            p->descriptor_read_indices[0].load(memory_order_relaxed),
            p->descriptor_read_indices[1].load(memory_order_relaxed));

        return PublishAttempt::Blocked;
    }

    LOG_DEBUG("publisher descriptor state: wr=%u min_rd=%u rd0=%u rd1=%u",
        wr,
        min_descriptor_read_index,
        p->descriptor_read_indices[0].load(memory_order_relaxed),
        p->descriptor_read_indices[1].load(memory_order_relaxed));

    uint32_t head_off = p->head.offset[size_class].load(memory_order_relaxed);
    uint32_t tail_off = p->tail.offset[size_class].load(std::memory_order_acquire);

    if (head_off == tail_off) {
        LOG_DEBUG("publisher free-list empty: size_class=%d", size_class);
        return PublishAttempt::Blocked;
    }

#ifdef ENABLE_DEBUG_CHECKS
    {
        if (head_off < kFirstOffset[size_class] || head_off > kLastOffset[size_class]) {
            LOG_FATAL("PublisherChunkOffsetError: class=%d offset=%u", size_class, head_off);
            return PublishAttempt::Failed;
        }

        pthread_mutex_lock(&p->chunk_usage_tracker.mutex);

        size_t chunk_index = (head_off - kFirstOffset[size_class]) / kClassSizeBytes[size_class];
        if (p->chunk_usage_tracker.is_in_use[size_class][chunk_index] == true) {
            LOG_FATAL("PublisherChunkAlreadyInUseError: class=%d index=%zu offset=%u",
                size_class,
                chunk_index,
                head_off);
            pthread_mutex_unlock(&p->chunk_usage_tracker.mutex);
            return PublishAttempt::Failed;
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

    uint32_t chunk_reference_mask = 0;
    for (int data_available_event_fd : data_available_event_fds) {
        auto it = subscriber_slot_index_by_data_available_event_fd.find(data_available_event_fd);
        if (it == subscriber_slot_index_by_data_available_event_fd.end()) {
            LOG_FATAL("PublisherEventFdSlotMapMissingError: data_available_event_fd=%d", data_available_event_fd);
            exit(1);
        }
        chunk_reference_mask += 1 << it->second;
    }
    LOG_DEBUG("publisher chunk_reference_mask=%u", chunk_reference_mask);
    p->chunk_reference_counts[chunk_index].fetch_add(chunk_reference_mask, memory_order_relaxed);

    uint32_t candidate = wr + 1;
    if (candidate >= kDescriptorSlotCount) p->descriptor_write_index.store(candidate % kDescriptorSlotCount, std::memory_order_release);
    else p->descriptor_write_index.store(candidate, std::memory_order_release);

    for (int data_available_event_fd : data_available_event_fds) {
        if (eventfd_write(data_available_event_fd, 1) < 0) {
            LOG_ERROR("PublisherDataAvailableSignalError: event_fd=%d errno=%d (%s)",
                data_available_event_fd,
                errno,
                strerror(errno));
            return PublishAttempt::Failed;
        }
    }
    LOG_DEBUG("publisher wrote message: bytes=%u fill=%d seq=%d",
        message_size_bytes,
        static_cast<int>(c),
        seq - 1);

    --task_num;

    next_message_size_bytes = get_msg_size();

    if (c++ == 'z') c = 'a';

    return PublishAttempt::Succeeded;
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
    if (shm_unlink(shm_name) < 0 && errno != ENOENT) {
        LOG_ERROR("PublisherShmUnlinkError: name=%s errno=%d (%s)", shm_name, errno, strerror(errno));
        return 1;
    }

    int shm_fd = shm_open(shm_name, O_CREAT | O_RDWR, 0666);
    if (shm_fd < 0) {
        LOG_ERROR("PublisherShmOpenError: name=%s errno=%d (%s)", shm_name, errno, strerror(errno));
        return 1;
    }
    if (ftruncate(shm_fd, sizeof(SharedData)) < 0) {
        LOG_ERROR("PublisherShmResizeError: fd=%d bytes=%zu errno=%d (%s)",
            shm_fd,
            sizeof(SharedData),
            errno,
            strerror(errno));
        return 1;
    }
    p = reinterpret_cast<SharedData *>(
        mmap(nullptr, sizeof(SharedData), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0));
    if (p == MAP_FAILED) {
        LOG_ERROR("PublisherShmMapError: fd=%d bytes=%zu errno=%d (%s)",
            shm_fd,
            sizeof(SharedData),
            errno,
            strerror(errno));
        return 1;
    }

    init_shm(*p);
#ifdef ENABLE_DEBUG_CHECKS
    init_chunk_usage_tracker(p->chunk_usage_tracker);
#endif

    const string path = "/tmp/broker.sock";

    broker_fd = socket(AF_UNIX, SOCK_SEQPACKET, 0);
    if (broker_fd < 0) {
        LOG_ERROR("PublisherBrokerSocketCreateError: errno=%d (%s)", errno, strerror(errno));
        return 1;
    }

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

    int space_available_event_fd = eventfd(0, EFD_NONBLOCK);
    if (space_available_event_fd < 0) {
        LOG_ERROR("PublisherSpaceAvailableEventFdCreateError: errno=%d (%s)", errno, strerror(errno));
        return 1;
    }

    char packet[kPublisherTopicRegistrationMessageSize];
    packet[0] = static_cast<char>(BrokerMessageType::PublisherTopicRegistration);

    memcpy(packet + sizeof(BrokerMessageType) / sizeof(char), &topic_id, sizeof(topic_id));

    const ssize_t registration_sent_size =
        send_packet_with_fds(broker_fd, packet, sizeof(packet), {shm_fd, space_available_event_fd});
    if (registration_sent_size != static_cast<ssize_t>(sizeof(packet))) {
        LOG_ERROR("PublisherRegistrationSendError: expected=%zu actual=%zd errno=%d (%s)",
            sizeof(packet),
            registration_sent_size,
            errno,
            strerror(errno));
        return 1;
    }

    uint32_t subscriber_count;
    {
        ssize_t n = recv(broker_fd, &subscriber_count, sizeof(subscriber_count), 0);
        if (n != static_cast<ssize_t>(sizeof(subscriber_count))) {
            if (n < 0) {
                LOG_ERROR("InitialSubscriberCountReceiveError: broker_fd=%d errno=%d (%s)",
                    broker_fd,
                    errno,
                    strerror(errno));
            } else {
                LOG_ERROR("InitialSubscriberCountReceiveSizeError: broker_fd=%d expected=%zu actual=%zd",
                    broker_fd,
                    sizeof(subscriber_count),
                    n);
            }
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

    if (!set_fd_nonblocking(broker_fd)) {
        LOG_ERROR("PublisherBrokerNonblockingError: broker_fd=%d errno=%d (%s)",
            broker_fd,
            errno,
            strerror(errno));
        return 1;
    }

    epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        LOG_ERROR("PublisherEpollCreateError: errno=%d (%s)", errno, strerror(errno));
        return 1;
    }

    if (!add_fd_to_epoll(epoll_fd, broker_fd)) {
        LOG_ERROR("PublisherBrokerEpollAddError: epoll_fd=%d broker_fd=%d errno=%d (%s)",
            epoll_fd,
            broker_fd,
            errno,
            strerror(errno));
        return 1;
    }

    int publish_work_event_fd = eventfd(0, EFD_NONBLOCK);
    if (publish_work_event_fd < 0) {
        LOG_ERROR("PublisherWorkEventFdCreateError: errno=%d (%s)", errno, strerror(errno));
        return 1;
    }
    if (!add_fd_to_epoll(epoll_fd, publish_work_event_fd)) {
        LOG_ERROR("PublisherWorkEpollAddError: epoll_fd=%d event_fd=%d errno=%d (%s)",
            epoll_fd,
            publish_work_event_fd,
            errno,
            strerror(errno));
        return 1;
    }
    if (eventfd_write(publish_work_event_fd, 1) < 0) {
        LOG_ERROR("PublisherWorkInitialSignalError: event_fd=%d errno=%d (%s)",
            publish_work_event_fd,
            errno,
            strerror(errno));
        return 1;
    }

    const size_t kMaxPublishesPerWorkEvent = 64;

    if (!add_fd_to_epoll(epoll_fd, space_available_event_fd)) {
        LOG_ERROR("PublisherSpaceAvailableEpollAddError: epoll_fd=%d event_fd=%d errno=%d (%s)",
            epoll_fd,
            space_available_event_fd,
            errno,
            strerror(errno));
        return 1;
    }

    epoll_event events[kMaxEvents]{};

    bool publisher_ready = true;

    while (task_num > 0) {
        int event_count = epoll_wait(epoll_fd, events, kMaxEvents, -1);

        if (event_count < 0) {
            LOG_ERROR("PublisherEpollWaitError: errno=%d (%s)", errno, strerror(errno));
            return 1;
        } else if (event_count == 0) {
            LOG_ERROR("PublisherUnexpectedEpollTimeoutError");
            return 1;
        }

        for (int event_index = 0; event_index < event_count; ++event_index) {
            int fd = events[event_index].data.fd;

            if (fd == publish_work_event_fd) {
                eventfd_t value;
                if (eventfd_read(fd, &value) < 0) {
                    LOG_ERROR("PublisherWorkEventReadError: event_fd=%d errno=%d (%s)",
                        fd,
                        errno,
                        strerror(errno));
                    return 1;
                }

                if (!publisher_ready) continue;

                size_t published_count = 0;
                while (published_count < kMaxPublishesPerWorkEvent && task_num > 0) {
                    const PublishAttempt result = try_publish_one();

                    if (result == PublishAttempt::Blocked) {
                        publisher_ready = false;
                        break;
                    }
                    if (result == PublishAttempt::Failed) {
                        return 1;
                    }

                    ++published_count;
                }

                if (!publisher_ready) {
                    continue;
                }

                if (task_num > 0) {
                    if (eventfd_write(publish_work_event_fd, 1) < 0) {
                        LOG_ERROR("PublisherWorkRescheduleError: event_fd=%d errno=%d (%s)",
                            publish_work_event_fd,
                            errno,
                            strerror(errno));
                        return 1;
                    }
                }
            } else if (fd == space_available_event_fd) {
                eventfd_t value;
                if (eventfd_read(fd, &value) < 0) {
                    LOG_ERROR("PublisherSpaceAvailableEventReadError: event_fd=%d errno=%d (%s)",
                        fd,
                        errno,
                        strerror(errno));
                    return 1;
                }

                if (can_publish_next_message()) {
                    publisher_ready = true;
                    if (eventfd_write(publish_work_event_fd, 1) < 0) {
                        LOG_ERROR("PublisherWorkResumeError: event_fd=%d errno=%d (%s)",
                            publish_work_event_fd,
                            errno,
                            strerror(errno));
                        return 1;
                    }
                }
            } else if (fd == broker_fd) {
                process_broker_control_messages();

                if (can_publish_next_message()) {
                    publisher_ready = true;
                    if (eventfd_write(publish_work_event_fd, 1) < 0) {
                        LOG_ERROR("PublisherBrokerResumeError: event_fd=%d errno=%d (%s)",
                            publish_work_event_fd,
                            errno,
                            strerror(errno));
                        return 1;
                    }
                }
                /*
                 1，初始没有 sub 时，pub job 发现 min RD 无效 -> 阻塞
                 2，sub disconnected 时，死亡处理可能归还 chunk
                 */
            }
        }
    }

    for (int data_available_event_fd : data_available_event_fds) {
        if (eventfd_write(data_available_event_fd, 1) < 0) {
            LOG_ERROR("PublisherDataAvailableSignalError: event_fd=%d errno=%d (%s)",
                data_available_event_fd,
                errno,
                strerror(errno));
            return 1;
        }
    }
    LOG_INFO("publisher completed message production");

    if (broker_fd > 0) close(broker_fd);
    close(epoll_fd);
    close(publish_work_event_fd);
    close(space_available_event_fd);

    munmap(p, sizeof(SharedData));
    close(shm_fd);
    shm_unlink(shm_name);

    return 0;
}
