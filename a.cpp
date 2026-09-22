#include "include/logger.h"
#include "include/broker_protocol.hpp"
#include "include/shared_memory_layout.hpp"
#include "include/shared_memory_layout_helpers.hpp"
#include "include/fd_helpers.hpp"
#include <atomic>
#include <cerrno>
#include <cstring>
#include <cstddef>
#include <iostream>
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
#include <unordered_map>
using namespace std;

constexpr size_t kMaxEvents = 1024;

std::filesystem::path subscriber_log_path() {
    const char *run_id_env = std::getenv("DRIVEBUS_RUN_ID");
    const string run_id = run_id_env != nullptr && *run_id_env != '\0' ? run_id_env : "standalone";
    const auto start_us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    return std::filesystem::path("logs") /
        run_id /
        "subscriber" /
        ("pid-" + std::to_string(getpid()) +
            "-start-" + std::to_string(start_us) + ".log");
}

struct PublisherAttachment {
    TopicId topic_id = TopicId::Invalid;
    int shm_fd = -1;
    SharedData *shared_data = nullptr;
    int space_available_event_fd = -1;
    int data_available_event_fd = -1;
    uint32_t slot_index = kInvalidIndex;
    uint32_t subscriber_read_index = kInvalidIndex;
    int expected_seq = -1;
};

ssize_t receive_broker_packet(int broker_fd, char packet[], size_t packet_size, int &received_fd) {
    struct iovec iov{};
    iov.iov_base = packet;
    iov.iov_len = packet_size;

    struct msghdr msg{};
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;

    char control[CMSG_SPACE(sizeof(int))];
    memset(control, 0, sizeof(control));

    msg.msg_control = control;
    msg.msg_controllen = sizeof(control);

    received_fd = -1;
    ssize_t received_size = recvmsg(broker_fd, &msg, 0);
    if (received_size <= 0) return received_size;

    struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    if (cmsg == nullptr) return received_size;
    if (cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SCM_RIGHTS) {
        errno = EPROTO;
        return -1;
    }

    received_fd = *reinterpret_cast<int *>(CMSG_DATA(cmsg));
    return received_size;
}

bool read_data(PublisherAttachment &publisher_attachment, MessageDescriptor desc_ring[], const size_t descriptor_count) {
    SharedData *p = publisher_attachment.shared_data;
    for (size_t descriptor_index = 0; descriptor_index < descriptor_count; ++descriptor_index) {
        uint32_t offset = desc_ring[descriptor_index].offset;
        uint32_t message_size_bytes = desc_ring[descriptor_index].len;

        int size_class = find_size_class(message_size_bytes);
        if (size_class < 0) {
            LOG_FATAL("SubscriberMessageSizeClassError: seq=%d, descriptor=%zu, size=%u",
                publisher_attachment.expected_seq,
                descriptor_index,
                message_size_bytes);
            return false;
        }

        int magic, seq;
        memcpy(&magic, p->data + offset, sizeof(int));
        memcpy(&seq, p->data + offset + sizeof(int), sizeof(int));
        LOG_DEBUG("read bytes=%u offset=%u seq=%d", message_size_bytes, offset, seq);

        if (publisher_attachment.expected_seq == -1) publisher_attachment.expected_seq = seq;

        bool is_error = false;
        if (magic != kMagic) {
            LOG_FATAL("shared-memory magic mismatch: expected=%d actual=%d",
                kMagic,
                magic);
            is_error = true;
        }
        if (seq != publisher_attachment.expected_seq) {
            LOG_FATAL("shared-memory seq mismatch: expected=%d actual=%d",
                publisher_attachment.expected_seq,
                seq);
            is_error = true;
        }
        if (is_error) return false;
        if (message_size_bytes > 2 * sizeof(int)) {
            write(1, p->data + offset + 2 * sizeof(int), message_size_bytes - 2 * sizeof(int));
        }
        cout << endl;

        ++publisher_attachment.expected_seq;

#ifdef ENABLE_DEBUG_CHECKS
        if (offset < kFirstOffset[size_class] || offset > kLastOffset[size_class]) {
            LOG_FATAL("SubscriberChunkOffsetError: class=%d, offset=%u", size_class, offset);
            return false;
        }
#endif

        uint32_t local_chunk_index = (offset - kFirstOffset[size_class]) / kClassSizeBytes[size_class];
        uint32_t chunk_index = kChunkIndexBaseBySizeClass[size_class] + local_chunk_index;
        uint32_t subscriber_reference_bit = 1 << publisher_attachment.slot_index;
        uint32_t previous_reference_count = p->chunk_reference_counts[chunk_index].fetch_and(~subscriber_reference_bit, std::memory_order_acq_rel);

        if (++publisher_attachment.subscriber_read_index >= kDescriptorSlotCount)
            publisher_attachment.subscriber_read_index %= kDescriptorSlotCount;
        p->descriptor_read_indices[publisher_attachment.slot_index].store(publisher_attachment.subscriber_read_index, std::memory_order_release);
        LOG_DEBUG("subscriber_read_index is %u", publisher_attachment.subscriber_read_index);

        if (previous_reference_count == subscriber_reference_bit) {
#ifdef ENABLE_DEBUG_CHECKS
            {
                pthread_mutex_lock(&p->chunk_usage_tracker.mutex);

                size_t chunk_index = (offset - kFirstOffset[size_class]) / kClassSizeBytes[size_class];
                LOG_DEBUG("the %zu chunk free", chunk_index);
                if (p->chunk_usage_tracker.is_in_use[size_class][chunk_index] == false) {
                    LOG_FATAL("SubscriberChunkDoubleFreeError: class=%d, index=%zu, offset=%u",
                        size_class,
                        chunk_index,
                        offset);
                    pthread_mutex_unlock(&p->chunk_usage_tracker.mutex);
                    return false;
                }
                p->chunk_usage_tracker.is_in_use[size_class][chunk_index] = false;

                pthread_mutex_unlock(&p->chunk_usage_tracker.mutex);
            }
#endif

            uint32_t last_tail_off = p->tail.offset[size_class].load(memory_order_relaxed);
            memcpy(p->data + last_tail_off, &offset, sizeof(uint32_t));
            p->tail.offset[size_class].store(offset, std::memory_order_release);

            if (eventfd_write(publisher_attachment.space_available_event_fd, 1) < 0) {
                LOG_ERROR("SubscriberSpaceAvailableSignalError: event_fd=%d errno=%d (%s)",
                    publisher_attachment.space_available_event_fd,
                    errno,
                    strerror(errno));
                return false;
            }
        }
    }
    return true;
}

void initialize_subscriber_read_index(PublisherAttachment &publisher_attachment) {
    if (publisher_attachment.subscriber_read_index == kInvalidIndex)
        publisher_attachment.subscriber_read_index = publisher_attachment.shared_data
                                                         ->descriptor_read_indices[publisher_attachment.slot_index]
                                                         .load(memory_order_relaxed);
}

bool consume_contiguous_messages(PublisherAttachment &publisher_attachment) {
    SharedData *p = publisher_attachment.shared_data;

    uint32_t wr = p->descriptor_write_index.load(std::memory_order_acquire);
    while (wr > publisher_attachment.subscriber_read_index) {
        const size_t descriptor_count = static_cast<size_t>(wr - publisher_attachment.subscriber_read_index);
        MessageDescriptor desc_ring[descriptor_count];
        copy(p->desc_ring + publisher_attachment.subscriber_read_index, p->desc_ring + wr, desc_ring);

        if (!read_data(publisher_attachment, desc_ring, descriptor_count)) {
            return false;
        }
        wr = p->descriptor_write_index.load(std::memory_order_acquire);
    }
    return true;
}

int epoll_fd = -1;
int broker_fd = -1;

unordered_map<int, PublisherAttachment> publisher_attachment_by_data_available_event_fd;
unordered_map<AttachmentId, int> data_available_event_fd_by_attachment_id;

bool process_broker_control_messages() {
    char packet[kMaxMessageSize];
    char control[CMSG_SPACE(kMaxAttachmentsPerBatch * 3 * sizeof(int))];

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
            LOG_ERROR("broker control connection closed: broker_fd=%d", broker_fd);
            return false;
        } else if (received_size < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            } else {
                LOG_ERROR("broker control recv failed: fd=%d errno=%d (%s)", broker_fd, errno, strerror(errno));
                return false;
            }
        } else if (msg.msg_flags & MSG_CTRUNC) {
            LOG_FATAL("MsgTruncatedError");
            return false;
        }

        size_t body_len = received_size - sizeof(BrokerMessageType);
        BrokerMessageType message_type;
        memcpy(&message_type, packet, sizeof(BrokerMessageType));

        switch (message_type) {
            case BrokerMessageType::SubscriberAttachmentBatch: {
                /*
                [type]
                重复直到普通数据结束：
                    [topic_id]
                    [slot_index]

                SCM_RIGHTS:
                    [shm_fd_0, space_event_fd_0, data_event_fd_0,
                     shm_fd_1, space_event_fd_1,  data_event_fd_1, ...]
                */

                if (body_len % kSubscriberAttachmentMetadataSize != 0) {
                    LOG_FATAL("SubscriberAttachmentBatchPayloadSizeError");
                    return false;
                }

                const size_t attachment_count = body_len / kSubscriberAttachmentMetadataSize;

                cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);

                if (attachment_count == 0) {
                    if (cmsg != nullptr) {
                        LOG_FATAL("SubscriberAttachmentBatchUnexpectedFdError");
                        return false;
                    }
                    break;
                }

                if (is_valid_scm_rights_cmsg(cmsg) == false) {
                    LOG_FATAL("SubscriberAttachmentBatchCmsgError");
                    return false;
                }

                const size_t fd_data_size = cmsg->cmsg_len - CMSG_LEN(0);
                if (fd_data_size % sizeof(int) != 0) {
                    LOG_FATAL("SubscriberAttachmentBatchFdSizeError");
                    return false;
                }

                const size_t fd_count = fd_data_size / sizeof(int);
                if (fd_count != attachment_count * 3) {
                    LOG_FATAL("SubscriberAttachmentBatchFdCountError");
                    return false;
                }

                const int *fds = reinterpret_cast<const int *>(CMSG_DATA(cmsg));

                size_t packet_offset = sizeof(BrokerMessageType);

                for (size_t attachment_index = 0; attachment_index < attachment_count; ++attachment_index) {
                    AttachmentId attachment_id = kInvalidAttachmentId;
                    memcpy(&attachment_id, packet + packet_offset, sizeof(AttachmentId));
                    packet_offset += sizeof(AttachmentId);

                    PublisherAttachment attachment;

                    memcpy(&attachment.topic_id, packet + packet_offset, sizeof(attachment.topic_id));
                    packet_offset += sizeof(attachment.topic_id);

                    memcpy(&attachment.slot_index, packet + packet_offset, sizeof(attachment.slot_index));
                    packet_offset += sizeof(attachment.slot_index);

                    attachment.shm_fd = fds[3 * attachment_index];
                    attachment.space_available_event_fd = fds[3 * attachment_index + 1];
                    attachment.data_available_event_fd = fds[3 * attachment_index + 2];

                    attachment.shared_data = static_cast<SharedData *>(mmap(nullptr, sizeof(SharedData), PROT_READ | PROT_WRITE, MAP_SHARED, attachment.shm_fd, 0));

                    if (attachment.shared_data == MAP_FAILED) {
                        LOG_ERROR("SubscriberAttachmentBatchMmapError");
                        return false;
                    }

                    if (!add_fd_to_epoll(epoll_fd, attachment.data_available_event_fd)) {
                        LOG_ERROR("SubscriberAttachmentEpollAddError: epoll_fd=%d event_fd=%d errno=%d (%s)",
                            epoll_fd,
                            attachment.data_available_event_fd,
                            errno,
                            strerror(errno));
                        munmap(attachment.shared_data, sizeof(SharedData));
                        close(attachment.shm_fd);
                        close(attachment.space_available_event_fd);
                        close(attachment.data_available_event_fd);
                        return false;
                    }

                    auto [attachment_id_it, attachment_id_inserted] =
                        data_available_event_fd_by_attachment_id.insert({attachment_id, attachment.data_available_event_fd});
                    if (!attachment_id_inserted) {
                        LOG_FATAL("SubscriberAttachmentIdAlreadyExistsError: attachment_id=%llu",
                            static_cast<unsigned long long>(attachment_id));
                        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, attachment.data_available_event_fd, nullptr);
                        munmap(attachment.shared_data, sizeof(SharedData));
                        close(attachment.shm_fd);
                        close(attachment.space_available_event_fd);
                        close(attachment.data_available_event_fd);
                        return false;
                    }

                    auto attachment_by_event_fd_it =
                        publisher_attachment_by_data_available_event_fd.find(attachment.data_available_event_fd);
                    if (attachment_by_event_fd_it != publisher_attachment_by_data_available_event_fd.end()) {
                        LOG_FATAL("SubscriberDataAvailableEventFdAlreadyExistsError: event_fd=%d",
                            attachment.data_available_event_fd);
                        data_available_event_fd_by_attachment_id.erase(attachment_id_it);
                        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, attachment.data_available_event_fd, nullptr);
                        munmap(attachment.shared_data, sizeof(SharedData));
                        close(attachment.shm_fd);
                        close(attachment.space_available_event_fd);
                        close(attachment.data_available_event_fd);
                        return false;
                    }

                    publisher_attachment_by_data_available_event_fd.emplace(
                        attachment.data_available_event_fd,
                        std::move(attachment));
                }

                LOG_INFO("subscriber attachment batch installed: count=%zu",
                    attachment_count);
                break;
            }
            case BrokerMessageType::PublisherDisconnected: {
                if (body_len != sizeof(AttachmentId)) {
                    LOG_FATAL("PublisherDisconnectedPayloadSizeError: bytes=%zu", body_len);
                    return false;
                }

                AttachmentId attachment_id = kInvalidAttachmentId;
                memcpy(&attachment_id, packet + sizeof(BrokerMessageType), sizeof(attachment_id));

                auto data_available_event_fd_it = data_available_event_fd_by_attachment_id.find(attachment_id);
                if (data_available_event_fd_it == data_available_event_fd_by_attachment_id.end()) {
                    LOG_FATAL("PublisherDisconnectedAttachmentMissingError: attachment_id=%llu",
                        static_cast<unsigned long long>(attachment_id));
                    return false;
                }

                int data_available_event_fd = data_available_event_fd_it->second;

                auto publisher_attachment_it = publisher_attachment_by_data_available_event_fd.find(data_available_event_fd);
                if (publisher_attachment_it == publisher_attachment_by_data_available_event_fd.end()) {
                    LOG_FATAL("PublisherDisconnectedEventFdMissingError: attachment_id=%llu data_available_event_fd=%d",
                        static_cast<unsigned long long>(attachment_id),
                        data_available_event_fd);
                    return false;
                }

                epoll_ctl(epoll_fd, EPOLL_CTL_DEL, data_available_event_fd, nullptr);
                close(publisher_attachment_it->second.space_available_event_fd);
                close(data_available_event_fd);

                munmap(publisher_attachment_it->second.shared_data, sizeof(SharedData));
                close(publisher_attachment_it->second.shm_fd);

                data_available_event_fd_by_attachment_id.erase(data_available_event_fd_it);
                publisher_attachment_by_data_available_event_fd.erase(publisher_attachment_it);
                break;
            }
            default: {
                LOG_FATAL("UnexpectedBrokerMessageTypeError: type=%u, bytes=%zd, broker_fd=%d", static_cast<unsigned int>(static_cast<unsigned char>(packet[0])), received_size, broker_fd);
                return false;
            }
        }
    }
    return true;
}

int main(int argc, char *argv[]) {
    if (argc < 2 || argc > static_cast<int>(kMaxSubscriberTopicCount) + 1) {
        cerr << "SubscriberTopicArgumentError: usage=" << argv[0]
             << " <topic_id> [topic_id...]"
             << ", max_topics=" << kMaxSubscriberTopicCount << endl;
        return 1;
    }

    Logger::init(subscriber_log_path());

    uint32_t topic_count = static_cast<uint32_t>(argc - 1);

    const string path = "/tmp/broker.sock";

    broker_fd = socket(AF_UNIX, SOCK_SEQPACKET, 0);
    if (broker_fd < 0) {
        LOG_ERROR("SubscriberBrokerSocketCreateError: errno=%d (%s)", errno, strerror(errno));
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
        LOG_FATAL("SubscriberBrokerConnectError: attempts=%d, errno=%d (%s)",
            fail_num,
            errno,
            strerror(errno));
        return 1;
    }

    char packet[kSubscriberTopicRegistrationMessageSize]{};
    packet[0] = static_cast<char>(BrokerMessageType::SubscriberTopicRegistration);
    memcpy(packet + sizeof(BrokerMessageType), &topic_count, sizeof(topic_count));
    for (uint32_t topic_index = 0; topic_index < topic_count; ++topic_index) {
        TopicId topic_id = static_cast<TopicId>(stoul(argv[topic_index + 1]));
        memcpy(packet + sizeof(BrokerMessageType) + sizeof(topic_count) + topic_index * sizeof(TopicId),
            &topic_id,
            sizeof(topic_id));
    }

    const ssize_t registration_sent_size = send(broker_fd, packet, sizeof(packet), 0);
    if (registration_sent_size != static_cast<ssize_t>(sizeof(packet))) {
        LOG_ERROR("SubscriberRegistrationSendError: expected=%zu actual=%zd errno=%d (%s)",
            sizeof(packet),
            registration_sent_size,
            errno,
            strerror(errno));
        return 1;
    }

    epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        LOG_ERROR("SubscriberEpollCreateError: errno=%d (%s)", errno, strerror(errno));
        return 1;
    }
    if (!add_fd_to_epoll(epoll_fd, broker_fd)) {
        LOG_ERROR("SubscriberBrokerEpollAddError: epoll_fd=%d broker_fd=%d errno=%d (%s)",
            epoll_fd,
            broker_fd,
            errno,
            strerror(errno));
        return 1;
    }

    epoll_event events[kMaxEvents];

    int subscriber_exit_code = 0;
    bool subscriber_running = true;
    while (subscriber_running) {
        int event_count = epoll_wait(epoll_fd, events, kMaxEvents, -1);
        if (event_count < 0) {
            LOG_ERROR("SubscriberEpollWaitError: errno=%d (%s)", errno, strerror(errno));
            return 1;
        }
        if (event_count == 0) {
            LOG_ERROR("SubscriberUnexpectedEpollTimeoutError");
            return 1;
        }
        LOG_DEBUG("epoll event_count=%d", event_count);

        for (int i = 0; i < event_count; ++i) {
            int fd = events[i].data.fd;
            if (fd != broker_fd) {
                auto publisher_attachment_it = publisher_attachment_by_data_available_event_fd.find(fd);
                if (publisher_attachment_it == publisher_attachment_by_data_available_event_fd.end()) {
                    LOG_FATAL("SubscriberEventFdAttachmentMissingError: data_available_event_fd=%d", fd);
                    subscriber_exit_code = 1;
                    subscriber_running = false;
                    break;
                }
                PublisherAttachment &publisher_attachment = publisher_attachment_it->second;

                uint64_t val;
                const ssize_t data_event_read_size = read(fd, &val, sizeof(val));
                if (data_event_read_size != static_cast<ssize_t>(sizeof(val))) {
                    LOG_ERROR("SubscriberDataAvailableEventReadError: event_fd=%d expected=%zu actual=%zd errno=%d (%s)",
                        fd,
                        sizeof(val),
                        data_event_read_size,
                        errno,
                        strerror(errno));
                    subscriber_exit_code = 1;
                    subscriber_running = false;
                    break;
                }

                initialize_subscriber_read_index(publisher_attachment);

                uint32_t wr = publisher_attachment.shared_data->descriptor_write_index.load(std::memory_order_acquire);
                LOG_DEBUG("descriptor indices: wr=%u rd=%u", wr, publisher_attachment.subscriber_read_index);

                if (publisher_attachment.subscriber_read_index > wr) {
                    const size_t descriptor_count = static_cast<size_t>(
                        kDescriptorSlotCount - publisher_attachment.subscriber_read_index);

                    MessageDescriptor desc_ring[descriptor_count];
                    copy(publisher_attachment.shared_data->desc_ring + publisher_attachment.subscriber_read_index,
                        publisher_attachment.shared_data->desc_ring + kDescriptorSlotCount,
                        desc_ring);

                    if (!read_data(publisher_attachment, desc_ring, descriptor_count)) {
                        subscriber_exit_code = 1;
                        subscriber_running = false;
                        break;
                    }
                    wr = publisher_attachment.shared_data->descriptor_write_index.load(std::memory_order_acquire);
                }

                if (!consume_contiguous_messages(publisher_attachment)) {
                    subscriber_exit_code = 1;
                    subscriber_running = false;
                    break;
                }

                LOG_DEBUG("subscriber event processed: data_available_event_fd=%d", fd);
            } else if (fd == broker_fd) {
                if (!process_broker_control_messages()) {
                    subscriber_exit_code = 1;
                    subscriber_running = false;
                    break;
                }
            }
        }
        this_thread::sleep_for(chrono::milliseconds(1));
    }

#ifdef ENABLE_DEBUG_CHECKS
    for (auto &[data_available_event_fd, publisher_attachment] : publisher_attachment_by_data_available_event_fd) {
        SharedData *p = publisher_attachment.shared_data;
        pthread_mutex_lock(&p->chunk_usage_tracker.mutex);

        for (int i = 0; i < kClassCount; ++i) {
            for (int j = 0; j < kMaxChunkCountPerSizeClass; ++j) {
                if (p->chunk_usage_tracker.is_in_use[i][j] == true) {
                    LOG_WARN("subscriber stopping with unreleased chunk: data_available_event_fd=%d class=%d index=%d",
                        data_available_event_fd,
                        i,
                        j);
                }
            }
        }

        pthread_mutex_unlock(&p->chunk_usage_tracker.mutex);
        pthread_mutex_destroy(&p->chunk_usage_tracker.mutex);
    }
#endif

    for (auto &[data_available_event_fd, publisher_attachment] : publisher_attachment_by_data_available_event_fd) {
        munmap(publisher_attachment.shared_data, sizeof(SharedData));
        close(publisher_attachment.shm_fd);
        close(publisher_attachment.space_available_event_fd);
        close(data_available_event_fd);
    }

    return subscriber_exit_code;
}
