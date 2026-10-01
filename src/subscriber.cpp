#include "../include/subscriber.hpp"
#include "../include/logger.h"
#include "../include/shared_memory_layout_helpers.hpp"
#include "../include/fd_helpers.hpp"
#include <filesystem>
#include <optional>
#include <utility>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/un.h>

namespace {
std::filesystem::path subscriber_log_path() {
    const char *run_id_env = std::getenv("DRIVEBUS_RUN_ID");
    const std::string run_id = run_id_env != nullptr && *run_id_env != '\0' ? run_id_env : "standalone";
    const auto start_us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    return std::filesystem::path("logs") /
        run_id /
        "subscriber" /
        ("pid-" + std::to_string(getpid()) +
            "-start-" + std::to_string(start_us) + ".log");
}

std::variant<int, Subscriber::CreateError> connect_to_broker() {
    const std::string path = "/tmp/broker.sock";

    int broker_fd = socket(AF_UNIX, SOCK_SEQPACKET, 0);
    if (broker_fd < 0) {
        LOG_ERROR("SubscriberBrokerSocketCreateError: errno=%d (%s)", errno, strerror(errno));
        return Subscriber::CreateError::BrokerSocketCreateFailed;
    }

    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, path.c_str());

    int connect_status = connect(broker_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
    int retry_count = 1;
    while (connect_status < 0 && retry_count < 2) {
        sleep(1);
        ++retry_count;
        connect_status = connect(broker_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
    }
    if (connect_status < 0) {
        LOG_FATAL("SubscriberBrokerConnectError: attempts=%d, errno=%d (%s)",
            retry_count,
            errno,
            strerror(errno));
        close_fd(broker_fd);
        return Subscriber::CreateError::BrokerConnectFailed;
    }

    return broker_fd;
}

std::optional<Subscriber::CreateError> send_subscriber_topic_registration(const int broker_fd, std::span<const TopicId> topic_ids) {
    char packet[kSubscriberTopicRegistrationMessageSize]{};
    BrokerMessageType type = BrokerMessageType::SubscriberTopicRegistration;
    memcpy(packet, &type, sizeof(BrokerMessageType));

    uint32_t topic_count = static_cast<uint32_t>(topic_ids.size());
    if (topic_count == 0 || topic_count > kMaxSubscriberTopicCount) {
        LOG_ERROR(
            "SubscriberTopicRegistrationCountExceededError: topic_count=%u max=%zu",
            topic_count,
            kMaxSubscriberTopicCount);
        return Subscriber::CreateError::TopicRegistrationCountExceeded;
    }

    memcpy(packet + sizeof(BrokerMessageType), &topic_count, sizeof(topic_count));

    for (uint32_t topic_index = 0; topic_index < topic_count; ++topic_index) {
        TopicId topic_id = static_cast<TopicId>(topic_ids[topic_index]);
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
        return Subscriber::CreateError::TopicRegistrationSendFailed;
    }
    return std::nullopt;
}
} // namespace

Subscriber::CreateResult Subscriber::create(std::span<const TopicId> topic_ids) {
    Logger::init(subscriber_log_path());

    int epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        LOG_ERROR("SubscriberEpollCreateError: errno=%d (%s)", errno, strerror(errno));
        return CreateResult{CreateError::EpollCreateFailed};
    }

    auto connect_result = connect_to_broker();
    auto *connect_error = std::get_if<CreateError>(&connect_result);
    if (connect_error != nullptr) {
        close_fd(epoll_fd);
        return CreateResult{*connect_error};
    }

    int broker_fd = get<int>(connect_result);

    auto registration_result = send_subscriber_topic_registration(broker_fd, topic_ids);
    if (registration_result.has_value()) {
        LOG_ERROR("SubscriberTopicRegistrationCreateError: topic_count=%zu", topic_ids.size());
        close_fd(epoll_fd);
        close_fd(broker_fd);
        return CreateResult{*registration_result};
    }
    // 阻塞等待

    if (!add_fd_to_epoll(epoll_fd, broker_fd)) {
        LOG_ERROR("SubscriberBrokerEpollAddError: epoll_fd=%d broker_fd=%d errno=%d (%s)",
            epoll_fd,
            broker_fd,
            errno,
            strerror(errno));
        close_fd(epoll_fd);
        close_fd(broker_fd);
        return CreateResult{CreateError::BrokerEpollRegistrationFailed};
    }

    return CreateResult{Subscriber(epoll_fd, broker_fd)};
}

Subscriber::Subscriber(Subscriber &&other) noexcept {
    take_resources_from(std::move(other));
}

Subscriber &Subscriber::operator=(Subscriber &&other) noexcept {
    if (this != &other) {
        release_resources();
        take_resources_from(std::move(other));
    }
    return *this;
}

Subscriber::~Subscriber() {
    release_resources();
}

Subscriber::Subscriber(int epoll_fd, int broker_fd) : epoll_fd_(epoll_fd), broker_fd_(broker_fd) {}

void Subscriber::take_resources_from(Subscriber &&other) {
    epoll_fd_ = std::exchange(other.epoll_fd_, -1);
    broker_fd_ = std::exchange(other.broker_fd_, -1);

    publisher_attachment_by_data_available_event_fd_.clear();
    publisher_attachment_by_data_available_event_fd_.swap(
        other.publisher_attachment_by_data_available_event_fd_);

    data_available_event_fd_by_attachment_id_.clear();
    data_available_event_fd_by_attachment_id_.swap(
        other.data_available_event_fd_by_attachment_id_);
}

void Subscriber::release_resources() {
    for (auto &[event_fd, attachment] : publisher_attachment_by_data_available_event_fd_) {
        release_publisher_attachment(attachment);
    }

    close_fd(epoll_fd_);
    close_fd(broker_fd_);
}

void Subscriber::release_publisher_attachment(PublisherAttachment &attachment) {
    if (attachment.data_available_event_fd > 0)
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, attachment.data_available_event_fd, nullptr);

    if (attachment.shared_data != MAP_FAILED)
        munmap(attachment.shared_data, sizeof(SharedData));

    close_fd(attachment.shm_fd);
    close_fd(attachment.space_available_event_fd);
    close_fd(attachment.data_available_event_fd);
}

bool Subscriber::read_data(PublisherAttachment &publisher_attachment, MessageDescriptor desc_ring[], const size_t descriptor_count) {
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

#ifdef ENABLE_DEBUG_CHECKS
        if (offset < kFirstOffset[size_class] || offset > kLastOffset[size_class]) {
            LOG_FATAL("SubscriberChunkOffsetError: class=%d, offset=%u", size_class, offset);
            return false;
        }
#endif

        if (message_size_bytes < 2 * sizeof(int)) {
            LOG_FATAL("SubscriberMessageHeaderSizeError: seq=%d, descriptor=%zu, size=%u, minimum=%zu",
                publisher_attachment.expected_seq,
                descriptor_index,
                message_size_bytes,
                2 * sizeof(int));
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
        // 方便观察输出，可删
        printf("\n");

        ++publisher_attachment.expected_seq;

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

            uint32_t last_tail_off = p->tail.offset[size_class].load(std::memory_order_relaxed);
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

bool Subscriber::consume_contiguous_messages(PublisherAttachment &publisher_attachment) {
    SharedData *p = publisher_attachment.shared_data;

    uint32_t wr = p->descriptor_write_index.load(std::memory_order_acquire);
    while (wr > publisher_attachment.subscriber_read_index) {
        std::vector<MessageDescriptor> desc_ring(p->desc_ring + publisher_attachment.subscriber_read_index, p->desc_ring + wr);

        if (!read_data(publisher_attachment, desc_ring.data(), desc_ring.size())) {
            return false;
        }
        wr = p->descriptor_write_index.load(std::memory_order_acquire);
    }
    return true;
}

bool Subscriber::process_broker_control_messages() {
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

        const ssize_t received_size = recvmsg(broker_fd_, &msg, 0);

        if (received_size == 0) {
            LOG_ERROR("broker control connection closed: broker_fd=%d", broker_fd_);
            return false;
        } else if (received_size < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            } else {
                LOG_ERROR("broker control recv failed: fd=%d errno=%d (%s)", broker_fd_, errno, strerror(errno));
                return false;
            }
        } else if (msg.msg_flags & MSG_CTRUNC || msg.msg_flags & MSG_TRUNC) {
            LOG_FATAL("MsgTruncatedError");
            return false;
        }

        if (received_size < static_cast<ssize_t>(sizeof(BrokerMessageType))) {
            LOG_FATAL("SubscriberBrokerControlMessageHeaderSizeError: minimum=%zu actual=%zd",
                sizeof(BrokerMessageType),
                received_size);
            return false;
        }

        size_t body_len = received_size - sizeof(BrokerMessageType);
        BrokerMessageType message_type;
        memcpy(&message_type, packet, sizeof(BrokerMessageType));

        switch (message_type) {
            case BrokerMessageType::SubscriberAttachmentBatch: {
                cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
                if (install_subscriber_attachment_batch(packet, body_len, cmsg) == false) return false;
                break;
            }
            case BrokerMessageType::PublisherDisconnected: {
                handle_publisher_disconnected(packet, body_len);
                break;
            }
            default: {
                LOG_FATAL("UnexpectedBrokerMessageTypeError: type=%u, bytes=%zd, broker_fd_=%d", static_cast<unsigned int>(static_cast<unsigned char>(packet[0])), received_size, broker_fd_);
                return false;
            }
        }
    }
    return true;
}

bool Subscriber::install_subscriber_attachment_batch(const char packet[], size_t body_len, cmsghdr *cmsg) {
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

    if (attachment_count == 0) {
        if (cmsg != nullptr) {
            LOG_FATAL("SubscriberAttachmentBatchUnexpectedFdError");
            return false;
        }
        return true;
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
    // SCM_RIGHTS 的控制数据按连续的 int 文件描述符数组存放，
    // 因此这里可将 CMSG_DATA 返回的原始数据地址按 const int* 解释。

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
            release_publisher_attachment(attachment);
            return false;
        }

        if (!add_fd_to_epoll(epoll_fd_, attachment.data_available_event_fd)) {
            LOG_ERROR("SubscriberAttachmentEpollAddError: epoll_fd=%d event_fd=%d errno=%d (%s)",
                epoll_fd_,
                attachment.data_available_event_fd,
                errno,
                strerror(errno));
            release_publisher_attachment(attachment);
            return false;
        }

        auto [attachment_id_it, attachment_id_inserted] =
            data_available_event_fd_by_attachment_id_.insert({attachment_id, attachment.data_available_event_fd});

        if (!attachment_id_inserted) {
            LOG_FATAL("SubscriberAttachmentIdAlreadyExistsError: attachment_id=%llu",
                static_cast<unsigned long long>(attachment_id));
            release_publisher_attachment(attachment);
            return false;
        }

        auto attachment_by_event_fd_it =
            publisher_attachment_by_data_available_event_fd_.find(attachment.data_available_event_fd);

        if (attachment_by_event_fd_it != publisher_attachment_by_data_available_event_fd_.end()) {
            LOG_FATAL("SubscriberDataAvailableEventFdAlreadyExistsError: event_fd=%d",
                attachment.data_available_event_fd);
            data_available_event_fd_by_attachment_id_.erase(attachment_id_it);
            release_publisher_attachment(attachment);
            return false;
        }

        publisher_attachment_by_data_available_event_fd_.emplace(
            attachment.data_available_event_fd,
            std::move(attachment));
    }

    LOG_INFO("subscriber attachment batch installed: count=%zu",
        attachment_count);
    return true;
}

bool Subscriber::handle_publisher_disconnected(const char packet[], size_t body_len) {
    if (body_len != sizeof(AttachmentId)) {
        LOG_FATAL("PublisherDisconnectedPayloadSizeError: bytes=%zu", body_len);
        return false;
    }

    AttachmentId attachment_id = kInvalidAttachmentId;
    memcpy(&attachment_id, packet + sizeof(BrokerMessageType), sizeof(attachment_id));

    auto data_available_event_fd_it = data_available_event_fd_by_attachment_id_.find(attachment_id);
    if (data_available_event_fd_it == data_available_event_fd_by_attachment_id_.end()) {
        LOG_FATAL("PublisherDisconnectedAttachmentMissingError: attachment_id=%llu",
            static_cast<unsigned long long>(attachment_id));
        return false;
    }

    int data_available_event_fd = data_available_event_fd_it->second;

    auto publisher_attachment_it = publisher_attachment_by_data_available_event_fd_.find(data_available_event_fd);
    if (publisher_attachment_it == publisher_attachment_by_data_available_event_fd_.end()) {
        LOG_FATAL("PublisherDisconnectedEventFdMissingError: attachment_id=%llu data_available_event_fd=%d",
            static_cast<unsigned long long>(attachment_id),
            data_available_event_fd);
        return false;
    }

    release_publisher_attachment(publisher_attachment_it->second);

    data_available_event_fd_by_attachment_id_.erase(data_available_event_fd_it);
    publisher_attachment_by_data_available_event_fd_.erase(publisher_attachment_it);
    return true;
}

void Subscriber::initialize_subscriber_read_index(PublisherAttachment &publisher_attachment) {
    if (publisher_attachment.subscriber_read_index == kInvalidIndex)
        publisher_attachment.subscriber_read_index = publisher_attachment.shared_data
                                                         ->descriptor_read_indices[publisher_attachment.slot_index]
                                                         .load(std::memory_order_relaxed);
}

Subscriber::RunError Subscriber::handle_data_available_event(const int event_fd) {
    auto publisher_attachment_it = publisher_attachment_by_data_available_event_fd_.find(event_fd);
    if (publisher_attachment_it == publisher_attachment_by_data_available_event_fd_.end()) {
        LOG_FATAL("SubscriberEventFdAttachmentMissingError: data_available_event_fd=%d", event_fd);
        return RunError::DataAvailableEventAttachmentMissing;
    }
    PublisherAttachment &publisher_attachment = publisher_attachment_it->second;

    uint64_t val;
    const ssize_t data_event_read_size = read(event_fd, &val, sizeof(val));
    if (data_event_read_size != static_cast<ssize_t>(sizeof(val))) {
        LOG_ERROR("SubscriberDataAvailableEventReadError: event_fd=%d expected=%zu actual=%zd errno=%d (%s)",
            event_fd,
            sizeof(val),
            data_event_read_size,
            errno,
            strerror(errno));
        return RunError::DataAvailableEventReadFailed;
    }

    initialize_subscriber_read_index(publisher_attachment);

    uint32_t wr = publisher_attachment.shared_data->descriptor_write_index.load(std::memory_order_acquire);
    LOG_DEBUG("descriptor indices: wr=%u rd=%u", wr, publisher_attachment.subscriber_read_index);

    if (publisher_attachment.subscriber_read_index > wr) {
        const size_t descriptor_count = static_cast<size_t>(
            kDescriptorSlotCount - publisher_attachment.subscriber_read_index);

        std::vector<MessageDescriptor> desc_ring(descriptor_count);
        std::copy(publisher_attachment.shared_data->desc_ring + publisher_attachment.subscriber_read_index,
            publisher_attachment.shared_data->desc_ring + kDescriptorSlotCount,
            desc_ring.data());

        if (!read_data(publisher_attachment, desc_ring.data(), descriptor_count)) {
            return RunError::WrappedDescriptorReadFailed;
        }
        wr = publisher_attachment.shared_data->descriptor_write_index.load(std::memory_order_acquire);
    }

    if (!consume_contiguous_messages(publisher_attachment)) {
        return RunError::ContiguousMessageConsumptionFailed;
    }

    LOG_DEBUG("subscriber event processed: data_available_event_fd=%d", event_fd);
    return RunError::None;
}

Subscriber::RunError Subscriber::run() {
    epoll_event events[kMaxEvents];

    RunError run_error = RunError::None;
    bool subscriber_running = true;

    while (subscriber_running) {
        int event_count = epoll_wait(epoll_fd_, events, kMaxEvents, -1);
        if (event_count < 0) {
            LOG_ERROR("SubscriberEpollWaitError: errno=%d (%s)", errno, strerror(errno));
            return RunError::EpollWaitFailed;
        }
        if (event_count == 0) {
            LOG_ERROR("SubscriberUnexpectedEpollTimeoutError");
            return RunError::UnexpectedEpollTimeout;
        }
        LOG_DEBUG("epoll event_count=%d", event_count);

        for (int i = 0; i < event_count; ++i) {
            int fd = events[i].data.fd;
            if (fd != broker_fd_) {
                run_error = handle_data_available_event(fd);
                if (run_error != RunError::None) {
                    subscriber_running = false;
                    break;
                }
            } else if (fd == broker_fd_) {
                if (!process_broker_control_messages()) {
                    run_error = RunError::BrokerControlMessageProcessingFailed;
                    subscriber_running = false;
                    break;
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

#ifdef ENABLE_DEBUG_CHECKS
    log_chunks_in_use();
#endif
    return run_error;
}

void Subscriber::log_chunks_in_use() {
    for (auto &[data_available_event_fd, publisher_attachment] : publisher_attachment_by_data_available_event_fd_) {
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
    }
}
