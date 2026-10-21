#include "../include/subscriber.hpp"
#include "../include/logger.h"
#include "../include/shared_memory_layout_helpers.hpp"
#include "../include/fd_helpers.hpp"
#include <algorithm>
#include <cassert>
#include <exception>
#include <filesystem>
#include <limits>
#include <optional>
#include <system_error>
#include <utility>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/un.h>

namespace {
constexpr AttachmentId kBrokerEventId = 0;
constexpr AttachmentId kShutdownEventId = std::numeric_limits<AttachmentId>::max();

class TransferEventDrainError : public std::system_error {
  public:
    explicit TransferEventDrainError(int error)
        : std::system_error(error, std::generic_category(), "Subscriber transfer event drain") {}
};

bool add_fd_to_epoll(const int epoll_fd, const int fd, const uint64_t event_id) {
    epoll_event event{};
    event.data.u64 = event_id;
    event.events = EPOLLET | EPOLLIN;
    if (!set_fd_nonblocking(fd)) return false;

    return epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &event) == 0;
}

Subscriber::SubscribeError map_run_error_to_subscribe_error(
    const Subscriber::RunError run_error) {
    assert(run_error != Subscriber::RunError::None);
    switch (run_error) {
        case Subscriber::RunError::None:
            std::terminate();
        case Subscriber::RunError::EpollWaitFailed:
            return Subscriber::SubscribeError::EventWaitFailed;
        case Subscriber::RunError::UnexpectedEpollTimeout:
            return Subscriber::SubscribeError::Timeout;
        case Subscriber::RunError::BrokerControlMessageProcessingFailed:
        case Subscriber::RunError::PublisherDisconnectedPayloadSizeMismatch:
        case Subscriber::RunError::PublisherDisconnectedAttachmentMissing:
        case Subscriber::RunError::PublisherDisconnectedEventFdMissing:
            return Subscriber::SubscribeError::BrokerUnavailable;
        case Subscriber::RunError::DataAvailableEventAttachmentMissing:
        case Subscriber::RunError::DataAvailableEventReadFailed:
        case Subscriber::RunError::WrappedDescriptorReadFailed:
        case Subscriber::RunError::ContiguousMessageConsumptionFailed:
        case Subscriber::RunError::ShutdownEventReadFailed:
        case Subscriber::RunError::WorkerException:
            return Subscriber::SubscribeError::DataPathFailed;
    }
    return Subscriber::SubscribeError::DataPathFailed;
}

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

std::optional<Subscriber::CreateError> send_subscriber_topic_registration(
    const int broker_fd,
    std::span<const TopicId> topic_ids) {
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

    const ssize_t registration_sent_size = send(broker_fd, packet, sizeof(packet), MSG_NOSIGNAL);
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

Subscriber::CreateResult Subscriber::create(std::span<const TopicId> topic_ids, Flags flags, std::function<void(char *, size_t)> callback) {
    Logger::init(subscriber_log_path());

    const Flags execution_mode_flags = flags & (SYNCHRONOUS | ASYNCHRONOUS);
    if (execution_mode_flags != SYNCHRONOUS && execution_mode_flags != ASYNCHRONOUS) {
        LOG_ERROR("SubscriberModeFlagsInvalidError: flags=%u mode_flags=%u", flags, execution_mode_flags);
        return CreateResult{CreateError::SubscriberModeFlagsInvalid};
    }
    if (execution_mode_flags == ASYNCHRONOUS && !callback) {
        LOG_ERROR("SubscriberAsyncCallbackMissingError");
        return CreateResult{CreateError::AsyncCallbackMissing};
    }

    int epoll_fd = epoll_create1(0);
    FdGuard epoll_guard(epoll_fd);
    if (epoll_fd < 0) {
        LOG_ERROR("SubscriberEpollCreateError: errno=%d (%s)", errno, strerror(errno));
        return CreateResult{CreateError::EpollCreateFailed};
    }

    auto connect_result = connect_to_broker();
    auto *connect_error = std::get_if<CreateError>(&connect_result);
    if (connect_error != nullptr) {
        return CreateResult{*connect_error};
    }

    int broker_fd = get<int>(connect_result);
    FdGuard broker_guard(broker_fd);

    auto registration_result = send_subscriber_topic_registration(broker_fd, topic_ids);
    if (registration_result.has_value()) {
        LOG_ERROR("SubscriberTopicRegistrationCreateError: topic_count=%zu", topic_ids.size());
        return CreateResult{*registration_result};
    }
    if (!add_fd_to_epoll(epoll_fd, broker_fd, kBrokerEventId)) {
        LOG_ERROR("SubscriberBrokerEpollAddError: epoll_fd=%d broker_fd=%d errno=%d (%s)",
            epoll_fd,
            broker_fd,
            errno,
            strerror(errno));
        return CreateResult{CreateError::BrokerEpollRegistrationFailed};
    }

    int shutdown_event_fd = -1;
    FdGuard shutdown_guard(shutdown_event_fd);
    int worker_exit_event_fd = -1;
    FdGuard worker_exit_guard(worker_exit_event_fd);
    if (execution_mode_flags == ASYNCHRONOUS) {
        shutdown_event_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (shutdown_event_fd < 0) {
            LOG_ERROR("SubscriberShutdownEventFdCreateError: errno=%d (%s)", errno, strerror(errno));
            return CreateResult{CreateError::ShutdownEventFdCreateFailed};
        }
        if (!add_fd_to_epoll(epoll_fd, shutdown_event_fd, kShutdownEventId)) {
            LOG_ERROR("SubscriberShutdownEpollRegistrationError: epoll_fd=%d event_fd=%d errno=%d (%s)",
                epoll_fd,
                shutdown_event_fd,
                errno,
                strerror(errno));
            return CreateResult{CreateError::ShutdownEpollRegistrationFailed};
        }

        worker_exit_event_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (worker_exit_event_fd < 0) {
            LOG_ERROR("SubscriberWorkerExitEventFdCreateError: errno=%d (%s)", errno, strerror(errno));
            return CreateResult{CreateError::WorkerExitEventFdCreateFailed};
        }
    }

    InitialResources resources{
        .epoll_fd = epoll_fd,
        .broker_fd = broker_fd,
        .shutdown_event_fd = shutdown_event_fd,
        .worker_exit_event_fd = worker_exit_event_fd,
    };
    Subscriber subscriber(resources, flags, std::move(callback));
    epoll_guard.release();
    broker_guard.release();
    shutdown_guard.release();
    worker_exit_guard.release();

    try {
        CreateResult result{std::move(subscriber)};
        Subscriber &created_subscriber = std::get<Subscriber>(result.value);
        if (execution_mode_flags == ASYNCHRONOUS) {
            created_subscriber.start_run_thread();
        }
        return result;
    } catch (const TransferEventDrainError &) {
        return CreateResult{CreateError::TransferEventDrainFailed};
    } catch (const std::system_error &error) {
        LOG_ERROR("SubscriberRunThreadCreateError: %s", error.what());
        return CreateResult{CreateError::RunThreadCreateFailed};
    }
}

// 移动/析构时不得并发调用成员函数，也不得从本对象的回调中执行。
// 移动可能因通知清理或 worker 重启失败抛出 std::system_error（含派生异常）。
// 内存分配失败也可能抛出 std::bad_alloc，因此移动操作不保证 noexcept。
Subscriber::Subscriber(Subscriber &&other) {
    take_resources_from(std::move(other));
}

Subscriber &Subscriber::operator=(Subscriber &&other) {
    if (this != &other) {
        release_resources();
        take_resources_from(std::move(other));
    }
    return *this;
}

Subscriber::~Subscriber() {
    release_resources();
}

Subscriber::Subscriber(
    InitialResources resources,
    Flags flags,
    std::function<void(char *, size_t)> callback)
    : flags_(flags),
      epoll_fd_(resources.epoll_fd),
      broker_fd_(resources.broker_fd),
      shutdown_event_fd_(resources.shutdown_event_fd),
      worker_exit_event_fd_(resources.worker_exit_event_fd),
      callback_(std::move(callback)) {}

void Subscriber::take_resources_from(Subscriber &&other) {
    const bool restart_run_thread = other.stop_run_thread();
    if (restart_run_thread) {
        for (const int event_fd : {other.shutdown_event_fd_, other.worker_exit_event_fd_}) {
            if (!drain_eventfd(event_fd)) {
                const int saved_errno = errno;
                LOG_ERROR("SubscriberTransferEventDrainError: event_fd=%d errno=%d (%s)",
                    event_fd,
                    saved_errno,
                    strerror(saved_errno));
                throw TransferEventDrainError(saved_errno);
            }
        }
    }

    flags_ = std::exchange(other.flags_, SYNCHRONOUS);

    epoll_fd_ = std::exchange(other.epoll_fd_, -1);
    broker_fd_ = std::exchange(other.broker_fd_, -1);
    shutdown_event_fd_ = std::exchange(other.shutdown_event_fd_, -1);
    worker_exit_event_fd_ = std::exchange(other.worker_exit_event_fd_, -1);
    worker_exit_promise_ = std::move(other.worker_exit_promise_);
    worker_exit_future_ = std::move(other.worker_exit_future_);
    callback_ = std::move(other.callback_);

    std::copy_n(other.events_, kMaxEvents, events_);
    event_count_ = std::exchange(other.event_count_, 0);
    event_index_ = std::exchange(other.event_index_, 0);

    publisher_attachment_by_data_available_event_fd_.clear();
    publisher_attachment_by_data_available_event_fd_.swap(
        other.publisher_attachment_by_data_available_event_fd_);

    data_available_event_fd_by_attachment_id_.clear();
    data_available_event_fd_by_attachment_id_.swap(
        other.data_available_event_fd_by_attachment_id_);

    try {
        if (restart_run_thread) {
            start_run_thread();
        }
    } catch (...) {
        release_resources();
        throw;
    }
}

void Subscriber::release_resources() {
    stop_run_thread();

    for (auto &[event_fd, attachment] : publisher_attachment_by_data_available_event_fd_) {
        release_publisher_attachment(attachment);
    }
    publisher_attachment_by_data_available_event_fd_.clear();
    data_available_event_fd_by_attachment_id_.clear();

    close_fd(shutdown_event_fd_);
    close_fd(worker_exit_event_fd_);
    close_fd(epoll_fd_);
    close_fd(broker_fd_);
    event_count_ = 0;
    event_index_ = 0;
}

void Subscriber::reset_worker_exit_notification() {
    worker_exit_promise_ = std::promise<RunError>{};
    worker_exit_future_ = worker_exit_promise_.get_future().share();
}

void Subscriber::start_run_thread() {
    reset_worker_exit_notification();
    run_thread_active_.store(true, std::memory_order_relaxed);
    try {
        run_thread_ = std::thread([this] {
            RunError reason = RunError::None;
            try {
                reason = run();
            } catch (const std::exception &error) {
                reason = RunError::WorkerException;
                LOG_ERROR("SubscriberWorkerException: %s", error.what());
            } catch (...) {
                reason = RunError::WorkerException;
                LOG_ERROR("SubscriberWorkerException: unknown exception");
            }
            worker_exit_promise_.set_value(reason);

            if (!signal_eventfd(worker_exit_event_fd_)) {
                LOG_ERROR("SubscriberWorkerExitSignalError: event_fd=%d errno=%d (%s)",
                    worker_exit_event_fd_,
                    errno,
                    strerror(errno));
            }
            run_thread_active_.store(false, std::memory_order_relaxed);
        });
    } catch (...) {
        run_thread_active_.store(false, std::memory_order_relaxed);
        throw;
    }
}

bool Subscriber::stop_run_thread() {
    if (!run_thread_.joinable()) {
        return false;
    }

    const bool was_active = run_thread_active_.load(std::memory_order_relaxed);
    if (was_active) {
        if (!signal_eventfd(shutdown_event_fd_)) {
            LOG_ERROR("SubscriberShutdownEventWriteError: event_fd=%d errno=%d (%s)",
                shutdown_event_fd_,
                errno,
                strerror(errno));
        }
    }

    run_thread_.join();
    return was_active &&
        worker_exit_future_.valid() &&
        worker_exit_future_.get() == RunError::None;
}

void Subscriber::release_publisher_attachment(PublisherAttachment &attachment) {
    if (attachment.data_available_event_fd >= 0)
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, attachment.data_available_event_fd, nullptr);

    if (attachment.shared_data != nullptr && attachment.shared_data != MAP_FAILED)
        munmap(attachment.shared_data, sizeof(SharedData));
    attachment.shared_data = nullptr;

    close_fd(attachment.shm_fd);
    close_fd(attachment.space_available_event_fd);
    close_fd(attachment.data_available_event_fd);
}

std::variant<Subscriber::MessageBatch, Subscriber::RunError> Subscriber::read_data(
    PublisherAttachment &publisher_attachment,
    MessageDescriptor desc_ring[],
    const size_t descriptor_count) {
    SharedData *p = publisher_attachment.shared_data;
    MessageBatch messages;

    for (size_t descriptor_index = 0; descriptor_index < descriptor_count; ++descriptor_index) {
        uint32_t offset = desc_ring[descriptor_index].offset;
        uint32_t message_size_bytes = desc_ring[descriptor_index].len;

        int size_class = find_size_class(message_size_bytes);
        if (size_class < 0) {
            LOG_FATAL("SubscriberMessageSizeClassError: seq=%d, descriptor=%zu, size=%u",
                publisher_attachment.expected_seq,
                descriptor_index,
                message_size_bytes);
            return RunError::ContiguousMessageConsumptionFailed;
        }

#ifdef ENABLE_DEBUG_CHECKS
        if (offset < kFirstOffset[size_class] || offset > kLastOffset[size_class]) {
            LOG_FATAL("SubscriberChunkOffsetError: class=%d, offset=%u", size_class, offset);
            return RunError::ContiguousMessageConsumptionFailed;
        }
#endif

        if (message_size_bytes < 2 * sizeof(int)) {
            LOG_FATAL("SubscriberMessageHeaderSizeError: seq=%d, descriptor=%zu, size=%u, minimum=%zu",
                publisher_attachment.expected_seq,
                descriptor_index,
                message_size_bytes,
                2 * sizeof(int));
            return RunError::ContiguousMessageConsumptionFailed;
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
        if (is_error) return RunError::ContiguousMessageConsumptionFailed;

        if (flags_ & SYNCHRONOUS) {
            auto bytes = std::as_bytes(
                std::span{p->data + offset + 2 * sizeof(int), message_size_bytes - 2 * sizeof(int)});
            messages.emplace_back(bytes.begin(), bytes.end());
        } else {
            callback_(p->data + offset + 2 * sizeof(int), message_size_bytes - 2 * sizeof(int));
        }

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
                    return RunError::ContiguousMessageConsumptionFailed;
                }
                p->chunk_usage_tracker.is_in_use[size_class][chunk_index] = false;

                pthread_mutex_unlock(&p->chunk_usage_tracker.mutex);
            }
#endif

            uint32_t last_tail_off = p->tail.offset[size_class].load(std::memory_order_relaxed);
            memcpy(p->data + last_tail_off, &offset, sizeof(uint32_t));
            p->tail.offset[size_class].store(offset, std::memory_order_release);

            if (!signal_eventfd(publisher_attachment.space_available_event_fd)) {
                LOG_ERROR("SubscriberSpaceAvailableSignalError: event_fd=%d errno=%d (%s)",
                    publisher_attachment.space_available_event_fd,
                    errno,
                    strerror(errno));
                return RunError::ContiguousMessageConsumptionFailed;
            }
        }
    }
    return messages;
}

std::variant<Subscriber::MessageBatch, Subscriber::RunError> Subscriber::consume_contiguous_messages(
    PublisherAttachment &publisher_attachment) {
    SharedData *p = publisher_attachment.shared_data;
    MessageBatch messages;

    uint32_t wr = p->descriptor_write_index.load(std::memory_order_acquire);
    while (wr > publisher_attachment.subscriber_read_index) {
        std::vector<MessageDescriptor> desc_ring(p->desc_ring + publisher_attachment.subscriber_read_index, p->desc_ring + wr);

        auto read_result = read_data(publisher_attachment, desc_ring.data(), desc_ring.size());
        if (const auto *read_error = std::get_if<RunError>(&read_result)) {
            return *read_error;
        }
        auto &read_messages = std::get<MessageBatch>(read_result);
        if (messages.empty()) {
            messages = std::move(read_messages);
        } else {
            for (Message &message : read_messages) {
                messages.emplace_back(std::move(message));
            }
        }
        wr = p->descriptor_write_index.load(std::memory_order_acquire);
    }
    return messages;
}

void Subscriber::initialize_subscriber_read_index(PublisherAttachment &publisher_attachment) {
    if (publisher_attachment.subscriber_read_index == kInvalidIndex)
        publisher_attachment.subscriber_read_index = publisher_attachment.shared_data
                                                         ->descriptor_read_indices[publisher_attachment.slot_index]
                                                         .load(std::memory_order_relaxed);
}

std::variant<Subscriber::MessageBatch, Subscriber::RunError> Subscriber::handle_data_available_event(
    const int event_fd) {
    auto publisher_attachment_it = publisher_attachment_by_data_available_event_fd_.find(event_fd);
    if (publisher_attachment_it == publisher_attachment_by_data_available_event_fd_.end()) {
        LOG_FATAL("SubscriberEventFdAttachmentMissingError: data_available_event_fd=%d", event_fd);
        return RunError::DataAvailableEventAttachmentMissing;
    }
    PublisherAttachment &publisher_attachment = publisher_attachment_it->second;
    MessageBatch messages;

    if (!drain_eventfd(event_fd)) {
        LOG_ERROR("SubscriberDataAvailableEventReadError: event_fd=%d errno=%d (%s)",
            event_fd,
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

        auto read_result = read_data(publisher_attachment, desc_ring.data(), descriptor_count);
        if (std::holds_alternative<RunError>(read_result)) {
            return RunError::WrappedDescriptorReadFailed;
        }
        messages = std::get<MessageBatch>(std::move(read_result));
    }

    auto consume_result = consume_contiguous_messages(publisher_attachment);
    if (const auto *consume_error = std::get_if<RunError>(&consume_result)) {
        return *consume_error;
    }
    auto &consume_messages = std::get<MessageBatch>(consume_result);
    if (messages.empty()) {
        messages = std::move(consume_messages);
    } else {
        for (Message &message : consume_messages) {
            messages.emplace_back(std::move(message));
        }
    }

    LOG_DEBUG("subscriber event processed: data_available_event_fd=%d", event_fd);
    return messages;
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

        if (received_size < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            } else {
                LOG_ERROR("broker control recv failed: fd=%d errno=%d (%s)", broker_fd_, errno, strerror(errno));
                return false;
            }
        }
        ReceivedFdsGuard received_fds(msg);
        // install_subscriber_attachment_batch insert/emplace 分配内存时可能抛出 std::bad_alloc
        // 内存问题应及时反馈给上层，所以选择守护类关闭剩余资源

        if (received_size == 0) {
            LOG_ERROR("broker control connection closed: broker_fd=%d", broker_fd_);
            return false;
        }
        if (msg.msg_flags & (MSG_CTRUNC | MSG_TRUNC)) {
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
                if (handle_publisher_disconnected(packet, body_len) != RunError::None) return false;
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

    int *fds = reinterpret_cast<int *>(CMSG_DATA(cmsg));
    // SCM_RIGHTS 的控制数据按连续的 int 文件描述符数组存放，
    // 接管 FD 时将对应项置为 -1，接收守卫只关闭尚未交接的项。

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

        attachment.shm_fd = std::exchange(fds[3 * attachment_index], -1);
        attachment.space_available_event_fd = std::exchange(fds[3 * attachment_index + 1], -1);
        attachment.data_available_event_fd = std::exchange(fds[3 * attachment_index + 2], -1);

        bool attachment_id_inserted = false;
        try {
            attachment.shared_data = static_cast<SharedData *>(mmap(nullptr, sizeof(SharedData), PROT_READ | PROT_WRITE, MAP_SHARED, attachment.shm_fd, 0));

            if (attachment.shared_data == MAP_FAILED) {
                LOG_ERROR("SubscriberAttachmentBatchMmapError");
                release_publisher_attachment(attachment);
                return false;
            }

            if (!add_fd_to_epoll(epoll_fd_, attachment.data_available_event_fd, attachment_id)) {
                LOG_ERROR("SubscriberAttachmentEpollAddError: epoll_fd=%d event_fd=%d errno=%d (%s)",
                    epoll_fd_,
                    attachment.data_available_event_fd,
                    errno,
                    strerror(errno));
                release_publisher_attachment(attachment);
                return false;
            }

            auto attachment_id_result =
                data_available_event_fd_by_attachment_id_.insert({attachment_id, attachment.data_available_event_fd});
            attachment_id_inserted = attachment_id_result.second;

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
                data_available_event_fd_by_attachment_id_.erase(attachment_id_result.first);
                release_publisher_attachment(attachment);
                return false;
            }

            publisher_attachment_by_data_available_event_fd_.emplace(
                attachment.data_available_event_fd,
                std::move(attachment));
        } catch (...) {
            if (attachment_id_inserted) {
                data_available_event_fd_by_attachment_id_.erase(attachment_id);
            }
            release_publisher_attachment(attachment);
            throw;
        }
    }

    LOG_INFO("subscriber attachment batch installed: count=%zu",
        attachment_count);
    return true;
}

Subscriber::RunError Subscriber::handle_publisher_disconnected(const char packet[], size_t body_len) {
    if (body_len != sizeof(AttachmentId)) {
        LOG_FATAL("PublisherDisconnectedPayloadSizeError: bytes=%zu", body_len);
        return RunError::PublisherDisconnectedPayloadSizeMismatch;
    }

    AttachmentId attachment_id = kInvalidAttachmentId;
    memcpy(&attachment_id, packet + sizeof(BrokerMessageType), sizeof(attachment_id));

    auto data_available_event_fd_it = data_available_event_fd_by_attachment_id_.find(attachment_id);
    if (data_available_event_fd_it == data_available_event_fd_by_attachment_id_.end()) {
        LOG_FATAL("PublisherDisconnectedAttachmentMissingError: attachment_id=%llu",
            static_cast<unsigned long long>(attachment_id));
        return RunError::PublisherDisconnectedAttachmentMissing;
    }

    int data_available_event_fd = data_available_event_fd_it->second;

    auto publisher_attachment_it = publisher_attachment_by_data_available_event_fd_.find(data_available_event_fd);
    if (publisher_attachment_it == publisher_attachment_by_data_available_event_fd_.end()) {
        LOG_FATAL("PublisherDisconnectedEventFdMissingError: attachment_id=%llu data_available_event_fd=%d",
            static_cast<unsigned long long>(attachment_id),
            data_available_event_fd);
        return RunError::PublisherDisconnectedEventFdMissing;
    }

    release_publisher_attachment(publisher_attachment_it->second);

    data_available_event_fd_by_attachment_id_.erase(data_available_event_fd_it);
    publisher_attachment_by_data_available_event_fd_.erase(publisher_attachment_it);
    return RunError::None;
}

Subscriber::SubscribeResult Subscriber::subscribe(std::chrono::microseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;

    while (true) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return SubscribeError::Timeout;
        }

        if (event_index_ >= event_count_) {
            const auto remaining = std::chrono::duration_cast<std::chrono::nanoseconds>(
                deadline - std::chrono::steady_clock::now());
            if (remaining.count() < 0) {
                return SubscribeError::Timeout;
            }

            const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(remaining);

            const timespec wait_timeout{
                .tv_sec = static_cast<time_t>(seconds.count()),
                .tv_nsec = static_cast<long>((remaining - seconds).count()),
            };

            event_count_ = epoll_pwait2(epoll_fd_, events_, kMaxEvents, &wait_timeout, nullptr);
            if (event_count_ < 0) {
                if (errno == EINTR) {
                    continue;
                }
                LOG_ERROR("SubscriberEpollWaitError: errno=%d (%s)", errno, strerror(errno));
                return SubscribeError::EventWaitFailed;
            }
            if (event_count_ == 0) {
                return SubscribeError::Timeout;
            }
            LOG_DEBUG("epoll event_count=%d", event_count_);

            event_index_ = 0;
            continue;
        }

        uint64_t event_id = events_[event_index_++].data.u64;

        if (event_id == kBrokerEventId) {
            if (!process_broker_control_messages()) {
                return SubscribeError::BrokerUnavailable;
            }
            continue;
        }

        auto it = data_available_event_fd_by_attachment_id_.find(event_id);
        if (it == data_available_event_fd_by_attachment_id_.end()) {
            continue;
        }
        int fd = it->second;

        auto event_result = handle_data_available_event(fd);
        if (const auto *event_error = std::get_if<RunError>(&event_result)) {
            return map_run_error_to_subscribe_error(*event_error);
        }

        auto messages = std::get<MessageBatch>(std::move(event_result));
        if (messages.empty()) continue;

        return messages;
    }
}

Subscriber::RunError Subscriber::run() {
    RunError reason = RunError::None;
    bool running = true;

    while (running && reason == RunError::None) {
        if (event_index_ >= event_count_) {
            event_index_ = 0;
            event_count_ = epoll_wait(epoll_fd_, events_, kMaxEvents, -1);
            if (event_count_ < 0) {
                LOG_ERROR("SubscriberEpollWaitError: errno=%d (%s)", errno, strerror(errno));
                reason = RunError::EpollWaitFailed;
                break;
            }
            if (event_count_ == 0) {
                LOG_ERROR("SubscriberUnexpectedEpollTimeoutError");
                reason = RunError::UnexpectedEpollTimeout;
                break;
            }
            LOG_DEBUG("epoll event_count=%d", event_count_);
        }

        while (event_index_ < event_count_) {
            const uint64_t event_id = events_[event_index_++].data.u64;
            if (event_id == kShutdownEventId) {
                if (!drain_eventfd(shutdown_event_fd_)) {
                    LOG_ERROR("SubscriberShutdownEventReadError: event_fd=%d errno=%d (%s)",
                        shutdown_event_fd_,
                        errno,
                        strerror(errno));
                    reason = RunError::ShutdownEventReadFailed;
                }
                running = false;
                break;
            }

            if (event_id == kBrokerEventId) {
                if (!process_broker_control_messages()) {
                    reason = RunError::BrokerControlMessageProcessingFailed;
                    running = false;
                    break;
                }
                continue;
            }

            auto it = data_available_event_fd_by_attachment_id_.find(event_id);
            if (it == data_available_event_fd_by_attachment_id_.end()) {
                continue;
            }

            auto event_result = handle_data_available_event(it->second);
            if (const auto *event_error = std::get_if<RunError>(&event_result)) {
                reason = *event_error;
                running = false;
                break;
            }
        }
    }

#ifdef ENABLE_DEBUG_CHECKS
    log_chunks_in_use();
#endif
    return reason;
}

#ifdef ENABLE_DEBUG_CHECKS
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
#endif
