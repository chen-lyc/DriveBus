#include "../include/publisher.hpp"
#include "../include/logger.h"
#include "../include/fd_helpers.hpp"
#include "../include/shared_memory_layout_helpers.hpp"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <exception>
#include <fcntl.h>
#include <filesystem>
#include <optional>
#include <system_error>
#include <utility>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/un.h>

namespace {
std::filesystem::path publisher_log_path() {
    const char *run_id_env = std::getenv("DRIVEBUS_RUN_ID");
    const std::string run_id = run_id_env != nullptr && *run_id_env != '\0' ? run_id_env : "standalone";
    const auto start_us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    return std::filesystem::path("logs") /
        run_id /
        "publisher" /
        ("pid-" + std::to_string(getpid()) + "-start-" + std::to_string(start_us) + ".log");
}

Publisher::PublishError map_sync_run_error_to_publish_error(const Publisher::RunError run_error) {
    assert(run_error != Publisher::RunError::None);
    switch (run_error) {
        case Publisher::RunError::None:
            std::terminate();
        case Publisher::RunError::SpaceAvailableEventReadFailed:
            return Publisher::PublishError::EventWaitFailed;
        case Publisher::RunError::BrokerClosed:
        case Publisher::RunError::BrokerControlMessageTooShort:
        case Publisher::RunError::BrokerControlMessageProcessingFailed:
        case Publisher::RunError::DynamicSubscriberRegistrationFailed:
        case Publisher::RunError::ChunkReferenceReleaseFailed:
        case Publisher::RunError::DeadSubscriberReapFailed:
            return Publisher::PublishError::BrokerUnavailable;
        case Publisher::RunError::EpollWaitFailed:
        case Publisher::RunError::UnexpectedEpollTimeout:
        case Publisher::RunError::PublishWorkEventReadFailed:
        case Publisher::RunError::MessageSizeClassInvalid:
        case Publisher::RunError::PublishAttemptFailed:
        case Publisher::RunError::PublishWorkRescheduleFailed:
        case Publisher::RunError::PublishWorkResumeFailed:
        case Publisher::RunError::PublishWorkBrokerResumeFailed:
        case Publisher::RunError::DataAvailableSignalFailed:
        case Publisher::RunError::ShutdownEventReadFailed:
        case Publisher::RunError::WorkerException:
            return Publisher::PublishError::DataPathFailed;
    }
    return Publisher::PublishError::DataPathFailed;
}

void initialize_shared_data(SharedData &shared_data) {
    for (size_t subscriber_slot_index = 0; subscriber_slot_index < kMaxSubscribers; ++subscriber_slot_index) {
        shared_data.descriptor_read_indices[subscriber_slot_index].store(kInvalidIndex, std::memory_order_relaxed);
    }
    shared_data.descriptor_write_index.store(0, std::memory_order_relaxed);

    shared_data.head.offset[0].store(0, std::memory_order_relaxed);
    for (int i = 1; i < kClassCount; ++i) {
        shared_data.head.offset[i].store(shared_data.head.offset[i - 1].load(std::memory_order_relaxed) + kClassSizeBytes[i - 1] * kChunkCountBySizeClass[i - 1], std::memory_order_relaxed);
    }

    for (int i = 0; i < kClassCount - 1; ++i) {
        shared_data.tail.offset[i].store(shared_data.head.offset[i + 1].load(std::memory_order_relaxed) - kClassSizeBytes[i], std::memory_order_relaxed);
    }
    shared_data.tail.offset[kClassCount - 1].store(sizeof(shared_data.data) - kClassSizeBytes[kClassCount - 1], std::memory_order_relaxed);

    for (uint32_t chunk_index = 0; chunk_index < kTotalChunkCount; ++chunk_index) {
        shared_data.chunk_reference_counts[chunk_index].store(0, std::memory_order_relaxed);
    }

    auto init = [&](uint32_t chunk_size_bytes, const uint32_t chunk_count, uint32_t offset) {
        for (uint32_t i = 0; i < chunk_count - 1; ++i) {
            uint32_t next_offset = offset + chunk_size_bytes;
            memcpy(shared_data.data + offset, &next_offset, sizeof(uint32_t));
            offset = next_offset;
        }
        memcpy(shared_data.data + offset, &kInvalidOffset, sizeof(uint32_t));
    };
    for (int i = 0; i < kClassCount; ++i) {
        init(kClassSizeBytes[i], kChunkCountBySizeClass[i], shared_data.head.offset[i]);
    }
}

void initialize_chunk_usage_tracker(ChunkUsageTracker &tracker) {
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
    pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
    pthread_mutex_init(&tracker.mutex, &attr);
    pthread_mutexattr_destroy(&attr);

    memset(tracker.is_in_use, 0, sizeof(tracker.is_in_use));
}

struct SharedDataMapping {
    int shm_fd;
    SharedData *shared_data;
};

std::variant<SharedDataMapping, Publisher::CreateError> create_and_map_shared_data(const char *shm_name) {
    if (shm_unlink(shm_name) < 0 && errno != ENOENT) {
        LOG_ERROR("PublisherShmUnlinkError: name=%s errno=%d (%s)", shm_name, errno, strerror(errno));
        return Publisher::CreateError::SharedMemoryUnlinkFailed;
    }

    int shm_fd = shm_open(shm_name, O_CREAT | O_RDWR, 0666);
    if (shm_fd < 0) {
        LOG_ERROR("PublisherShmOpenError: name=%s errno=%d (%s)", shm_name, errno, strerror(errno));
        return Publisher::CreateError::SharedMemoryOpenFailed;
    }

    auto cleanup_shm = [](const char *shm_name) {
        shm_unlink(shm_name);
    };
    std::unique_ptr<const char, decltype(cleanup_shm)> shm_guard(shm_name, cleanup_shm);

    if (ftruncate(shm_fd, sizeof(SharedData)) < 0) {
        LOG_ERROR("PublisherShmResizeError: fd=%d bytes=%zu errno=%d (%s)",
            shm_fd,
            sizeof(SharedData),
            errno,
            strerror(errno));
        close_fd(shm_fd);
        return Publisher::CreateError::SharedMemoryResizeFailed;
    }

    SharedData *shared_data = static_cast<SharedData *>(mmap(nullptr, sizeof(SharedData), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0));
    if (shared_data == MAP_FAILED) {
        LOG_ERROR("PublisherShmMapError: fd=%d bytes=%zu errno=%d (%s)",
            shm_fd,
            sizeof(SharedData),
            errno,
            strerror(errno));
        close_fd(shm_fd);
        return Publisher::CreateError::SharedMemoryMapFailed;
    }

    initialize_shared_data(*shared_data);
#ifdef ENABLE_DEBUG_CHECKS
    initialize_chunk_usage_tracker(shared_data->chunk_usage_tracker);
#endif

    // 成功返回不进行 cleanup
    // NOLINTNEXTLINE(bugprone-unused-return-value)
    shm_guard.release();
    return SharedDataMapping{shm_fd, shared_data};
}

std::variant<int, Publisher::CreateError> connect_to_broker() {
    const std::string path = "/tmp/broker.sock";

    int broker_fd = socket(AF_UNIX, SOCK_SEQPACKET, 0);
    if (broker_fd < 0) {
        LOG_ERROR("PublisherBrokerSocketCreateError: errno=%d (%s)", errno, strerror(errno));
        return Publisher::CreateError::BrokerSocketCreateFailed;
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
        LOG_FATAL("PublisherBrokerConnectError: attempts=%d, errno=%d (%s)",
            retry_count,
            errno,
            strerror(errno));
        close_fd(broker_fd);
        return Publisher::CreateError::BrokerConnectFailed;
    }

    return broker_fd;
}

std::optional<Publisher::CreateError> send_publisher_topic_registration(int broker_fd, TopicId topic_id, int shm_fd, int space_available_event_fd) {
    char packet[kPublisherTopicRegistrationMessageSize];

    BrokerMessageType message_type = BrokerMessageType::PublisherTopicRegistration;
    memcpy(packet, &message_type, sizeof(BrokerMessageType));

    memcpy(packet + sizeof(BrokerMessageType) / sizeof(char), &topic_id, sizeof(topic_id));

    const ssize_t registration_sent_size =
        send_packet_with_fds(broker_fd, packet, sizeof(packet), {shm_fd, space_available_event_fd});
    if (registration_sent_size != static_cast<ssize_t>(sizeof(packet))) {
        LOG_ERROR("PublisherRegistrationSendError: expected=%zu actual=%zd errno=%d (%s)",
            sizeof(packet),
            registration_sent_size,
            errno,
            strerror(errno));
        return Publisher::CreateError::TopicRegistrationSendFailed;
    }
    return std::nullopt;
}

} // namespace

std::variant<Publisher::SubscriberRegistration, Publisher::CreateError>
Publisher::receive_subscriber_registration() {
    int fd = -1;
    FdGuard fd_guard(fd);
    char packet[kSubscriberEventFdAndSlotMessageSize];
    ssize_t received_size = receive_packet_with_fd(broker_fd_, packet, sizeof(packet), fd);
    if (received_size != static_cast<ssize_t>(sizeof(packet))) {
        if (received_size < 0) {
            LOG_ERROR("InitialSubscriberRegistrationReceiveError: broker_fd=%d errno=%d (%s)",
                broker_fd_,
                errno,
                strerror(errno));
        } else {
            LOG_ERROR("InitialSubscriberRegistrationReceiveSizeError: broker_fd=%d expected=%zu actual=%zd",
                broker_fd_,
                sizeof(packet),
                received_size);
        }
        return CreateError::InitialSubscriberRegistrationReceiveFailed;
    }
    if (fd < 0) {
        LOG_ERROR("InitialSubscriberRegistrationEventFdMissingError: broker_fd=%d", broker_fd_);
        return CreateError::InitialSubscriberRegistrationEventFdMissing;
    }

    BrokerMessageType message_type = static_cast<BrokerMessageType>(packet[0]);
    if (message_type != BrokerMessageType::SubscriberEventFdAndSlot) {
        LOG_ERROR("InitialSubscriberRegistrationTypeError: expected=%u actual=%u bytes=%zd broker_fd=%d",
            static_cast<unsigned>(BrokerMessageType::SubscriberEventFdAndSlot),
            static_cast<unsigned>(static_cast<unsigned char>(packet[0])),
            received_size,
            broker_fd_);
        return CreateError::InitialSubscriberRegistrationTypeInvalid;
    }
    uint32_t received_subscriber_slot_index;
    memcpy(&received_subscriber_slot_index, packet + 1, sizeof(received_subscriber_slot_index));

    return SubscriberRegistration{fd_guard.release(), received_subscriber_slot_index};
}

std::optional<Publisher::SubscriberTrackingError>
Publisher::track_subscriber(SubscriberRegistration registration) {
    FdGuard registration_fd(registration.data_available_event_fd);
    if (registration.slot_index >= kMaxSubscribers) {
        LOG_ERROR("PublisherSubscriberSlotRangeError: slot=%u max=%zu data_available_event_fd=%d",
            registration.slot_index,
            kMaxSubscribers,
            registration.data_available_event_fd);
        return SubscriberTrackingError::SlotOutOfRange;
    }
    if (shared_data_->descriptor_read_indices[registration.slot_index].load(std::memory_order_relaxed) != kInvalidIndex) {
        LOG_ERROR("PublisherSubscriberSlotAlreadyTrackedError: slot=%u data_available_event_fd=%d",
            registration.slot_index,
            registration.data_available_event_fd);
        return SubscriberTrackingError::SlotAlreadyTracked;
    }

    const auto [event_fd_it, event_fd_inserted] =
        subscriber_slot_index_by_data_available_event_fd_
            .insert({registration.data_available_event_fd, registration.slot_index});
    if (!event_fd_inserted) {
        LOG_ERROR("PublisherDataAvailableEventFdAlreadyTrackedError: event_fd=%d slot=%u",
            registration.data_available_event_fd,
            registration.slot_index);
        return SubscriberTrackingError::EventFdAlreadyTracked;
    }
    try {
        data_available_event_fds_.emplace_back(registration.data_available_event_fd);
    } catch (...) {
        subscriber_slot_index_by_data_available_event_fd_.erase(event_fd_it);
        throw;
    }
    data_available_event_fd_by_subscriber_slot_index_[registration.slot_index] = registration.data_available_event_fd;

    shared_data_->descriptor_read_indices[registration.slot_index].store(
        shared_data_->descriptor_write_index.load(std::memory_order_relaxed),
        std::memory_order_relaxed);
    registration_fd.release();
    return std::nullopt;
}

std::optional<Publisher::CreateError> Publisher::receive_and_track_initial_subscriber_registrations() {
    uint32_t subscriber_count;
    {
        ssize_t n = recv(broker_fd_, &subscriber_count, sizeof(subscriber_count), 0);
        if (n != static_cast<ssize_t>(sizeof(subscriber_count))) {
            if (n < 0) {
                LOG_ERROR("InitialSubscriberCountReceiveError: broker_fd=%d errno=%d (%s)",
                    broker_fd_,
                    errno,
                    strerror(errno));
            } else {
                LOG_ERROR("InitialSubscriberCountReceiveSizeError: broker_fd=%d expected=%zu actual=%zd",
                    broker_fd_,
                    sizeof(subscriber_count),
                    n);
            }
            return Publisher::CreateError::InitialSubscriberCountReceiveFailed;
        }
        LOG_INFO("publisher registered: initial_subscriber_count=%u", subscriber_count);
    }

    auto close_initial_subscriber_event_fds = [&]() {
        for (int &event_fd : data_available_event_fds_) {
            close_fd(event_fd);
        }
    };

    for (size_t i = 0; i < subscriber_count; ++i) {
        auto registration_result = receive_subscriber_registration();
        if (auto *registration_error = std::get_if<CreateError>(&registration_result)) {
            close_initial_subscriber_event_fds();
            return *registration_error;
        }

        auto tracking_error = track_subscriber(std::get<SubscriberRegistration>(registration_result));
        if (tracking_error.has_value()) {
            close_initial_subscriber_event_fds();
            switch (*tracking_error) {
                case SubscriberTrackingError::SlotOutOfRange:
                    return CreateError::SubscriberSlotOutOfRange;
                case SubscriberTrackingError::SlotAlreadyTracked:
                    return CreateError::SubscriberSlotAlreadyTracked;
                case SubscriberTrackingError::EventFdAlreadyTracked:
                    return CreateError::SubscriberEventFdAlreadyTracked;
            }
        }
    }
    return std::nullopt;
}

Publisher::Publisher(InitialResources resources, const Flags publisher_flags)
    : flags_(publisher_flags),
      shm_name_(std::move(resources.shm_name)),
      shared_data_(resources.shared_data),
      broker_fd_(resources.broker_fd),
      epoll_fd_(resources.epoll_fd),
      space_available_event_fd_(resources.space_available_event_fd),
      publish_work_event_fd_(resources.publish_work_event_fd),
      shutdown_event_fd_(resources.shutdown_event_fd),
      worker_exit_event_fd_(resources.worker_exit_event_fd) {}

Publisher::CreateResult Publisher::create(TopicId topic_id, const char *shm_name, const Flags flags) {
    Logger::init(publisher_log_path());

    const Flags execution_mode_flags = flags & (SYNCHRONOUS | ASYNCHRONOUS);
    if (execution_mode_flags != SYNCHRONOUS && execution_mode_flags != ASYNCHRONOUS) {
        LOG_ERROR("PublisherModeFlagsInvalidError: flags=%u mode_flags=%u",
            flags,
            execution_mode_flags);
        return CreateResult{CreateError::PublisherModeFlagsInvalid};
    }

    auto shared_data_result = create_and_map_shared_data(shm_name);
    auto *shared_data_error = std::get_if<CreateError>(&shared_data_result);
    if (shared_data_error != nullptr) {
        return CreateResult{*shared_data_error};
    }

    auto [shm_fd, shared_data] = std::get<SharedDataMapping>(shared_data_result);
    FdGuard shm_guard(shm_fd);

    auto cleanup_mapping = [shm_name](SharedData *data) noexcept {
        munmap(data, sizeof(SharedData));
        shm_unlink(shm_name);
    };
    std::unique_ptr<SharedData, decltype(cleanup_mapping)> mapping_guard(shared_data, cleanup_mapping);

    int epoll_fd = epoll_create1(0);
    FdGuard epoll_guard(epoll_fd);
    if (epoll_fd < 0) {
        LOG_ERROR("PublisherEpollCreateError: errno=%d (%s)", errno, strerror(errno));
        return CreateResult{CreateError::EpollCreateFailed};
    }

    auto connect_result = connect_to_broker();
    auto *connect_error = std::get_if<CreateError>(&connect_result);
    if (connect_error != nullptr) {
        return CreateResult{*connect_error};
    }

    int broker_fd = std::get<int>(connect_result);
    FdGuard broker_guard(broker_fd);

    int space_available_event_fd = eventfd(0, EFD_NONBLOCK);
    FdGuard space_available_guard(space_available_event_fd);
    if (space_available_event_fd < 0) {
        LOG_ERROR("PublisherSpaceAvailableEventFdCreateError: errno=%d (%s)", errno, strerror(errno));
        return CreateResult{CreateError::SpaceAvailableEventFdCreateFailed};
    }
    if (!add_fd_to_epoll(epoll_fd, space_available_event_fd)) {
        LOG_ERROR("PublisherSpaceAvailableEpollAddError: epoll_fd=%d event_fd=%d errno=%d (%s)",
            epoll_fd,
            space_available_event_fd,
            errno,
            strerror(errno));
        return CreateResult{CreateError::SpaceAvailableEpollAddFailed};
    }

    auto registration_result = send_publisher_topic_registration(
        broker_fd,
        topic_id,
        shm_fd,
        space_available_event_fd);
    if (registration_result.has_value()) {
        return CreateResult{*registration_result};
    }

    int publish_work_event_fd = eventfd(0, EFD_NONBLOCK);
    FdGuard publish_work_guard(publish_work_event_fd);
    if (publish_work_event_fd < 0) {
        LOG_ERROR("PublisherWorkEventFdCreateError: errno=%d (%s)", errno, strerror(errno));
        return CreateResult{CreateError::PublishWorkEventFdCreateFailed};
    }
    if (!add_fd_to_epoll(epoll_fd, publish_work_event_fd)) {
        LOG_ERROR("PublisherWorkEpollAddError: epoll_fd=%d event_fd=%d errno=%d (%s)",
            epoll_fd,
            publish_work_event_fd,
            errno,
            strerror(errno));
        return CreateResult{CreateError::PublishWorkEpollAddFailed};
    }
    if (!signal_eventfd(publish_work_event_fd)) {
        LOG_ERROR("PublisherWorkInitialSignalError: event_fd=%d errno=%d (%s)",
            publish_work_event_fd,
            errno,
            strerror(errno));
        return CreateResult{CreateError::PublishWorkInitialSignalFailed};
    }

    int shutdown_event_fd = -1;
    FdGuard shutdown_guard(shutdown_event_fd);
    int worker_exit_event_fd = -1;
    FdGuard worker_exit_guard(worker_exit_event_fd);
    if (flags & ASYNCHRONOUS) {
        shutdown_event_fd = eventfd(0, EFD_NONBLOCK);
        if (shutdown_event_fd < 0) {
            LOG_ERROR("PublisherShutdownEventFdCreateError: errno=%d (%s)", errno, strerror(errno));
            return CreateResult{CreateError::ShutdownEventFdCreateFailed};
        }
        if (!add_fd_to_epoll(epoll_fd, shutdown_event_fd)) {
            LOG_ERROR("PublisherShutdownEpollAddError: epoll_fd=%d event_fd=%d errno=%d (%s)",
                epoll_fd,
                shutdown_event_fd,
                errno,
                strerror(errno));
            return CreateResult{CreateError::ShutdownEpollAddFailed};
        }

        worker_exit_event_fd = eventfd(0, EFD_NONBLOCK);
        if (worker_exit_event_fd < 0) {
            LOG_ERROR("PublisherWorkerExitEventFdCreateError: errno=%d (%s)", errno, strerror(errno));
            return CreateResult{CreateError::WorkerExitEventFdCreateFailed};
        }
    }

    InitialResources resources{
        .shm_name = shm_name,
        .shared_data = shared_data,
        .broker_fd = broker_fd,
        .epoll_fd = epoll_fd,
        .space_available_event_fd = space_available_event_fd,
        .publish_work_event_fd = publish_work_event_fd,
        .shutdown_event_fd = shutdown_event_fd,
        .worker_exit_event_fd = worker_exit_event_fd,
    };
    Publisher publisher(std::move(resources), flags);
    // Publisher 已接管映射，守卫仅放弃所有权。
    // NOLINTNEXTLINE(bugprone-unused-return-value)
    mapping_guard.release();
    epoll_guard.release();
    broker_guard.release();
    space_available_guard.release();
    publish_work_guard.release();
    shutdown_guard.release();
    worker_exit_guard.release();

    auto recv_and_track_result = publisher.receive_and_track_initial_subscriber_registrations();
    if (recv_and_track_result.has_value()) {
        return CreateResult{*recv_and_track_result};
    }

    // 注册请求发送和初始订阅者信息接收完成后，再将 broker FD 加入 epoll。
    // add_fd_to_epoll() 会设置 O_NONBLOCK；提前调用可能使握手接收返回 EAGAIN，导致创建失败。
    if (!add_fd_to_epoll(publisher.epoll_fd_, publisher.broker_fd_)) {
        LOG_ERROR("PublisherBrokerEpollAddError: epoll_fd=%d broker_fd=%d errno=%d (%s)",
            publisher.epoll_fd_,
            publisher.broker_fd_,
            errno,
            strerror(errno));
        return CreateResult{CreateError::BrokerEpollAddFailed};
    }

    CreateResult result{std::move(publisher)};
    Publisher &created_publisher = std::get<Publisher>(result.value);
    if (created_publisher.flags_ & ASYNCHRONOUS) {
        try {
            created_publisher.start_run_thread();
        } catch (const std::system_error &error) {
            LOG_ERROR("PublisherRunThreadCreateError: %s", error.what());
            return CreateResult{CreateError::RunThreadCreateFailed};
        }
    }
    return result;
}

// 移动/析构时不得并发调用成员函数，也不得从本对象的回调中执行。
// 移动可能因通知清理或 worker 重启失败抛出 std::system_error（含派生异常）。
// 内存分配失败也可能抛出 std::bad_alloc，因此移动操作不保证 noexcept。
Publisher::Publisher(Publisher &&other) {
    take_resources_from(std::move(other));
}

Publisher &Publisher::operator=(Publisher &&other) {
    if (this != &other) {
        release_resources();
        take_resources_from(std::move(other));
    }
    return *this;
}

Publisher::~Publisher() {
    release_resources();
}

void Publisher::release_resources() {
    stop_run_thread(WorkerStopIntent::Destroy);

    close_fd(broker_fd_);
    close_fd(publish_work_event_fd_);
    close_fd(space_available_event_fd_);
    close_fd(shutdown_event_fd_);
    close_fd(worker_exit_event_fd_);

    for (int &event_fd : data_available_event_fds_) {
        close_fd(event_fd);
    }
    data_available_event_fds_.clear();
    subscriber_slot_index_by_data_available_event_fd_.clear();

    close_fd(epoll_fd_);

    if (shared_data_ != nullptr) {
        munmap(shared_data_, sizeof(SharedData));
        shared_data_ = nullptr;
    }
    if (!shm_name_.empty()) {
        shm_unlink(shm_name_.c_str());
        shm_name_.clear();
    }
}

// 移动/析构 Publisher 时，调用方不得并发调用其任何成员函数
void Publisher::take_resources_from(Publisher &&other) {
    const bool restart_run_thread = other.stop_run_thread(WorkerStopIntent::Transfer);
    if (restart_run_thread) {
        for (const int event_fd : {other.shutdown_event_fd_, other.worker_exit_event_fd_}) {
            if (!drain_eventfd(event_fd)) {
                const int saved_errno = errno;
                LOG_ERROR("PublisherTransferEventDrainError: event_fd=%d errno=%d (%s)",
                    event_fd,
                    saved_errno,
                    strerror(saved_errno));
                throw std::system_error(saved_errno, std::generic_category(), "Publisher transfer event drain");
            }
        }
    }

    flags_ = std::exchange(other.flags_, SYNCHRONOUS);

    pending_messages_ = std::move(other.pending_messages_); // 并不上锁处理
    other.pending_messages_.clear();

    in_flight_message_ = std::exchange(other.in_flight_message_, std::nullopt);

    shm_name_ = std::move(other.shm_name_);
    other.shm_name_.clear();

    shared_data_ = std::exchange(other.shared_data_, nullptr);

    broker_fd_ = std::exchange(other.broker_fd_, -1);
    epoll_fd_ = std::exchange(other.epoll_fd_, -1);
    publish_work_event_fd_ = std::exchange(other.publish_work_event_fd_, -1);
    space_available_event_fd_ = std::exchange(other.space_available_event_fd_, -1);
    shutdown_event_fd_ = std::exchange(other.shutdown_event_fd_, -1);
    worker_exit_event_fd_ = std::exchange(other.worker_exit_event_fd_, -1);
    worker_exit_future_ = std::move(other.worker_exit_future_);
    worker_exit_promise_ = std::move(other.worker_exit_promise_);

    std::copy_n(other.events_, kMaxEvents, events_);
    event_count_ = std::exchange(other.event_count_, 0);
    event_index_ = std::exchange(other.event_index_, 0);

    data_available_event_fds_ = std::move(other.data_available_event_fds_);
    other.data_available_event_fds_.clear();

    data_available_event_fd_by_subscriber_slot_index_ =
        std::move(other.data_available_event_fd_by_subscriber_slot_index_);

    subscriber_slot_index_by_data_available_event_fd_ =
        std::move(other.subscriber_slot_index_by_data_available_event_fd_);

    seq_ = std::exchange(other.seq_, 0);

    try {
        if (restart_run_thread) {
            start_run_thread();
        }
    } catch (...) {
        release_resources();
        throw;
    }
}

void Publisher::reset_worker_exit_notification() {
    worker_exit_promise_ = std::promise<RunError>{};
    worker_exit_future_ = worker_exit_promise_.get_future().share();
}

void Publisher::start_run_thread() {
    worker_stop_intent_.store(WorkerStopIntent::Destroy, std::memory_order_relaxed);
    reset_worker_exit_notification();
    run_thread_active_.store(true, std::memory_order_relaxed);
    try {
        run_thread_ = std::thread([this] {
            RunError reason = RunError::None;
            try {
                reason = run();
            } catch (const std::exception &error) {
                reason = RunError::WorkerException;
                LOG_ERROR("PublisherWorkerException: %s", error.what());
            } catch (...) {
                reason = RunError::WorkerException;
                LOG_ERROR("PublisherWorkerException: unknown exception");
            }
            run_thread_active_.store(false, std::memory_order_relaxed);

            if (reason != RunError::None) {
                complete_outstanding_messages(PublishCompletion::WorkerFailed);
            } else if (worker_stop_intent_ != WorkerStopIntent::Transfer) {
                complete_outstanding_messages(PublishCompletion::WorkerStopped);
            }
            worker_exit_promise_.set_value(reason);

            if (!signal_eventfd(worker_exit_event_fd_)) {
                LOG_ERROR("PublisherWorkerExitSignalError: event_fd=%d errno=%d (%s)",
                    worker_exit_event_fd_,
                    errno,
                    strerror(errno));
            }
        });
    } catch (...) {
        run_thread_active_.store(false, std::memory_order_relaxed);
        throw;
    }
}

bool Publisher::stop_run_thread(const WorkerStopIntent stop_intent) {
    if (!run_thread_.joinable()) {
        return false;
    }

    const bool was_active = run_thread_active_.load(std::memory_order_relaxed);
    if (was_active) {
        worker_stop_intent_.store(stop_intent, std::memory_order_relaxed);
        if (!signal_eventfd(shutdown_event_fd_)) {
            LOG_ERROR("PublisherShutdownEventWriteError: event_fd=%d errno=%d (%s)",
                shutdown_event_fd_,
                errno,
                strerror(errno));
        }
    }

    run_thread_.join();
    return was_active &&
        stop_intent == WorkerStopIntent::Transfer &&
        worker_exit_future_.valid() &&
        worker_exit_future_.get() == RunError::None;
}

uint32_t Publisher::find_slowest_read_index() {
    uint32_t min_read_index_before_write = kInvalidIndex;
    uint32_t min_read_index_after_write = kInvalidIndex;
    uint32_t descriptor_write_index = shared_data_->descriptor_write_index.load(std::memory_order_relaxed);
    for (size_t subscriber_index = 0; subscriber_index < kMaxSubscribers; ++subscriber_index) {
        uint32_t descriptor_read_index = shared_data_->descriptor_read_indices[subscriber_index].load(std::memory_order_acquire);
        if (descriptor_read_index == kInvalidIndex) continue;

        if (descriptor_read_index <= descriptor_write_index)
            min_read_index_after_write = std::min(min_read_index_after_write, descriptor_read_index);
        else
            min_read_index_before_write = std::min(min_read_index_before_write, descriptor_read_index);
    }

    if (min_read_index_before_write != std::numeric_limits<uint32_t>::max())
        return min_read_index_before_write;

    return min_read_index_after_write;
}

Publisher::RunError Publisher::release_chunk_references_in_descriptor_range(
    size_t start,
    size_t end,
    uint32_t subscriber_reference_bit) {
    while (start < end) {
        uint32_t offset = shared_data_->desc_ring[start].offset;
        uint32_t len = shared_data_->desc_ring[start].len;
        int size_class = find_size_class(len);

        uint32_t local_chunk_index = (offset - kFirstOffset[size_class]) / kClassSizeBytes[size_class];
        uint32_t chunk_index = kChunkIndexBaseBySizeClass[size_class] + local_chunk_index;
        uint32_t previous_reference_count = shared_data_->chunk_reference_counts[chunk_index].fetch_and(~subscriber_reference_bit, std::memory_order_acq_rel);

        if (previous_reference_count == subscriber_reference_bit) {
#ifdef ENABLE_DEBUG_CHECKS
            {
                pthread_mutex_lock(&shared_data_->chunk_usage_tracker.mutex);

                size_t chunk_index = (offset - kFirstOffset[size_class]) / kClassSizeBytes[size_class];
                if (shared_data_->chunk_usage_tracker.is_in_use[size_class][chunk_index] == false) {
                    LOG_FATAL("PublisherChunkDoubleFreeError: class=%d index=%zu offset=%u",
                        size_class,
                        chunk_index,
                        offset);
                    pthread_mutex_unlock(&shared_data_->chunk_usage_tracker.mutex);
                    return RunError::ChunkReferenceReleaseFailed;
                }
                shared_data_->chunk_usage_tracker.is_in_use[size_class][chunk_index] = false;

                pthread_mutex_unlock(&shared_data_->chunk_usage_tracker.mutex);
            }
#endif

            uint32_t last_tail_off = shared_data_->tail.offset[size_class].load(std::memory_order_relaxed);
            memcpy(shared_data_->data + last_tail_off, &offset, sizeof(uint32_t));
            shared_data_->tail.offset[size_class].store(offset, std::memory_order_release);
        }
        ++start;
    }
    return RunError::None;
}

Publisher::RunError Publisher::reap_dead_subscribers(uint32_t slot_index) {
    if (slot_index >= kMaxSubscribers) {
        LOG_FATAL("PublisherDeadSubscriberSlotRangeError: slot=%u max=%zu",
            slot_index,
            kMaxSubscribers);
        return RunError::DeadSubscriberReapFailed;
    }

    uint32_t subscriber_reference_bit = 1 << slot_index;
    uint32_t rd = shared_data_->descriptor_read_indices[slot_index].load(std::memory_order_acquire);
    if (rd == kInvalidIndex) {
        LOG_FATAL("PublisherDeadSubscriberNotTrackedError: slot=%u", slot_index);
        return RunError::DeadSubscriberReapFailed;
    }
    uint32_t wr = shared_data_->descriptor_write_index.load(std::memory_order_relaxed);
    if (rd > wr) {
        const RunError wrapped_release_result =
            release_chunk_references_in_descriptor_range(rd, kDescriptorSlotCount, subscriber_reference_bit);
        if (wrapped_release_result != RunError::None) {
            return wrapped_release_result;
        }
        rd = 0;
    }

    const RunError contiguous_release_result =
        release_chunk_references_in_descriptor_range(rd, wr, subscriber_reference_bit);
    if (contiguous_release_result != RunError::None) {
        return contiguous_release_result;
    }
    shared_data_->descriptor_read_indices[slot_index].store(kInvalidIndex, std::memory_order_relaxed);

    const int data_available_event_fd = data_available_event_fd_by_subscriber_slot_index_[slot_index];
    auto data_available_event_fd_it =
        subscriber_slot_index_by_data_available_event_fd_.find(data_available_event_fd);
    if (data_available_event_fd_it == subscriber_slot_index_by_data_available_event_fd_.end() ||
        data_available_event_fd_it->second != slot_index) {
        LOG_FATAL("PublisherDeadSubscriberEventFdMissingError: slot=%u data_available_event_fd=%d",
            slot_index,
            data_available_event_fd);
        return RunError::DeadSubscriberReapFailed;
    }
    subscriber_slot_index_by_data_available_event_fd_.erase(data_available_event_fd_it);
    erase(data_available_event_fds_, data_available_event_fd);
    close(data_available_event_fd);
    return RunError::None;
}

Publisher::RunError Publisher::handle_subscriber_disconnected_message(
    const char packet[],
    size_t body_len,
    cmsghdr *) {
    if (body_len != sizeof(uint32_t)) {
        LOG_FATAL("SubscriberDisconnectPayloadSizeError: expected=%zu actual=%zu",
            sizeof(uint32_t),
            body_len);
        return RunError::BrokerControlMessageProcessingFailed;
    }

    uint32_t subscriber_slot_index;
    memcpy(&subscriber_slot_index, packet + sizeof(BrokerMessageType), sizeof(subscriber_slot_index));
    return reap_dead_subscribers(subscriber_slot_index);
}

Publisher::RunError Publisher::handle_subscriber_registered_message(
    const char packet[],
    size_t body_len,
    cmsghdr *cmsg) {
    if (body_len != sizeof(uint32_t)) {
        LOG_FATAL("SubscriberRegisteredPayloadSizeError: expected=%zu actual=%zu",
            sizeof(uint32_t),
            body_len);
        return RunError::BrokerControlMessageProcessingFailed;
    }
    if (cmsg == nullptr) {
        LOG_FATAL("SubscriberRegisteredEventFdMissingError: broker_fd=%d", broker_fd_);
        return RunError::BrokerControlMessageProcessingFailed;
    }
    if (cmsg->cmsg_level != SOL_SOCKET ||
        cmsg->cmsg_type != SCM_RIGHTS ||
        cmsg->cmsg_len != CMSG_LEN(sizeof(int))) {
        LOG_FATAL("SubscriberRegisteredCmsgError: level=%d type=%d broker_fd=%d",
            cmsg->cmsg_level,
            cmsg->cmsg_type,
            broker_fd_);
        return RunError::BrokerControlMessageProcessingFailed;
    }

    uint32_t subscriber_slot_index;
    memcpy(&subscriber_slot_index, packet + sizeof(BrokerMessageType), sizeof(subscriber_slot_index));

    int *received_event_fd = reinterpret_cast<int *>(CMSG_DATA(cmsg));
    const int data_available_event_fd = std::exchange(*received_event_fd, -1);
    const auto tracking_error = track_subscriber({data_available_event_fd, subscriber_slot_index});
    if (tracking_error.has_value()) {
        return RunError::DynamicSubscriberRegistrationFailed;
    }
    return RunError::None;
}

Publisher::RunError Publisher::process_broker_control_messages() {
    char packet[kMaxMessageSize];
    char control[CMSG_SPACE(sizeof(int))];

    iovec iov{};
    iov.iov_base = packet;
    iov.iov_len = kMaxMessageSize;

    msghdr msg{};
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
            }

            LOG_ERROR("PublisherControlRecvError: broker_fd=%d errno=%d (%s)",
                broker_fd_,
                errno,
                strerror(errno));
            return RunError::BrokerControlMessageProcessingFailed;
        }
        ReceivedFdsGuard received_fds(msg);
        if (received_size == 0) {
            LOG_ERROR("PublisherBrokerClosed: broker_fd=%d", broker_fd_);
            epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, broker_fd_, nullptr);
            close(broker_fd_);
            broker_fd_ = -1;
            return RunError::BrokerClosed;
        }
        if (msg.msg_flags & (MSG_CTRUNC | MSG_TRUNC)) {
            LOG_FATAL("MsgTruncatedError");
            return RunError::BrokerControlMessageProcessingFailed;
        }
        if (received_size < static_cast<ssize_t>(sizeof(BrokerMessageType))) {
            LOG_FATAL("BrokerControlMessageHeaderSizeError: expected_at_least=%zu actual=%zd broker_fd=%d",
                sizeof(BrokerMessageType),
                received_size,
                broker_fd_);
            return RunError::BrokerControlMessageTooShort;
        }

        const size_t body_len = static_cast<size_t>(received_size) - sizeof(BrokerMessageType);
        BrokerMessageType message_type;
        memcpy(&message_type, packet, sizeof(message_type));

        RunError handler_result = RunError::None;
        switch (message_type) {
            case BrokerMessageType::SubscriberDisconnected:
                handler_result = handle_subscriber_disconnected_message(
                    packet,
                    body_len,
                    CMSG_FIRSTHDR(&msg));
                break;
            case BrokerMessageType::SubscriberEventFdAndSlot:
                handler_result = handle_subscriber_registered_message(
                    packet,
                    body_len,
                    CMSG_FIRSTHDR(&msg));
                break;
            default:
                LOG_FATAL("UnexpectedBrokerMessageTypeError: type=%u bytes=%zd broker_fd=%d",
                    static_cast<unsigned>(static_cast<unsigned char>(packet[0])),
                    received_size,
                    broker_fd_);
                return RunError::BrokerControlMessageProcessingFailed;
        }

        if (handler_result != RunError::None) {
            return handler_result;
        }
    }
    return RunError::None;
}

std::variant<Publisher::PublishReadiness, Publisher::RunError>
Publisher::can_publish_next_message() {
    uint32_t min_descriptor_read_index = find_slowest_read_index();

    uint32_t message_size_bytes = kMessageHeaderSizeBytes;
    {
        std::lock_guard<std::mutex> lock(publish_state_mutex_);

        if (in_flight_message_.has_value())
            message_size_bytes += in_flight_message_->message.payload.size();
        else if (!pending_messages_.empty())
            message_size_bytes += pending_messages_.front().payload.size();
        else
            return PublishReadiness::NoMessage;
    }

    int size_class = find_size_class(message_size_bytes);
    if (size_class < 0) {
        LOG_FATAL("PublisherMessageSizeClassError: seq=%d size=%u", seq_, message_size_bytes);
        return RunError::MessageSizeClassInvalid;
    }

    uint32_t wr = shared_data_->descriptor_write_index.load(std::memory_order_relaxed);
    if (min_descriptor_read_index == kInvalidIndex ||
        (min_descriptor_read_index > wr && min_descriptor_read_index <= wr + 1) ||
        wr + 1 >= min_descriptor_read_index + kDescriptorSlotCount) {
        LOG_DEBUG("publisher backpressure: min_rd=%u wr=%u rd0=%u rd1=%u",
            min_descriptor_read_index,
            wr,
            shared_data_->descriptor_read_indices[0].load(std::memory_order_relaxed),
            shared_data_->descriptor_read_indices[1].load(std::memory_order_relaxed));

        return PublishReadiness::Blocked;
    }

    LOG_DEBUG("publisher descriptor state: wr=%u min_rd=%u rd0=%u rd1=%u",
        wr,
        min_descriptor_read_index,
        shared_data_->descriptor_read_indices[0].load(std::memory_order_relaxed),
        shared_data_->descriptor_read_indices[1].load(std::memory_order_relaxed));

    uint32_t head_off = shared_data_->head.offset[size_class].load(std::memory_order_relaxed);
    uint32_t tail_off = shared_data_->tail.offset[size_class].load(std::memory_order_acquire);

    if (head_off == tail_off) {
        LOG_DEBUG("publisher free-list empty: size_class=%d", size_class);
        return PublishReadiness::Blocked;
    }

    return PublishReadiness::Ready;
}

std::variant<Publisher::PublishAttempt, Publisher::RunError>
Publisher::try_publish_one(std::span<const std::byte> payload) {
    uint32_t min_descriptor_read_index = find_slowest_read_index();

    if (payload.size() > std::numeric_limits<uint32_t>::max() - kMessageHeaderSizeBytes) {
        LOG_FATAL("PublisherMessageSizeClassError: seq=%d payload_size=%zu", seq_, payload.size());
        return RunError::MessageSizeClassInvalid;
    }

    const uint32_t message_size_bytes =
        static_cast<uint32_t>(payload.size() + kMessageHeaderSizeBytes);
    int size_class = find_size_class(message_size_bytes);
    if (size_class < 0) {
        LOG_FATAL("PublisherMessageSizeClassError: seq=%d size=%u", seq_, message_size_bytes);
        return RunError::MessageSizeClassInvalid;
    }

    uint32_t wr = shared_data_->descriptor_write_index.load(std::memory_order_relaxed);
    if (min_descriptor_read_index == kInvalidIndex ||
        (min_descriptor_read_index > wr && min_descriptor_read_index <= wr + 1) ||
        wr + 1 >= min_descriptor_read_index + kDescriptorSlotCount) {
        LOG_DEBUG("publisher backpressure: min_rd=%u wr=%u rd0=%u rd1=%u",
            min_descriptor_read_index,
            wr,
            shared_data_->descriptor_read_indices[0].load(std::memory_order_relaxed),
            shared_data_->descriptor_read_indices[1].load(std::memory_order_relaxed));

        return PublishAttempt::Blocked;
    }

    LOG_DEBUG("publisher descriptor state: wr=%u min_rd=%u rd0=%u rd1=%u",
        wr,
        min_descriptor_read_index,
        shared_data_->descriptor_read_indices[0].load(std::memory_order_relaxed),
        shared_data_->descriptor_read_indices[1].load(std::memory_order_relaxed));

    uint32_t head_off = shared_data_->head.offset[size_class].load(std::memory_order_relaxed);
    uint32_t tail_off = shared_data_->tail.offset[size_class].load(std::memory_order_acquire);

    if (head_off == tail_off) {
        LOG_DEBUG("publisher free-list empty: size_class=%d", size_class);
        return PublishAttempt::Blocked;
    }

#ifdef ENABLE_DEBUG_CHECKS
    {
        if (head_off < kFirstOffset[size_class] || head_off > kLastOffset[size_class]) {
            LOG_FATAL("PublisherChunkOffsetError: class=%d offset=%u", size_class, head_off);
            return RunError::PublishAttemptFailed;
        }

        pthread_mutex_lock(&shared_data_->chunk_usage_tracker.mutex);

        size_t chunk_index = (head_off - kFirstOffset[size_class]) / kClassSizeBytes[size_class];
        if (shared_data_->chunk_usage_tracker.is_in_use[size_class][chunk_index] == true) {
            LOG_FATAL("PublisherChunkAlreadyInUseError: class=%d index=%zu offset=%u",
                size_class,
                chunk_index,
                head_off);
            pthread_mutex_unlock(&shared_data_->chunk_usage_tracker.mutex);
            return RunError::PublishAttemptFailed;
        }
        shared_data_->chunk_usage_tracker.is_in_use[size_class][chunk_index] = true;

        pthread_mutex_unlock(&shared_data_->chunk_usage_tracker.mutex);
    }
#endif

    uint32_t next_head_off;
    memcpy(&next_head_off, shared_data_->data + head_off, sizeof(uint32_t));
    shared_data_->head.offset[size_class].store(next_head_off, std::memory_order_relaxed);

    MessageDescriptor desc{head_off, message_size_bytes};
    memcpy(shared_data_->desc_ring + wr, &desc, sizeof(MessageDescriptor));

    memcpy(shared_data_->data + head_off, &kMagic, sizeof(int));
    memcpy(shared_data_->data + head_off + sizeof(int), &seq_, sizeof(int));
    ++seq_;
    if (!payload.empty()) {
        memcpy(shared_data_->data + head_off + kMessageHeaderSizeBytes, payload.data(), payload.size());
    }

    uint32_t local_chunk_index = (head_off - kFirstOffset[size_class]) / kClassSizeBytes[size_class];
    uint32_t chunk_index = kChunkIndexBaseBySizeClass[size_class] + local_chunk_index;

    uint32_t chunk_reference_mask = 0;
    for (int data_available_event_fd : data_available_event_fds_) {
        auto it = subscriber_slot_index_by_data_available_event_fd_.find(data_available_event_fd);
        if (it == subscriber_slot_index_by_data_available_event_fd_.end()) {
            LOG_FATAL("PublisherEventFdSlotMapMissingError: data_available_event_fd=%d", data_available_event_fd);
            return RunError::PublishAttemptFailed;
        }
        chunk_reference_mask += 1 << it->second;
    }
    LOG_DEBUG("publisher chunk_reference_mask=%u", chunk_reference_mask);
    shared_data_->chunk_reference_counts[chunk_index].fetch_add(chunk_reference_mask, std::memory_order_relaxed);

    uint32_t candidate = (wr + 1) % kDescriptorSlotCount;
    if (candidate >= kDescriptorSlotCount) shared_data_->descriptor_write_index.store(candidate % kDescriptorSlotCount, std::memory_order_release);
    else shared_data_->descriptor_write_index.store(candidate, std::memory_order_release);

    for (int data_available_event_fd : data_available_event_fds_) {
        if (!signal_eventfd(data_available_event_fd)) {
            LOG_ERROR("PublisherDataAvailableSignalError: event_fd=%d errno=%d (%s)",
                data_available_event_fd,
                errno,
                strerror(errno));
            return RunError::PublishAttemptFailed;
        }
    }
    LOG_DEBUG("publisher wrote message: bytes=%u seq=%d",
        message_size_bytes,
        seq_ - 1);

    return PublishAttempt::Succeeded;
}

Publisher::RunError Publisher::handle_publish_work_event(bool &publisher_ready) {
    if (!drain_eventfd(publish_work_event_fd_)) {
        LOG_ERROR("PublisherWorkEventReadError: event_fd=%d errno=%d (%s)",
            publish_work_event_fd_,
            errno,
            strerror(errno));
        return RunError::PublishWorkEventReadFailed;
    }

    {
        std::unique_lock<std::mutex> lock(publish_state_mutex_);

        if (!publisher_ready &&
            in_flight_message_.has_value() &&
            in_flight_message_->state == InFlightState::Blocked) {
            return RunError::None;
        }
        publisher_ready = true;

        size_t published_count = 0;
        while (published_count < kMaxPublishesPerWorkEvent && (!pending_messages_.empty() || in_flight_message_.has_value())) {
            if (!in_flight_message_.has_value()) {
                in_flight_message_ = {std::move(pending_messages_.front()), InFlightState::Publishing}; // 防止 pending_messages_.front() 被覆盖导致 payload 悬空
                pending_messages_.pop_front();
            }

            in_flight_message_->state = InFlightState::Publishing;
            std::span<const std::byte> payload{in_flight_message_.value().message.payload};

            lock.unlock();

            const auto publish_result = try_publish_one(payload);
            if (const auto *publish_error = std::get_if<RunError>(&publish_result)) {
                return *publish_error;
            }

            lock.lock();

            const PublishAttempt publish_attempt = std::get<PublishAttempt>(publish_result);
            if (publish_attempt == PublishAttempt::Blocked) {
                publisher_ready = false;
                in_flight_message_->state = InFlightState::Blocked;
                break;
            }

            ++published_count;

            in_flight_message_->message.completion_promise.set_value(PublishCompletion::Published);
            in_flight_message_ = std::nullopt;
        }

        if (!publisher_ready || (pending_messages_.empty() && !in_flight_message_.has_value())) {
            return RunError::None;
        }
    }

    if (!signal_eventfd(publish_work_event_fd_)) {
        LOG_ERROR("PublisherWorkRescheduleError: event_fd=%d errno=%d (%s)",
            publish_work_event_fd_,
            errno,
            strerror(errno));
        return RunError::PublishWorkRescheduleFailed;
    }
    return RunError::None;
}

Publisher::RunError Publisher::handle_space_available_event(bool &publisher_ready) {
    if (!drain_eventfd(space_available_event_fd_)) {
        LOG_ERROR("PublisherSpaceAvailableEventReadError: event_fd=%d errno=%d (%s)",
            space_available_event_fd_,
            errno,
            strerror(errno));
        return RunError::SpaceAvailableEventReadFailed;
    }

    const auto publish_readiness_result = can_publish_next_message();
    if (const auto *readiness_error = std::get_if<RunError>(&publish_readiness_result)) {
        return *readiness_error;
    }
    if (std::get<PublishReadiness>(publish_readiness_result) != PublishReadiness::Ready) {
        return RunError::None;
    }

    publisher_ready = true;
    if (!signal_eventfd(publish_work_event_fd_)) {
        LOG_ERROR("PublisherWorkResumeError: event_fd=%d errno=%d (%s)",
            publish_work_event_fd_,
            errno,
            strerror(errno));
        return RunError::PublishWorkResumeFailed;
    }
    return RunError::None;
}

Publisher::RunError Publisher::handle_broker_control_event(bool &publisher_ready) {
    const RunError control_result = process_broker_control_messages();
    if (control_result != RunError::None) {
        return control_result;
    }

    const auto publish_readiness_result = can_publish_next_message();
    if (const auto *readiness_error = std::get_if<RunError>(&publish_readiness_result)) {
        return *readiness_error;
    }
    if (std::get<PublishReadiness>(publish_readiness_result) != PublishReadiness::Ready) {
        return RunError::None;
    }

    publisher_ready = true;
    if (!signal_eventfd(publish_work_event_fd_)) {
        LOG_ERROR("PublisherBrokerResumeError: event_fd=%d errno=%d (%s)",
            publish_work_event_fd_,
            errno,
            strerror(errno));
        return RunError::PublishWorkBrokerResumeFailed;
    }
    return RunError::None;
}

Publisher::PublishResult Publisher::publish(std::span<const std::byte> payload,
    std::chrono::microseconds timeout) {
    if (!(flags_ & SYNCHRONOUS)) {
        return PublishError::ModeMismatch;
    }
    if (payload.size() > kMaxPayloadSizeBytes) {
        return PublishError::PayloadTooLarge;
    }

    const auto deadline = std::chrono::steady_clock::now() + timeout;

    while (true) {
        for (; event_index_ < event_count_; ++event_index_) {
            if (std::chrono::steady_clock::now() >= deadline) {
                return PublishError::Timeout;
            }

            const int event_fd = events_[event_index_].data.fd;
            if (event_fd == space_available_event_fd_) {
                if (!drain_eventfd(space_available_event_fd_)) {
                    return PublishError::EventWaitFailed;
                }
            } else if (event_fd == broker_fd_) {
                const RunError run_error = process_broker_control_messages();
                if (run_error != RunError::None) {
                    return map_sync_run_error_to_publish_error(run_error);
                }
            }
        }

        if (std::chrono::steady_clock::now() >= deadline) {
            return PublishError::Timeout;
        }

        const auto result = try_publish_one(payload);
        if (const auto *run_error = std::get_if<RunError>(&result)) {
            return map_sync_run_error_to_publish_error(*run_error);
        }

        const auto attempt = std::get<PublishAttempt>(result);
        if (attempt == PublishAttempt::Succeeded) {
            std::promise<PublishCompletion> promise;
            promise.set_value(PublishCompletion::Published);
            return PublishTicket{PublishDisposition::Published, promise.get_future().share()};
        }

        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            return PublishError::Timeout;
        }

        const auto remaining = std::chrono::duration_cast<std::chrono::nanoseconds>(deadline - now);
        const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(remaining);

        const timespec wait_timeout{
            .tv_sec = static_cast<time_t>(seconds.count()),
            .tv_nsec = static_cast<long>((remaining - seconds).count()),
        };

        event_count_ = epoll_pwait2(epoll_fd_, events_, kMaxEvents, &wait_timeout, nullptr);
        event_index_ = 0;

        if (event_count_ == 0) {
            return PublishError::Timeout;
        }
        if (event_count_ < 0) {
            if (errno == EINTR) {
                continue;
            }
            return PublishError::EventWaitFailed;
        }
    }
}

Publisher::PublishResult Publisher::publish(std::span<const std::byte> payload) {
    if (!(flags_ & ASYNCHRONOUS)) {
        return PublishError::ModeMismatch;
    }
    if (payload.size() > kMaxPayloadSizeBytes) {
        return PublishError::PayloadTooLarge;
    }

    {
        std::lock_guard<std::mutex> lock(publish_state_mutex_);
        if (!run_thread_active_.load(std::memory_order_relaxed)) {
            return PublishError::WorkerExited;
        }

        std::optional<PendingMessage> overwritten_message;
        const size_t queued_message_count =
            pending_messages_.size() + (in_flight_message_.has_value() ? 1U : 0U);
        if (queued_message_count >= kMaxOutstandingMessages) {
            if (in_flight_message_.has_value() && in_flight_message_->state == InFlightState::Blocked) {
                overwritten_message.emplace(std::move(in_flight_message_->message));
                in_flight_message_ = std::nullopt;
            } else {
                overwritten_message.emplace(std::move(pending_messages_.front()));
                pending_messages_.pop_front();
            }
        }

        pending_messages_.emplace_back(payload);

        if (!signal_eventfd(publish_work_event_fd_)) {
            pending_messages_.pop_back();

            if (overwritten_message.has_value()) {
                pending_messages_.emplace_front(std::move(*overwritten_message));
            }
            LOG_ERROR("PublisherWorkSignalError: event_fd=%d errno=%d (%s)",
                publish_work_event_fd_,
                errno,
                strerror(errno));
            return PublishError::NotificationFailed;
        }

        if (overwritten_message.has_value())
            overwritten_message->completion_promise.set_value(PublishCompletion::Overwritten);

        return overwritten_message.has_value()
            ? PublishTicket{PublishDisposition::EnqueuedAfterOverwritingOldest, pending_messages_.back().completion_promise.get_future().share()}
            : PublishTicket{PublishDisposition::Enqueued, pending_messages_.back().completion_promise.get_future().share()};
    }
}

Publisher::RunError Publisher::run() {
    bool publisher_ready = true;

    bool running = true;
    RunError reason = RunError::None;
    while (running && reason == RunError::None) {
        if (event_index_ >= event_count_) {
            event_index_ = 0;
            event_count_ = epoll_wait(epoll_fd_, events_, kMaxEvents, -1);
            if (event_count_ < 0) {
                LOG_ERROR("PublisherEpollWaitError: errno=%d (%s)", errno, strerror(errno));
                reason = RunError::EpollWaitFailed;
                running = false;
                break;
            }
            if (event_count_ == 0) {
                LOG_ERROR("PublisherUnexpectedEpollTimeoutError");
                reason = RunError::UnexpectedEpollTimeout;
                running = false;
                break;
            }
            LOG_DEBUG("epoll event_count=%d", event_count_);
        }

        while (event_index_ < event_count_) {
            const int event_fd = events_[event_index_++].data.fd;

            if (event_fd == publish_work_event_fd_) {
                reason = handle_publish_work_event(publisher_ready);
            } else if (event_fd == space_available_event_fd_) {
                reason = handle_space_available_event(publisher_ready);
            } else if (event_fd == broker_fd_) {
                reason = handle_broker_control_event(publisher_ready);
            } else if (event_fd == shutdown_event_fd_) {
                if (!drain_eventfd(shutdown_event_fd_)) {
                    LOG_ERROR("PublisherShutdownEventReadError: event_fd=%d errno=%d (%s)",
                        shutdown_event_fd_,
                        errno,
                        strerror(errno));
                    reason = RunError::ShutdownEventReadFailed;
                    running = false;
                    break;
                }
                running = false;
                break;
            }

            if (reason != RunError::None) break;
        }
    }

    run_thread_active_.store(false, std::memory_order_relaxed);

    LOG_INFO("publisher completed message production");
    return reason;
}

void Publisher::complete_outstanding_messages(PublishCompletion completion) {
    std::lock_guard<std::mutex> lock(publish_state_mutex_);
    if (in_flight_message_.has_value())
        in_flight_message_->message.completion_promise.set_value(completion);
    for (auto &[payload, promise] : this->pending_messages_) {
        promise.set_value(completion);
    }
}
