#include "../include/publisher.hpp"
#include "../include/logger.h"
#include "../include/fd_helpers.hpp"
#include "../include/shared_memory_layout_helpers.hpp"
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <optional>
#include <random>
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

uint32_t generate_message_size() {
    std::random_device random_device;
    std::mt19937 random_engine(random_device());

    std::uniform_int_distribution<uint32_t> choose_distribution(0, 1);
    std::uniform_int_distribution<uint32_t> small_message_size_distribution(8, 16);
    std::uniform_int_distribution<uint32_t> large_message_size_distribution(128, 1024);

    if (choose_distribution(random_engine) == 0) {
        return small_message_size_distribution(random_engine);
    }
    return large_message_size_distribution(random_engine);
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

void close_received_fds(cmsghdr *cmsg) {
    if (cmsg == nullptr ||
        cmsg->cmsg_level != SOL_SOCKET ||
        cmsg->cmsg_type != SCM_RIGHTS ||
        cmsg->cmsg_len < CMSG_LEN(0)) {
        return;
    }

    const size_t fd_bytes = cmsg->cmsg_len - CMSG_LEN(0);
    if (fd_bytes % sizeof(int) != 0) {
        return;
    }

    int *received_fds = reinterpret_cast<int *>(CMSG_DATA(cmsg));
    const size_t fd_count = fd_bytes / sizeof(int);
    for (size_t fd_index = 0; fd_index < fd_count; ++fd_index) {
        close(received_fds[fd_index]);
    }
}

} // namespace

std::variant<Publisher::SubscriberRegistration, Publisher::CreateError>
Publisher::receive_subscriber_registration() {
    int fd = -1;
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
        close_fd(fd);
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
        close_fd(fd);
        return CreateError::InitialSubscriberRegistrationTypeInvalid;
    }
    uint32_t received_subscriber_slot_index;
    memcpy(&received_subscriber_slot_index, packet + 1, sizeof(received_subscriber_slot_index));

    return SubscriberRegistration{fd, received_subscriber_slot_index};
}

std::optional<Publisher::SubscriberTrackingError>
Publisher::track_subscriber(SubscriberRegistration registration) {
    if (registration.slot_index >= kMaxSubscribers) {
        LOG_ERROR("PublisherSubscriberSlotRangeError: slot=%u max=%zu data_available_event_fd=%d",
            registration.slot_index,
            kMaxSubscribers,
            registration.data_available_event_fd);
        close_fd(registration.data_available_event_fd);
        return SubscriberTrackingError::SlotOutOfRange;
    }
    if (shared_data_->descriptor_read_indices[registration.slot_index].load(std::memory_order_relaxed) != kInvalidIndex) {
        LOG_ERROR("PublisherSubscriberSlotAlreadyTrackedError: slot=%u data_available_event_fd=%d",
            registration.slot_index,
            registration.data_available_event_fd);
        close_fd(registration.data_available_event_fd);
        return SubscriberTrackingError::SlotAlreadyTracked;
    }

    const bool event_fd_inserted =
        subscriber_slot_index_by_data_available_event_fd_
            .insert({registration.data_available_event_fd, registration.slot_index})
            .second;
    if (!event_fd_inserted) {
        LOG_ERROR("PublisherDataAvailableEventFdAlreadyTrackedError: event_fd=%d slot=%u",
            registration.data_available_event_fd,
            registration.slot_index);
        close_fd(registration.data_available_event_fd);
        return SubscriberTrackingError::EventFdAlreadyTracked;
    }
    data_available_event_fd_by_subscriber_slot_index_[registration.slot_index] = registration.data_available_event_fd;
    data_available_event_fds_.emplace_back(registration.data_available_event_fd);

    shared_data_->descriptor_read_indices[registration.slot_index].store(
        shared_data_->descriptor_write_index.load(std::memory_order_relaxed),
        std::memory_order_relaxed);
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

Publisher::Publisher(InitialResources resources)
    : shm_name_(std::move(resources.shm_name)),
      shared_data_(resources.shared_data),
      broker_fd_(resources.broker_fd),
      epoll_fd_(resources.epoll_fd),
      space_available_event_fd_(resources.space_available_event_fd),
      publish_work_event_fd_(resources.publish_work_event_fd),
      next_message_size_bytes_(generate_message_size()) {}

Publisher::CreateResult Publisher::create(TopicId topic_id, const char *shm_name) {
    Logger::init(publisher_log_path());

    auto shared_data_result = create_and_map_shared_data(shm_name);
    auto *shared_data_error = std::get_if<CreateError>(&shared_data_result);
    if (shared_data_error != nullptr) {
        return CreateResult{*shared_data_error};
    }

    auto [shm_fd, shared_data] = std::get<SharedDataMapping>(shared_data_result);

    int epoll_fd = -1;
    int broker_fd = -1;
    int space_available_event_fd = -1;
    int publish_work_event_fd = -1;

    auto close_create_resources = [&]() {
        close_fd(publish_work_event_fd);
        close_fd(space_available_event_fd);
        close_fd(broker_fd);
        close_fd(epoll_fd);
        munmap(shared_data, sizeof(SharedData));
        close_fd(shm_fd);
    };

    epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        LOG_ERROR("PublisherEpollCreateError: errno=%d (%s)", errno, strerror(errno));
        close_create_resources();
        return CreateResult{CreateError::EpollCreateFailed};
    }

    auto connect_result = connect_to_broker();
    auto *connect_error = std::get_if<CreateError>(&connect_result);
    if (connect_error != nullptr) {
        close_create_resources();
        return CreateResult{*connect_error};
    }

    broker_fd = std::get<int>(connect_result);

    space_available_event_fd = eventfd(0, EFD_NONBLOCK);
    if (space_available_event_fd < 0) {
        LOG_ERROR("PublisherSpaceAvailableEventFdCreateError: errno=%d (%s)", errno, strerror(errno));
        close_create_resources();
        return CreateResult{CreateError::SpaceAvailableEventFdCreateFailed};
    }
    if (!add_fd_to_epoll(epoll_fd, space_available_event_fd)) {
        LOG_ERROR("PublisherSpaceAvailableEpollAddError: epoll_fd=%d event_fd=%d errno=%d (%s)",
            epoll_fd,
            space_available_event_fd,
            errno,
            strerror(errno));
        close_create_resources();
        return CreateResult{CreateError::SpaceAvailableEpollAddFailed};
    }

    auto registration_result = send_publisher_topic_registration(
        broker_fd,
        topic_id,
        shm_fd,
        space_available_event_fd);
    if (registration_result.has_value()) {
        close_create_resources();
        return CreateResult{*registration_result};
    }

    publish_work_event_fd = eventfd(0, EFD_NONBLOCK);
    if (publish_work_event_fd < 0) {
        LOG_ERROR("PublisherWorkEventFdCreateError: errno=%d (%s)", errno, strerror(errno));
        close_create_resources();
        return CreateResult{CreateError::PublishWorkEventFdCreateFailed};
    }
    if (!add_fd_to_epoll(epoll_fd, publish_work_event_fd)) {
        LOG_ERROR("PublisherWorkEpollAddError: epoll_fd=%d event_fd=%d errno=%d (%s)",
            epoll_fd,
            publish_work_event_fd,
            errno,
            strerror(errno));
        close_create_resources();
        return CreateResult{CreateError::PublishWorkEpollAddFailed};
    }
    if (eventfd_write(publish_work_event_fd, 1) < 0) {
        LOG_ERROR("PublisherWorkInitialSignalError: event_fd=%d errno=%d (%s)",
            publish_work_event_fd,
            errno,
            strerror(errno));
        close_create_resources();
        return CreateResult{CreateError::PublishWorkInitialSignalFailed};
    }

    close_fd(shm_fd);

    InitialResources resources{
        .shm_name = shm_name,
        .shared_data = shared_data,
        .broker_fd = broker_fd,
        .epoll_fd = epoll_fd,
        .space_available_event_fd = space_available_event_fd,
        .publish_work_event_fd = publish_work_event_fd,
    };
    Publisher publisher(std::move(resources));

    auto recv_and_track_result = publisher.receive_and_track_initial_subscriber_registrations();
    if (recv_and_track_result.has_value()) {
        return CreateResult{*recv_and_track_result};
    }

    if (!add_fd_to_epoll(epoll_fd, broker_fd)) {
        LOG_ERROR("PublisherBrokerEpollAddError: epoll_fd=%d broker_fd=%d errno=%d (%s)",
            epoll_fd,
            broker_fd,
            errno,
            strerror(errno));
        return CreateResult{CreateError::BrokerEpollAddFailed};
    }

    return CreateResult{std::move(publisher)};
}

Publisher::Publisher(Publisher &&other) noexcept {
    take_resources_from(std::move(other));
}

Publisher &Publisher::operator=(Publisher &&other) noexcept {
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
    close(broker_fd_);
    close(publish_work_event_fd_);
    close(space_available_event_fd_);

    for (int event_fd : data_available_event_fds_) {
        close_fd(event_fd);
    }

    close(epoll_fd_);

    if (shared_data_ != nullptr) {
        munmap(shared_data_, sizeof(SharedData));
        shared_data_ = nullptr;
    }
    if (!shm_name_.empty()) {
        shm_unlink(shm_name_.c_str());
        shm_name_.clear();
    }
}

void Publisher::take_resources_from(Publisher &&other) {
    shm_name_ = std::move(other.shm_name_);
    other.shm_name_.clear();

    shared_data_ = std::exchange(other.shared_data_, nullptr);

    broker_fd_ = std::exchange(other.broker_fd_, -1);
    epoll_fd_ = std::exchange(other.epoll_fd_, -1);
    publish_work_event_fd_ = std::exchange(other.publish_work_event_fd_, -1);
    space_available_event_fd_ = std::exchange(other.space_available_event_fd_, -1);

    data_available_event_fds_ = std::move(other.data_available_event_fds_);
    other.data_available_event_fds_.clear();

    data_available_event_fd_by_subscriber_slot_index_ =
        std::move(other.data_available_event_fd_by_subscriber_slot_index_);

    subscriber_slot_index_by_data_available_event_fd_ =
        std::move(other.subscriber_slot_index_by_data_available_event_fd_);

    task_num_ = std::exchange(other.task_num_, 0);
    seq_ = std::exchange(other.seq_, 0);
    next_message_size_bytes_ = std::exchange(other.next_message_size_bytes_, 0);
    fill_character_ = std::exchange(other.fill_character_, 'a');
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
    cmsghdr *cmsg) {
    if (body_len != sizeof(uint32_t)) {
        LOG_FATAL("SubscriberDisconnectPayloadSizeError: expected=%zu actual=%zu",
            sizeof(uint32_t),
            body_len);
        close_received_fds(cmsg);
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
        close_received_fds(cmsg);
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
        close_received_fds(cmsg);
        return RunError::BrokerControlMessageProcessingFailed;
    }

    uint32_t subscriber_slot_index;
    memcpy(&subscriber_slot_index, packet + sizeof(BrokerMessageType), sizeof(subscriber_slot_index));

    const int data_available_event_fd = *reinterpret_cast<int *>(CMSG_DATA(cmsg));
    const auto tracking_error = track_subscriber({data_available_event_fd, subscriber_slot_index});
    if (tracking_error.has_value()) {
        return RunError::DynamicSubscriberRegistrationFailed;
    }
    return RunError::None;
}

Publisher::RunError Publisher::process_broker_control_messages() {
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

        const ssize_t received_size = recvmsg(broker_fd_, &msg, 0);

        if (received_size == 0) {
            LOG_ERROR("PublisherBrokerClosed: broker_fd=%d", broker_fd_);
            epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, broker_fd_, nullptr);
            close(broker_fd_);
            broker_fd_ = -1;
            return RunError::BrokerClosed;
        }
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
        if (msg.msg_flags & MSG_CTRUNC) {
            LOG_FATAL("MsgTruncatedError");
            close_received_fds(CMSG_FIRSTHDR(&msg));
            return RunError::BrokerControlMessageProcessingFailed;
        }
        if (received_size < static_cast<ssize_t>(sizeof(BrokerMessageType))) {
            LOG_FATAL("BrokerControlMessageHeaderSizeError: expected_at_least=%zu actual=%zd broker_fd=%d",
                sizeof(BrokerMessageType),
                received_size,
                broker_fd_);
            close_received_fds(CMSG_FIRSTHDR(&msg));
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
                close_received_fds(CMSG_FIRSTHDR(&msg));
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

    uint32_t message_size_bytes = next_message_size_bytes_;
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
Publisher::try_publish_one() {
    uint32_t min_descriptor_read_index = find_slowest_read_index();

    uint32_t message_size_bytes = next_message_size_bytes_;
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
    if (message_size_bytes > 2 * sizeof(int)) {
        memset(shared_data_->data + head_off + 2 * sizeof(int), fill_character_, message_size_bytes - 2 * sizeof(int));
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

    uint32_t candidate = wr + 1;
    if (candidate >= kDescriptorSlotCount) shared_data_->descriptor_write_index.store(candidate % kDescriptorSlotCount, std::memory_order_release);
    else shared_data_->descriptor_write_index.store(candidate, std::memory_order_release);

    for (int data_available_event_fd : data_available_event_fds_) {
        if (eventfd_write(data_available_event_fd, 1) < 0) {
            LOG_ERROR("PublisherDataAvailableSignalError: event_fd=%d errno=%d (%s)",
                data_available_event_fd,
                errno,
                strerror(errno));
            return RunError::PublishAttemptFailed;
        }
    }
    LOG_DEBUG("publisher wrote message: bytes=%u fill=%d seq=%d",
        message_size_bytes,
        static_cast<int>(fill_character_),
        seq_ - 1);

    --task_num_;

    next_message_size_bytes_ = generate_message_size();

    if (fill_character_++ == 'z') fill_character_ = 'a';

    return PublishAttempt::Succeeded;
}

Publisher::RunError Publisher::handle_publish_work_event(bool &publisher_ready) {
    eventfd_t value;
    if (eventfd_read(publish_work_event_fd_, &value) < 0) {
        LOG_ERROR("PublisherWorkEventReadError: event_fd=%d errno=%d (%s)",
            publish_work_event_fd_,
            errno,
            strerror(errno));
        return RunError::PublishWorkEventReadFailed;
    }
    if (!publisher_ready) {
        return RunError::None;
    }

    size_t published_count = 0;
    while (published_count < kMaxPublishesPerWorkEvent && task_num_ > 0) {
        const auto publish_result = try_publish_one();
        if (const auto *publish_error = std::get_if<RunError>(&publish_result)) {
            return *publish_error;
        }

        const PublishAttempt publish_attempt = std::get<PublishAttempt>(publish_result);
        if (publish_attempt == PublishAttempt::Blocked) {
            publisher_ready = false;
            break;
        }

        ++published_count;
    }

    if (!publisher_ready || task_num_ == 0) {
        return RunError::None;
    }
    if (eventfd_write(publish_work_event_fd_, 1) < 0) {
        LOG_ERROR("PublisherWorkRescheduleError: event_fd=%d errno=%d (%s)",
            publish_work_event_fd_,
            errno,
            strerror(errno));
        return RunError::PublishWorkRescheduleFailed;
    }
    return RunError::None;
}

Publisher::RunError Publisher::handle_space_available_event(bool &publisher_ready) {
    eventfd_t value;
    if (eventfd_read(space_available_event_fd_, &value) < 0) {
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
    if (eventfd_write(publish_work_event_fd_, 1) < 0) {
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
    if (eventfd_write(publish_work_event_fd_, 1) < 0) {
        LOG_ERROR("PublisherBrokerResumeError: event_fd=%d errno=%d (%s)",
            publish_work_event_fd_,
            errno,
            strerror(errno));
        return RunError::PublishWorkBrokerResumeFailed;
    }
    return RunError::None;
}

Publisher::RunError Publisher::run() {
    epoll_event events[kMaxEvents]{};
    bool publisher_ready = true;

    while (task_num_ > 0) {
        const int event_count = epoll_wait(epoll_fd_, events, kMaxEvents, -1);
        if (event_count < 0) {
            LOG_ERROR("PublisherEpollWaitError: errno=%d (%s)", errno, strerror(errno));
            return RunError::EpollWaitFailed;
        }
        if (event_count == 0) {
            LOG_ERROR("PublisherUnexpectedEpollTimeoutError");
            return RunError::UnexpectedEpollTimeout;
        }

        for (int event_index = 0; event_index < event_count; ++event_index) {
            const int event_fd = events[event_index].data.fd;
            RunError event_result = RunError::None;

            if (event_fd == publish_work_event_fd_) {
                event_result = handle_publish_work_event(publisher_ready);
            } else if (event_fd == space_available_event_fd_) {
                event_result = handle_space_available_event(publisher_ready);
            } else if (event_fd == broker_fd_) {
                event_result = handle_broker_control_event(publisher_ready);
            }

            if (event_result != RunError::None) {
                return event_result;
            }
        }
    }

    for (int data_available_event_fd : data_available_event_fds_) {
        if (eventfd_write(data_available_event_fd, 1) < 0) {
            LOG_ERROR("PublisherDataAvailableSignalError: event_fd=%d errno=%d (%s)",
                data_available_event_fd,
                errno,
                strerror(errno));
            return RunError::DataAvailableSignalFailed;
        }
    }
    LOG_INFO("publisher completed message production");
    return RunError::None;
}
