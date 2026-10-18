#pragma once

#include "broker_protocol.hpp"
#include "shared_memory_layout.hpp"
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <future>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <unordered_map>
#include <variant>
#include <vector>

struct cmsghdr;

inline constexpr size_t kMaxEvents = 1024;

class Publisher {
  public:
    using Flags = uint32_t;
    inline static constexpr Flags SYNCHRONOUS = 1U << 0;
    inline static constexpr Flags ASYNCHRONOUS = 1U << 1;

    enum class CreateError {
        SharedMemoryUnlinkFailed,
        SharedMemoryOpenFailed,
        SharedMemoryResizeFailed,
        SharedMemoryMapFailed,
        EpollCreateFailed,
        BrokerSocketCreateFailed,
        BrokerConnectFailed,
        BrokerEpollAddFailed,
        SpaceAvailableEventFdCreateFailed,
        SpaceAvailableEpollAddFailed,
        TopicRegistrationSendFailed,
        PublishWorkEventFdCreateFailed,
        PublishWorkEpollAddFailed,
        PublishWorkInitialSignalFailed,
        ShutdownEventFdCreateFailed,
        ShutdownEpollAddFailed,
        WorkerExitEventFdCreateFailed,
        RunThreadCreateFailed,
        PublisherModeFlagsInvalid,
        InitialSubscriberCountReceiveFailed,
        InitialSubscriberRegistrationReceiveFailed,
        InitialSubscriberRegistrationEventFdMissing,
        InitialSubscriberRegistrationTypeInvalid,
        SubscriberSlotOutOfRange,
        SubscriberSlotAlreadyTracked,
        SubscriberEventFdAlreadyTracked,
    };

    enum class PublishAttempt {
        Succeeded,
        Blocked,
    };

    enum class RunError {
        None,
        EpollWaitFailed,
        UnexpectedEpollTimeout,
        PublishWorkEventReadFailed,
        MessageSizeClassInvalid,
        PublishAttemptFailed,
        PublishWorkRescheduleFailed,
        SpaceAvailableEventReadFailed,
        PublishWorkResumeFailed,
        ChunkReferenceReleaseFailed,
        DeadSubscriberReapFailed,
        BrokerClosed,
        BrokerControlMessageTooShort,
        BrokerControlMessageProcessingFailed,
        DynamicSubscriberRegistrationFailed,
        PublishWorkBrokerResumeFailed,
        DataAvailableSignalFailed,
        ShutdownEventReadFailed,
    };

    enum class PublishDisposition {
        Published,
        Enqueued,
        EnqueuedAfterOverwritingOldest,
    };

    enum class PublishError {
        ModeMismatch,
        PayloadTooLarge,
        WorkerExited,
        NotificationFailed,
        Timeout,
        EventWaitFailed,
        BrokerUnavailable,
        DataPathFailed,
    };

    enum class PublishCompletion {
        Published,
        Overwritten,
        WorkerStopped,
        WorkerFailed,
    };

    struct PublishTicket {
        PublishDisposition disposition;
        std::shared_future<PublishCompletion> future;
    };

    using PublishResult = std::variant<PublishTicket, PublishError>;

    struct CreateResult;

    static CreateResult create(TopicId topic_id,
        const char *shm_name,
        Flags publisher_flags = SYNCHRONOUS);

    Publisher(const Publisher &) = delete;
    Publisher &operator=(const Publisher &) = delete;

    Publisher(Publisher &&other);
    Publisher &operator=(Publisher &&other);

    PublishResult publish(std::span<const std::byte> payload, std::chrono::microseconds timeout);
    PublishResult publish(std::span<const std::byte> payload);

    std::shared_future<RunError> worker_exit_future() {
        return worker_exit_future_;
    }

    int worker_exit_event_fd() const {
        return worker_exit_event_fd_;
    }

    ~Publisher();

  private:
    struct SubscriberRegistration {
        int data_available_event_fd;
        uint32_t slot_index;
    };

    enum class SubscriberTrackingError {
        SlotOutOfRange,
        SlotAlreadyTracked,
        EventFdAlreadyTracked,
    };

    struct InitialResources {
        std::string shm_name;
        SharedData *shared_data = nullptr;
        int broker_fd = -1;
        int epoll_fd = -1;
        int space_available_event_fd = -1;
        int publish_work_event_fd = -1;
        int shutdown_event_fd = -1;
        int worker_exit_event_fd = -1;
    };

    enum class PublishReadiness {
        Ready,
        Blocked,
        NoMessage,
    };

    struct PendingMessage {
        std::vector<std::byte> payload;
        std::promise<PublishCompletion> completion_promise;

        PendingMessage(std::span<const std::byte> bytes) : payload(bytes.begin(), bytes.end()) {};
    };

    enum class InFlightState {
        Publishing,
        Blocked,
    };

    struct InFlightMessage {
        PendingMessage message;
        InFlightState state;
    };

    enum class WorkerStopIntent {
        Destroy,
        Transfer,
    };

    explicit Publisher(InitialResources resources, Flags publisher_flags);

    void release_resources();
    void take_resources_from(Publisher &&other);
    void reset_worker_exit_notification();
    void start_run_thread();
    bool stop_run_thread(WorkerStopIntent stop_intent);

    std::variant<SubscriberRegistration, CreateError> receive_subscriber_registration();
    std::optional<SubscriberTrackingError> track_subscriber(SubscriberRegistration registration);
    std::optional<CreateError> receive_and_track_initial_subscriber_registrations();

    uint32_t find_slowest_read_index();

    RunError release_chunk_references_in_descriptor_range(size_t start, size_t end, uint32_t subscriber_reference_bit);
    RunError reap_dead_subscribers(uint32_t slot_index);
    RunError handle_subscriber_disconnected_message(const char packet[], size_t body_len, cmsghdr *cmsg);
    RunError handle_subscriber_registered_message(const char packet[], size_t body_len, cmsghdr *cmsg);
    RunError process_broker_control_messages();

    std::variant<PublishReadiness, RunError> can_publish_next_message();
    std::variant<PublishAttempt, RunError> try_publish_one(std::span<const std::byte> payload);
    RunError handle_publish_work_event(bool &publisher_ready);
    RunError handle_space_available_event(bool &publisher_ready);
    RunError handle_broker_control_event(bool &publisher_ready);

    RunError run();

    std::vector<int> data_available_event_fds_{};

    std::unordered_map<int, size_t> subscriber_slot_index_by_data_available_event_fd_{};
    std::array<int, kMaxSubscribers> data_available_event_fd_by_subscriber_slot_index_{};

    Flags flags_ = SYNCHRONOUS;

    std::thread run_thread_;
    std::atomic<bool> run_thread_active_{false};
    std::promise<RunError> worker_exit_promise_;
    std::shared_future<RunError> worker_exit_future_;
    WorkerStopIntent worker_stop_intent_ = WorkerStopIntent::Destroy;

    std::deque<PendingMessage> pending_messages_;
    std::mutex publish_state_mutex_;

    std::optional<InFlightMessage> in_flight_message_;

    std::string shm_name_;
    SharedData *shared_data_;

    int broker_fd_ = -1;
    int epoll_fd_ = -1;
    int space_available_event_fd_ = -1;
    int publish_work_event_fd_ = -1;
    int shutdown_event_fd_ = -1;
    int worker_exit_event_fd_ = -1;

    int seq_ = 0;

    static constexpr size_t kMessageHeaderSizeBytes = 2 * sizeof(int);
    static constexpr size_t kMaxPayloadSizeBytes =
        kClassSizeBytes[kClassCount - 1] - kMessageHeaderSizeBytes;
    static constexpr size_t kMaxOutstandingMessages = 64;
    static constexpr size_t kMaxPublishesPerWorkEvent = 64;
};

struct Publisher::CreateResult {
    std::variant<Publisher, CreateError> value;
};
