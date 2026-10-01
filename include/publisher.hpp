#pragma once

#include "broker_protocol.hpp"
#include "shared_memory_layout.hpp"
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

struct cmsghdr;

inline constexpr size_t kMaxEvents = 1024;

class Publisher {
  public:
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
    };

    struct CreateResult;

    static CreateResult create(TopicId topic_id, const char *shm_name);

    Publisher(const Publisher &) = delete;
    Publisher &operator=(const Publisher &) = delete;

    Publisher(Publisher &&other) noexcept;
    Publisher &operator=(Publisher &&other) noexcept;

    RunError run();

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
    };

    explicit Publisher(InitialResources resources);

    void release_resources();
    void take_resources_from(Publisher &&other);

    std::variant<SubscriberRegistration, CreateError> receive_subscriber_registration();
    std::optional<SubscriberTrackingError> track_subscriber(SubscriberRegistration registration);
    std::optional<CreateError> receive_and_track_initial_subscriber_registrations();

    enum class PublishReadiness {
        Ready,
        Blocked,
    };

    uint32_t find_slowest_read_index();

    RunError release_chunk_references_in_descriptor_range(size_t start, size_t end, uint32_t subscriber_reference_bit);
    RunError reap_dead_subscribers(uint32_t slot_index);
    RunError handle_subscriber_disconnected_message(const char packet[], size_t body_len, cmsghdr *cmsg);
    RunError handle_subscriber_registered_message(const char packet[], size_t body_len, cmsghdr *cmsg);
    RunError process_broker_control_messages();

    std::variant<PublishReadiness, RunError> can_publish_next_message();
    std::variant<PublishAttempt, RunError> try_publish_one();
    RunError handle_publish_work_event(bool &publisher_ready);
    RunError handle_space_available_event(bool &publisher_ready);
    RunError handle_broker_control_event(bool &publisher_ready);

    std::vector<int> data_available_event_fds_{};

    std::unordered_map<int, size_t> subscriber_slot_index_by_data_available_event_fd_{};
    std::array<int, kMaxSubscribers> data_available_event_fd_by_subscriber_slot_index_{};

    std::string shm_name_;
    SharedData *shared_data_;

    int broker_fd_ = -1;
    int epoll_fd_ = -1;
    int space_available_event_fd_ = -1;
    int publish_work_event_fd_ = -1;

    int task_num_ = 26 * 3000;
    int seq_ = 0;

    uint32_t next_message_size_bytes_;

    char fill_character_ = 'a';

    static constexpr size_t kMaxPublishesPerWorkEvent = 64;
};

struct Publisher::CreateResult {
    std::variant<Publisher, CreateError> value;
};
