#pragma once

#include "broker_protocol.hpp"
#include "shared_memory_layout.hpp"
#include <cstddef>
#include <span>
#include <sys/socket.h>
#include <unordered_map>
#include <variant>

inline constexpr size_t kMaxEvents = 1024;

class Subscriber {
  public:
    enum class CreateError {
        EpollCreateFailed,
        BrokerSocketCreateFailed,
        BrokerConnectFailed,
        BrokerEpollRegistrationFailed,
        TopicRegistrationSendFailed,
        TopicRegistrationCountExceeded,
    };

    enum class RunError {
        None,
        EpollWaitFailed,
        UnexpectedEpollTimeout,
        DataAvailableEventAttachmentMissing,
        DataAvailableEventReadFailed,
        WrappedDescriptorReadFailed,
        ContiguousMessageConsumptionFailed,
        BrokerControlMessageProcessingFailed,
    };

    struct CreateResult;

    static CreateResult create(std::span<const TopicId> topic_ids);

    Subscriber(const Subscriber &) = delete;
    Subscriber &operator=(const Subscriber &) = delete;

    Subscriber(Subscriber &&other) noexcept;
    Subscriber &operator=(Subscriber &&other) noexcept;

    RunError run();

    ~Subscriber();

  private:
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

    Subscriber(int epoll_fd, int broker_fd);

    void release_resources();
    void take_resources_from(Subscriber &&other);

    void release_publisher_attachment(PublisherAttachment &attachment);

    bool consume_contiguous_messages(PublisherAttachment &publisher_attachment);
    bool read_data(PublisherAttachment &publisher_attachment,
        MessageDescriptor desc_ring[],
        const size_t descriptor_count);

    bool process_broker_control_messages();
    bool install_subscriber_attachment_batch(const char packet[], size_t body_len, cmsghdr *cmsg);
    bool handle_publisher_disconnected(const char packet[], size_t body_len);

    void initialize_subscriber_read_index(PublisherAttachment &publisher_attachment);

    RunError handle_data_available_event(const int event_fd);

    void log_chunks_in_use();

    int epoll_fd_ = -1;
    int broker_fd_ = -1;

    std::unordered_map<int, PublisherAttachment> publisher_attachment_by_data_available_event_fd_;
    std::unordered_map<AttachmentId, int> data_available_event_fd_by_attachment_id_;
};

struct Subscriber::CreateResult {
    std::variant<Subscriber, Subscriber::CreateError> value;
};
