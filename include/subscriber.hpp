#pragma once

#include "broker_protocol.hpp"
#include "shared_memory_layout.hpp"
#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <future>
#include <span>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <thread>
#include <unordered_map>
#include <variant>
#include <vector>

inline constexpr size_t kMaxEvents = 1024;

class Subscriber {
  public:
    using Flags = uint32_t;
    inline static constexpr Flags SYNCHRONOUS = 1U << 0;
    inline static constexpr Flags ASYNCHRONOUS = 1U << 1;

    enum class CreateError {
        EpollCreateFailed,
        BrokerSocketCreateFailed,
        BrokerConnectFailed,
        BrokerEpollRegistrationFailed,
        TopicRegistrationSendFailed,
        TopicRegistrationCountExceeded,
        SubscriberModeFlagsInvalid,
        AsyncCallbackMissing,
        ShutdownEventFdCreateFailed,
        ShutdownEpollRegistrationFailed,
        WorkerExitEventFdCreateFailed,
        RunThreadCreateFailed,
        TransferEventDrainFailed,
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
        PublisherDisconnectedPayloadSizeMismatch,
        PublisherDisconnectedAttachmentMissing,
        PublisherDisconnectedEventFdMissing,
        ShutdownEventReadFailed,
        WorkerException,
    };

    enum class SubscribeError {
        Timeout,
        EventWaitFailed,
        BrokerUnavailable,
        DataPathFailed,
    };

    using Message = std::vector<std::byte>;
    using MessageBatch = std::vector<Message>;

    using SubscribeResult = std::variant<MessageBatch, SubscribeError>;

    struct CreateResult;

    static CreateResult create(std::span<const TopicId> topic_ids,
        Flags flags = SYNCHRONOUS,
        std::function<void(char *, size_t)> callback = {});

    Subscriber(const Subscriber &) = delete;
    Subscriber &operator=(const Subscriber &) = delete;

    // 移动/析构时不得并发调用成员函数，也不得从本对象的回调中执行。
    // 移动可能因通知清理或 worker 重启失败抛出 std::system_error（含派生异常）。
    // 内存分配失败也可能抛出 std::bad_alloc，因此移动操作不保证 noexcept。
    Subscriber(Subscriber &&other);
    Subscriber &operator=(Subscriber &&other);

    SubscribeResult subscribe(std::chrono::microseconds timeout);

    std::shared_future<RunError> worker_exit_future() {
        return worker_exit_future_;
    }

    int worker_exit_event_fd() {
        return worker_exit_event_fd_;
    }

    ~Subscriber();

  private:
    struct InitialResources {
        int epoll_fd = -1;
        int broker_fd = -1;
        int shutdown_event_fd = -1;
        int worker_exit_event_fd = -1;
    };

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

    Subscriber(InitialResources resources, Flags flags, std::function<void(char *, size_t)> callback);

    void release_resources();
    void take_resources_from(Subscriber &&other);
    void reset_worker_exit_notification();
    void start_run_thread();
    bool stop_run_thread();

    void release_publisher_attachment(PublisherAttachment &attachment);

    void initialize_subscriber_read_index(PublisherAttachment &publisher_attachment);

    std::variant<MessageBatch, RunError> consume_contiguous_messages(PublisherAttachment &publisher_attachment);
    std::variant<MessageBatch, RunError> read_data(PublisherAttachment &publisher_attachment,
        MessageDescriptor desc_ring[],
        const size_t descriptor_count);
    std::variant<MessageBatch, RunError> handle_data_available_event(const int event_fd);

    bool process_broker_control_messages();
    bool install_subscriber_attachment_batch(const char packet[], size_t body_len, cmsghdr *cmsg);
    RunError handle_publisher_disconnected(const char packet[], size_t body_len);

    RunError run();

#ifdef ENABLE_DEBUG_CHECKS
    void log_chunks_in_use();
#endif

    Flags flags_ = SYNCHRONOUS;

    std::thread run_thread_;
    std::atomic<bool> run_thread_active_{false};
    std::promise<RunError> worker_exit_promise_;
    std::shared_future<RunError> worker_exit_future_;

    int epoll_fd_ = -1;
    int broker_fd_ = -1;
    int shutdown_event_fd_ = -1;
    int worker_exit_event_fd_ = -1;

    int event_count_ = 0;
    int event_index_ = 0;
    epoll_event events_[kMaxEvents]{};

    std::function<void(char *, size_t)> callback_;

    std::unordered_map<int, PublisherAttachment> publisher_attachment_by_data_available_event_fd_;
    std::unordered_map<AttachmentId, int> data_available_event_fd_by_attachment_id_;
};

struct Subscriber::CreateResult {
    std::variant<Subscriber, Subscriber::CreateError> value;
};
