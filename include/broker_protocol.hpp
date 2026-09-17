#pragma once

#include <cstddef>
#include <cstdint>

enum class BrokerMessageType : uint8_t {
    PublisherTopicRegistration = 0,
    SubscriberTopicRegistration,
    SubscriberDisconnected,
    SubscriberEventFdAndSlot,
    PublisherDisconnected,
    SubscriberAttachmentBatch,
};

enum class BrokerRole : uint8_t {
    Subscriber = 1,
    Publisher = 2,
};

enum class TopicId : uint8_t {
    Invalid = 0,
    Camera = 1,
    Lidar = 2,
    VehicleState = 3,
};

using AttachmentId = uint64_t;

inline constexpr size_t kMaxMessageSize = 1024;
// 订阅者注册包最多携带 5 个 topic_id，topic_count 指定有效个数。
inline constexpr uint32_t kMaxSubscriberTopicCount = 5;
inline constexpr size_t kSubscriberDisconnectedMessageSize = sizeof(BrokerMessageType) + sizeof(uint32_t);
inline constexpr size_t kSubscriberEventFdAndSlotMessageSize = sizeof(BrokerMessageType) + sizeof(uint32_t);
inline constexpr size_t kSubscriberTopicRegistrationMessageSize =
    sizeof(BrokerMessageType) + sizeof(uint32_t) + kMaxSubscriberTopicCount * sizeof(TopicId);
inline constexpr size_t kPublisherTopicRegistrationMessageSize = sizeof(BrokerMessageType) + sizeof(TopicId);
inline constexpr size_t kPublisherDisconnectedMessageSize = sizeof(BrokerMessageType) + sizeof(AttachmentId);

inline constexpr size_t kSubscriberAttachmentBatchMessageMinSize = sizeof(BrokerMessageType);
inline constexpr size_t kSubscriberAttachmentMetadataSize = sizeof(AttachmentId) + sizeof(TopicId) + sizeof(uint32_t);
inline constexpr size_t kMaxAttachmentsPerBatch = 10;

inline constexpr AttachmentId kInvalidAttachmentId = 0;
