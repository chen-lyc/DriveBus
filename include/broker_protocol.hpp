#pragma once

#include <cstddef>
#include <cstdint>

enum class BrokerMessageType : uint8_t {
    PublisherTopicRegistration = 1,
    SubscriberTopicRegistration = 2,
    SubscriberDisconnected = 3,
    SubscriberEventFdAndSlot = 4,
    PublisherTopicShmFd = 5,
    SubscriberPublisherCount = 6,
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

inline constexpr size_t kMaxMessageSize = 1024;
// 订阅者注册包最多携带 5 个 topic_id，topic_count 指定有效个数。
inline constexpr uint32_t kMaxSubscriberTopicCount = 5;
inline constexpr size_t kSubscriberDisconnectedMessageSize = sizeof(BrokerMessageType) + sizeof(uint32_t);
inline constexpr size_t kSubscriberEventFdAndSlotMessageSize = sizeof(BrokerMessageType) + sizeof(uint32_t);
inline constexpr size_t kSubscriberPublisherCountMessageSize = sizeof(BrokerMessageType) + sizeof(uint32_t);
inline constexpr size_t kSubscriberTopicRegistrationMessageSize =
    sizeof(BrokerMessageType) + sizeof(uint32_t) + kMaxSubscriberTopicCount * sizeof(TopicId);
inline constexpr size_t kPublisherTopicRegistrationMessageSize = sizeof(BrokerMessageType) + sizeof(TopicId);
inline constexpr size_t kPublisherTopicShmFdMessageSize = sizeof(BrokerMessageType) + sizeof(TopicId);
