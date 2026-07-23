#pragma once

#include <cstddef>
#include <cstdint>

enum class BrokerMessageType : uint8_t {
    PublisherRegistration = 1,
    SubscriberRegistration = 2,
    SubscriberDisconnected = 3,
};

enum class BrokerRole : uint8_t {
    Subscriber = 1,
    Publisher = 2,
};

inline constexpr size_t kMaxMessageSize = 1024;
inline constexpr size_t kSubscriberDisconnectedMessageSize = sizeof(BrokerMessageType) + sizeof(uint32_t);