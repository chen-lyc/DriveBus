#include "include/publisher.hpp"
#include <chrono>
#include <iostream>
#include <span>
#include <string_view>

int main() {
    constexpr char kSharedMemoryName[] = "/drivebus_pub_node_camera";

    auto result = Publisher::create(TopicId::Camera, kSharedMemoryName, Publisher::SYNCHRONOUS);
    if (std::holds_alternative<Publisher::CreateError>(result.value)) {
        std::cerr << "publisher create error="
                  << static_cast<int>(std::get<Publisher::CreateError>(result.value))
                  << '\n';
        return 1;
    }

    auto &publisher = std::get<Publisher>(result.value);
    constexpr std::string_view kPayload = "pub_node publish payload";
    const auto payload = std::as_bytes(std::span{kPayload.data(), kPayload.size()});

    const auto publish_result = publisher.publish(payload, std::chrono::seconds(1));
    if (const auto *publish_error = std::get_if<Publisher::PublishError>(&publish_result)) {
        std::cerr << "publisher publish error=" << static_cast<int>(*publish_error) << '\n';
        return 2;
    }

    return 0;
}
