#include "include/subscriber.hpp"
#include <array>
#include <chrono>
#include <iostream>

int main() {
    constexpr std::array<TopicId, 1> topic_ids = {TopicId::Camera};

    auto result = Subscriber::create(topic_ids);
    if (std::holds_alternative<Subscriber::CreateError>(result.value)) {
        std::cerr << "subscriber create error="
                  << static_cast<int>(std::get<Subscriber::CreateError>(result.value))
                  << '\n';
        return 1;
    }

    auto &subscriber = std::get<Subscriber>(result.value);
    while (true) {
        auto subscribe_result = subscriber.subscribe(std::chrono::seconds{1});
        if (const auto *subscribe_error = std::get_if<Subscriber::SubscribeError>(&subscribe_result)) {
            if (*subscribe_error == Subscriber::SubscribeError::Timeout) {
                continue;
            }

            std::cerr << "subscriber subscribe error=" << static_cast<int>(*subscribe_error) << '\n';
            return 2;
        }

        for (const auto &message : std::get<Subscriber::MessageBatch>(subscribe_result)) {
            if (!message.empty()) {
                std::cout.write(reinterpret_cast<const char *>(message.data()),
                    static_cast<std::streamsize>(message.size()));
            }
            std::cout << '\n';
        }
    }
}
