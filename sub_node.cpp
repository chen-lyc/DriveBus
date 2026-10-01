#include "include/subscriber.hpp"
#include <array>
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
    const Subscriber::RunError run_error = subscriber.run();
    if (run_error != Subscriber::RunError::None) {
        std::cerr << "subscriber run error=" << static_cast<int>(run_error) << '\n';
        return 2;
    }

    return 0;
}
