#include "include/publisher.hpp"
#include <iostream>

int main() {
    constexpr char kSharedMemoryName[] = "/drivebus_pub_node_camera";

    auto result = Publisher::create(TopicId::Camera, kSharedMemoryName);
    if (std::holds_alternative<Publisher::CreateError>(result.value)) {
        std::cerr << "publisher create error="
                  << static_cast<int>(std::get<Publisher::CreateError>(result.value))
                  << '\n';
        return 1;
    }

    auto &publisher = std::get<Publisher>(result.value);
    const Publisher::RunError run_error = publisher.run();
    if (run_error != Publisher::RunError::None) {
        std::cerr << "publisher run error=" << static_cast<int>(run_error) << '\n';
        return 2;
    }

    return 0;
}
