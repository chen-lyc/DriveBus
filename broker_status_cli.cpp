#include "include/broker_protocol.hpp"

#include <array>
#include <cstdio>
#include <cstring>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

int main() {
    constexpr char kBrokerSocketPath[] = "/tmp/broker.sock";

    if (sizeof(kBrokerSocketPath) > sizeof(sockaddr_un::sun_path)) {
        std::fprintf(stderr, "BrokerStatusSocketPathTooLongError\n");
        return 1;
    }

    const int broker_fd = socket(AF_UNIX, SOCK_SEQPACKET, 0);
    if (broker_fd < 0) {
        std::perror("BrokerStatusSocketCreateError");
        return 1;
    }

    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::memcpy(addr.sun_path, kBrokerSocketPath, sizeof(kBrokerSocketPath));

    if (connect(broker_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
        std::perror("BrokerStatusConnectError");
        close(broker_fd);
        return 1;
    }

    const BrokerMessageType query_type = BrokerMessageType::StatusQuery;
    const ssize_t sent_size = send(broker_fd, &query_type, sizeof(query_type), 0);
    if (sent_size != static_cast<ssize_t>(kStatusQuerySize)) {
        if (sent_size < 0) std::perror("BrokerStatusQuerySendError");
        else std::fprintf(stderr, "BrokerStatusQuerySendSizeError: expected=%zu actual=%zd\n",
            kStatusQuerySize, sent_size);
        close(broker_fd);
        return 1;
    }

    std::array<char, kMaxStatusSnapshotMessageSize> snapshot_packet{};
    iovec iov{};
    iov.iov_base = snapshot_packet.data();
    iov.iov_len = snapshot_packet.size();

    msghdr message{};
    message.msg_iov = &iov;
    message.msg_iovlen = 1;

    const ssize_t received_size = recvmsg(broker_fd, &message, 0);
    if (received_size < 0) {
        std::perror("BrokerStatusSnapshotReceiveError");
        close(broker_fd);
        return 1;
    }
    if (received_size == 0) {
        std::fprintf(stderr, "BrokerStatusBrokerClosedError\n");
        close(broker_fd);
        return 1;
    }
    if ((message.msg_flags & MSG_TRUNC) != 0) {
        std::fprintf(stderr, "BrokerStatusSnapshotTruncatedError\n");
        close(broker_fd);
        return 1;
    }
    if (received_size < static_cast<ssize_t>(sizeof(BrokerMessageType))) {
        std::fprintf(stderr, "BrokerStatusSnapshotTooShortError: actual=%zd\n", received_size);
        close(broker_fd);
        return 1;
    }

    BrokerMessageType response_type;
    std::memcpy(&response_type, snapshot_packet.data(), sizeof(response_type));
    if (response_type != BrokerMessageType::StatusSnapshot) {
        std::fprintf(stderr, "BrokerStatusUnexpectedResponseTypeError\n");
        close(broker_fd);
        return 1;
    }

    const size_t payload_size = static_cast<size_t>(received_size) - sizeof(response_type);
    const size_t written_size = std::fwrite(
        snapshot_packet.data() + sizeof(response_type), 1, payload_size, stdout);
    if (written_size != payload_size) {
        std::perror("BrokerStatusOutputWriteError");
        close(broker_fd);
        return 1;
    }

    close(broker_fd);
    return 0;
}
