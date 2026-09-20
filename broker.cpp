#include "include/broker_protocol.hpp"
#include "include/shared_memory_layout.hpp"
#include "include/shared_memory_layout_helpers.hpp"
#include "include/fd_helpers.hpp"
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <cstddef>
#include <iomanip>
#include <iostream>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/signalfd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <fcntl.h>
#include <stdint.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>
using namespace std;

const char *get_topic_name_from_id(TopicId topic_id) {
    switch (topic_id) {
        case TopicId::Camera: return "Camera";
        case TopicId::Lidar: return "Lidar";
        case TopicId::VehicleState: return "VehicleState";
        case TopicId::Invalid: return "Invalid";
    }
    return "Unknown";
}

const char *get_broker_role_name(BrokerRole role) {
    switch (role) {
        case BrokerRole::Subscriber: return "Subscriber";
        case BrokerRole::Publisher: return "Publisher";
    }
    return "Unknown";
}

inline constexpr array<TopicId, 4> kKnownTopicIds{
    TopicId::Camera,
    TopicId::Lidar,
    TopicId::VehicleState,
    TopicId::Invalid,
};

AttachmentId next_attachment_id = 1;

struct SubscriberAttachment {
    AttachmentId attachment_id;
    TopicId topic_id;
    int publisher_fd;
    int event_fd;
    uint32_t slot_index;
};

void send_subscriber_registered_message(int conn_fd, int fd_to_send, uint32_t send_subscriber_slot_index) {
    char packet[kSubscriberEventFdAndSlotMessageSize];
    const BrokerMessageType type = BrokerMessageType::SubscriberEventFdAndSlot;
    memcpy(packet, &type, sizeof(BrokerMessageType));
    memcpy(packet + 1, &send_subscriber_slot_index, sizeof(send_subscriber_slot_index));

    send_packet_with_fd(conn_fd, packet, sizeof(packet), fd_to_send);
}

ssize_t send_subscriber_attachment_batch_message(int subscriber_fd, const vector<SubscriberAttachment> &subscriber_attachments, const vector<int> &shm_fds) {
    if (shm_fds.size() != subscriber_attachments.size()) {
        exit(1);
    }

    if (subscriber_attachments.size() > kMaxAttachmentsPerBatch) {
        cout << "publish too much" << endl;
    }

    vector<char> packet(kSubscriberAttachmentBatchMessageMinSize + subscriber_attachments.size() * kSubscriberAttachmentMetadataSize);

    const BrokerMessageType type = BrokerMessageType::SubscriberAttachmentBatch;
    memcpy(packet.data(), &type, sizeof(BrokerMessageType));

    vector<int> attachment_fds;
    attachment_fds.reserve(2 * subscriber_attachments.size());

    size_t packet_offset = kSubscriberAttachmentBatchMessageMinSize;

    for (size_t attachment_index = 0; attachment_index < subscriber_attachments.size(); ++attachment_index) {
        const auto &subscriber_attachment = subscriber_attachments[attachment_index];
        memcpy(packet.data() + packet_offset, &subscriber_attachment.attachment_id, sizeof(AttachmentId));
        packet_offset += sizeof(AttachmentId);

        memcpy(packet.data() + packet_offset, &subscriber_attachment.topic_id, sizeof(subscriber_attachment.topic_id));
        packet_offset += sizeof(subscriber_attachment.topic_id);

        memcpy(packet.data() + packet_offset, &subscriber_attachment.slot_index, sizeof(subscriber_attachment.slot_index));
        packet_offset += sizeof(subscriber_attachment.slot_index);

        attachment_fds.emplace_back(shm_fds[attachment_index]);
        attachment_fds.emplace_back(subscriber_attachment.event_fd);
    }

    return send_packet_with_fds(subscriber_fd, packet.data(), packet.size(), attachment_fds);
}

using AvailableSubscriberSlotMinHeap = std::priority_queue<size_t, std::vector<size_t>, std::greater<size_t>>;
std::unordered_map<TopicId, std::unordered_map<int, AvailableSubscriberSlotMinHeap>> available_subscriber_slots_by_topic_id_and_publisher_fd;

void init_available_subscriber_slots(AvailableSubscriberSlotMinHeap &available_subscriber_slots) {
    for (size_t slot_index = 0; slot_index < kMaxSubscribers; ++slot_index) {
        available_subscriber_slots.emplace(slot_index);
    }
}

std::unordered_map<int, std::vector<SubscriberAttachment>> subscriber_attachments_by_subscriber_fd{};
std::unordered_map<int, BrokerRole> role_by_fd;

std::unordered_map<int, int> shm_fd_by_publisher_fd;
std::unordered_map<int, std::vector<TopicId>> topic_ids_by_subscriber_fd;
std::unordered_map<TopicId, std::vector<int>> publisher_fds_by_topic_id;

std::unordered_map<TopicId, std::vector<int>> subscriber_fds_by_topic_id;

std::unordered_set<int> client_fds;

string format_topic_ids(const vector<TopicId> &topic_ids) {
    if (topic_ids.empty()) return "-";

    ostringstream text;
    for (size_t topic_index = 0; topic_index < topic_ids.size(); ++topic_index) {
        if (topic_index != 0) text << ',';
        text << get_topic_name_from_id(topic_ids[topic_index]);
    }
    return text.str();
}

string format_fd_list(vector<int> fds) {
    if (fds.empty()) return "-";

    sort(fds.begin(), fds.end());
    ostringstream text;
    for (size_t fd_index = 0; fd_index < fds.size(); ++fd_index) {
        if (fd_index != 0) text << ',';
        text << fds[fd_index];
    }
    return text.str();
}

vector<TopicId> find_publisher_topic_ids(int publisher_fd) {
    vector<TopicId> topic_ids;
    for (TopicId topic_id : kKnownTopicIds) {
        auto publishers_it = publisher_fds_by_topic_id.find(topic_id);
        if (publishers_it != publisher_fds_by_topic_id.end() &&
            find(publishers_it->second.begin(), publishers_it->second.end(), publisher_fd) != publishers_it->second.end()) {
            topic_ids.emplace_back(topic_id);
        }
    }
    return topic_ids;
}

string format_free_slots(AvailableSubscriberSlotMinHeap free_slots) {
    if (free_slots.empty()) return "-";

    ostringstream text;
    bool first_slot = true;
    while (!free_slots.empty()) {
        if (!first_slot) text << ',';
        text << free_slots.top();
        free_slots.pop();
        first_slot = false;
    }
    return text.str();
}

string format_occupied_slots(TopicId topic_id, int publisher_fd) {
    vector<pair<uint32_t, int>> occupied_slots;
    for (const auto &[subscriber_fd, attachments] : subscriber_attachments_by_subscriber_fd) {
        for (const SubscriberAttachment &attachment : attachments) {
            if (attachment.topic_id == topic_id && attachment.publisher_fd == publisher_fd) {
                occupied_slots.emplace_back(attachment.slot_index, subscriber_fd);
            }
        }
    }
    if (occupied_slots.empty()) return "-";

    sort(occupied_slots.begin(), occupied_slots.end());
    ostringstream text;
    for (size_t slot_index = 0; slot_index < occupied_slots.size(); ++slot_index) {
        if (slot_index != 0) text << ',';
        text << occupied_slots[slot_index].first << "->sub=" << occupied_slots[slot_index].second;
    }
    return text.str();
}

string build_status_snapshot_payload() {
    ostringstream text;
    text << "BROKER STATUS\n"
         << "connection_state=registered_control_connection\n"
         << "connection_count=" << role_by_fd.size() << "\n";

    vector<int> connection_fds;
    connection_fds.reserve(role_by_fd.size());
    for (const auto &[fd, role] : role_by_fd) {
        connection_fds.emplace_back(fd);
    }
    sort(connection_fds.begin(), connection_fds.end());

    text << "\nCONNECTIONS\n"
         << left << setw(6) << "FD"
         << setw(12) << "ROLE"
         << setw(13) << "STATE"
         << setw(25) << "TOPICS"
         << "ATTACHMENTS\n";
    for (int fd : connection_fds) {
        const BrokerRole role = role_by_fd.at(fd);
        vector<TopicId> topic_ids;
        string attachment_count = "-";

        if (role == BrokerRole::Subscriber) {
            auto topics_it = topic_ids_by_subscriber_fd.find(fd);
            if (topics_it != topic_ids_by_subscriber_fd.end()) topic_ids = topics_it->second;

            auto attachments_it = subscriber_attachments_by_subscriber_fd.find(fd);
            attachment_count = attachments_it == subscriber_attachments_by_subscriber_fd.end()
                ? "0"
                : to_string(attachments_it->second.size());
        } else if (role == BrokerRole::Publisher) {
            topic_ids = find_publisher_topic_ids(fd);
        }

        text << left << setw(6) << fd
             << setw(12) << get_broker_role_name(role)
             << setw(13) << "registered"
             << setw(25) << format_topic_ids(topic_ids)
             << attachment_count << '\n';
    }

    text << "\nTOPIC ROUTING\n"
         << left << setw(15) << "TOPIC"
         << setw(25) << "PUBLISHERS"
         << "SUBSCRIBERS\n";
    for (TopicId topic_id : kKnownTopicIds) {
        const auto publishers_it = publisher_fds_by_topic_id.find(topic_id);
        const auto subscribers_it = subscriber_fds_by_topic_id.find(topic_id);
        const vector<int> no_fds;
        const vector<int> &publisher_fds = publishers_it == publisher_fds_by_topic_id.end()
            ? no_fds
            : publishers_it->second;
        const vector<int> &subscriber_fds = subscribers_it == subscriber_fds_by_topic_id.end()
            ? no_fds
            : subscribers_it->second;

        text << left << setw(15) << get_topic_name_from_id(topic_id)
             << setw(25) << format_fd_list(publisher_fds)
             << format_fd_list(subscriber_fds) << '\n';
    }

    text << "\nSLOTS\n"
         << left << setw(15) << "TOPIC"
         << setw(15) << "PUBLISHER_FD"
         << setw(28) << "OCCUPIED"
         << "FREE\n";
    bool has_slot_rows = false;
    for (TopicId topic_id : kKnownTopicIds) {
        const auto slots_by_publisher_it = available_subscriber_slots_by_topic_id_and_publisher_fd.find(topic_id);
        if (slots_by_publisher_it == available_subscriber_slots_by_topic_id_and_publisher_fd.end()) continue;

        vector<int> publisher_fds;
        publisher_fds.reserve(slots_by_publisher_it->second.size());
        for (const auto &[publisher_fd, free_slots] : slots_by_publisher_it->second) {
            publisher_fds.emplace_back(publisher_fd);
        }
        sort(publisher_fds.begin(), publisher_fds.end());

        for (int publisher_fd : publisher_fds) {
            const auto free_slots_it = slots_by_publisher_it->second.find(publisher_fd);
            text << left << setw(15) << get_topic_name_from_id(topic_id)
                 << setw(15) << publisher_fd
                 << setw(28) << format_occupied_slots(topic_id, publisher_fd)
                 << format_free_slots(free_slots_it->second) << '\n';
            has_slot_rows = true;
        }
    }
    if (!has_slot_rows) text << "-\n";

    return text.str();
}

void handle_subsciber_disconect(int subscriber_fd) {
    auto subscriber_attachments_it = subscriber_attachments_by_subscriber_fd.find(subscriber_fd);
    if (subscriber_attachments_it == subscriber_attachments_by_subscriber_fd.end()) {
        cerr << "SubscriberDisconnectAttachmentsMissingError: subscriber_fd="
             << subscriber_fd << endl;
        exit(1);
    }

    auto subscriber_topics_it = topic_ids_by_subscriber_fd.find(subscriber_fd);
    if (subscriber_topics_it == topic_ids_by_subscriber_fd.end()) {
        cerr << "SubscriberDisconnectTopicsMapMissingError: subscriber_fd="
             << subscriber_fd << endl;
        exit(1);
    }

    for (const SubscriberAttachment &subscriber_attachment : subscriber_attachments_it->second) {
        char packet[kSubscriberDisconnectedMessageSize];

        const BrokerMessageType message_type = BrokerMessageType::SubscriberDisconnected;
        memcpy(packet, &message_type, sizeof(BrokerMessageType));
        memcpy(packet + 1, &subscriber_attachment.slot_index, sizeof(subscriber_attachment.slot_index));

        ssize_t n = send(subscriber_attachment.publisher_fd, packet, kSubscriberDisconnectedMessageSize, 0);
        if (n < 0) {
            cerr << "SubscriberDisconnectSendError: publisher_fd=" << subscriber_attachment.publisher_fd
                 << ", slot=" << subscriber_attachment.slot_index << ", errno=" << errno
                 << " (" << strerror(errno) << ')' << endl;
            exit(1);
        }

        auto available_subscriber_slots_by_topic_id_it =
            available_subscriber_slots_by_topic_id_and_publisher_fd.find(subscriber_attachment.topic_id);
        if (available_subscriber_slots_by_topic_id_it == available_subscriber_slots_by_topic_id_and_publisher_fd.end()) {
            cerr << "SubscriberDisconnectAvailableTopicMapMissingError: topic="
                 << static_cast<unsigned>(subscriber_attachment.topic_id) << endl;
            exit(1);
        }
        auto available_subscriber_slots_by_publisher_fd_it =
            available_subscriber_slots_by_topic_id_it->second.find(subscriber_attachment.publisher_fd);
        if (available_subscriber_slots_by_publisher_fd_it == available_subscriber_slots_by_topic_id_it->second.end()) {
            cerr << "SubscriberDisconnectAvailablePublisherMapMissingError: topic="
                 << static_cast<unsigned>(subscriber_attachment.topic_id)
                 << ", publisher_fd=" << subscriber_attachment.publisher_fd << endl;
            exit(1);
        }
        available_subscriber_slots_by_publisher_fd_it->second.emplace(subscriber_attachment.slot_index);

        close(subscriber_attachment.event_fd);
    }

    subscriber_attachments_by_subscriber_fd.erase(subscriber_attachments_it);
    role_by_fd.erase(subscriber_fd);

    for (TopicId topic_id : subscriber_topics_it->second) {
        auto topic_subscribers_it = subscriber_fds_by_topic_id.find(topic_id);
        if (topic_subscribers_it == subscriber_fds_by_topic_id.end()) {
            cerr << "SubscriberDisconnectTopicSubscribersMissingError: topic="
                 << static_cast<unsigned>(topic_id) << endl;
            exit(1);
        }
        auto subscriber_fd_it = find(topic_subscribers_it->second.begin(),
            topic_subscribers_it->second.end(),
            subscriber_fd);
        if (subscriber_fd_it == topic_subscribers_it->second.end()) {
            cerr << "SubscriberDisconnectTopicSubscriberMissingError: topic="
                 << static_cast<unsigned>(topic_id) << ", subscriber_fd=" << subscriber_fd << endl;
            exit(1);
        }
        topic_subscribers_it->second.erase(subscriber_fd_it);
    }

    topic_ids_by_subscriber_fd.erase(subscriber_topics_it);
    close(subscriber_fd);
}

void handle_publisher_disconnect(int pubilsher_fd) {
    auto shm_it = shm_fd_by_publisher_fd.find(pubilsher_fd);
    if (shm_it == shm_fd_by_publisher_fd.end()) {
        cerr << "PublisherDisconnectShmFdMapMissingError: publisher_fd="
             << pubilsher_fd << endl;
        exit(1);
    }

    auto topic_publishers_it = publisher_fds_by_topic_id.end();
    for (auto current_topic_publishers_it = publisher_fds_by_topic_id.begin();
        current_topic_publishers_it != publisher_fds_by_topic_id.end();
        ++current_topic_publishers_it) {
        auto publisher_fd_it = find(current_topic_publishers_it->second.begin(),
            current_topic_publishers_it->second.end(),
            pubilsher_fd);
        if (publisher_fd_it != current_topic_publishers_it->second.end()) {
            topic_publishers_it = current_topic_publishers_it;
            break;
        }
    }
    if (topic_publishers_it == publisher_fds_by_topic_id.end()) {
        cerr << "PublisherDisconnectTopicMapMissingError: publisher_fd="
             << pubilsher_fd << endl;
        exit(1);
    }
    TopicId topic_id = topic_publishers_it->first;

    auto available_subscriber_slots_by_topic_id_it = available_subscriber_slots_by_topic_id_and_publisher_fd.find(topic_id);
    if (available_subscriber_slots_by_topic_id_it == available_subscriber_slots_by_topic_id_and_publisher_fd.end()) {
        cerr << "PublisherDisconnectAvailableTopicMapMissingError: topic="
             << static_cast<unsigned>(topic_id) << endl;
        exit(1);
    }
    auto available_subscriber_slots_by_publisher_fd_it = available_subscriber_slots_by_topic_id_it->second.find(pubilsher_fd);
    if (available_subscriber_slots_by_publisher_fd_it == available_subscriber_slots_by_topic_id_it->second.end()) {
        cerr << "PublisherDisconnectAvailableSlotMapMissingError: topic="
             << static_cast<unsigned>(topic_id) << ", publisher_fd=" << pubilsher_fd << endl;
        exit(1);
    }

    auto topic_subscribers_it = subscriber_fds_by_topic_id.find(topic_id);
    if (topic_subscribers_it != subscriber_fds_by_topic_id.end()) {
        for (int subscriber_fd : topic_subscribers_it->second) {
            auto subscriber_attachments_it = subscriber_attachments_by_subscriber_fd.find(subscriber_fd);
            if (subscriber_attachments_it == subscriber_attachments_by_subscriber_fd.end()) {
                cerr << "PublisherDisconnectSubscriberAttachmentsMissingError: topic="
                     << static_cast<unsigned>(topic_id) << ", subscriber_fd=" << subscriber_fd << endl;
                exit(1);
            }

            auto subscriber_attachment_it = find_if(subscriber_attachments_it->second.begin(),
                subscriber_attachments_it->second.end(),
                [pubilsher_fd](const SubscriberAttachment &subscriber_attachment) {
                    return subscriber_attachment.publisher_fd == pubilsher_fd;
                });
            if (subscriber_attachment_it == subscriber_attachments_it->second.end()) {
                cerr << "PublisherDisconnectSubscriberAttachmentMissingError: topic="
                     << static_cast<unsigned>(topic_id) << ", subscriber_fd=" << subscriber_fd
                     << ", publisher_fd=" << pubilsher_fd << endl;
                exit(1);
            }

            char packet[kPublisherDisconnectedMessageSize];
            const BrokerMessageType message_type = BrokerMessageType::PublisherDisconnected;
            memcpy(packet, &message_type, sizeof(BrokerMessageType));
            memcpy(packet + sizeof(BrokerMessageType), &subscriber_attachment_it->attachment_id, sizeof(AttachmentId));

            ssize_t n = send(subscriber_fd, packet, kPublisherDisconnectedMessageSize, 0);
            if (n < 0) {
                exit(1);
            }

            close(subscriber_attachment_it->event_fd);
            subscriber_attachments_it->second.erase(subscriber_attachment_it);
        }
    }

    auto publisher_fd_it = find(topic_publishers_it->second.begin(),
        topic_publishers_it->second.end(),
        pubilsher_fd);
    topic_publishers_it->second.erase(publisher_fd_it);
    if (topic_publishers_it->second.empty()) {
        publisher_fds_by_topic_id.erase(topic_publishers_it);
    }

    close(shm_it->second);
    shm_fd_by_publisher_fd.erase(shm_it);

    available_subscriber_slots_by_topic_id_it->second.erase(available_subscriber_slots_by_publisher_fd_it);
    if (available_subscriber_slots_by_topic_id_it->second.empty()) {
        available_subscriber_slots_by_topic_id_and_publisher_fd.erase(available_subscriber_slots_by_topic_id_it);
    }

    role_by_fd.erase(pubilsher_fd);

    close(pubilsher_fd);
}

int main() {
    const char *unix_path = "/tmp/broker.sock";
    unlink(unix_path);

    int listen_fd = socket(AF_UNIX, SOCK_SEQPACKET, 0);
    if (listen_fd < 0) {
        cerr << "BrokerListenSocketCreateError: errno=" << errno
             << " (" << strerror(errno) << ')' << endl;
        return 1;
    }
    struct sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, unix_path);

    int bind_ret = bind(listen_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
    if (bind_ret < 0) {
        cerr << "BrokerBindError: path=" << unix_path << ", errno=" << errno
             << " (" << strerror(errno) << ')' << endl;
        return 1;
    }

    int listen_ret = listen(listen_fd, 1024);
    if (listen_ret < 0) {
        cerr << "BrokerListenError: fd=" << listen_fd << ", errno=" << errno
             << " (" << strerror(errno) << ')' << endl;
        return 1;
    }

    std::cout << "receiver listening...\n";

    int epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        cerr << "BrokerEpollCreateError: errno=" << errno
             << " (" << strerror(errno) << ')' << endl;
        return 1;
    }

    add_fd_to_epoll(epoll_fd, listen_fd);

    sigset_t shutdown_signals;
    sigemptyset(&shutdown_signals);
    sigaddset(&shutdown_signals, SIGINT);
    sigaddset(&shutdown_signals, SIGTERM);

    if (sigprocmask(SIG_BLOCK, &shutdown_signals, nullptr) < 0) {
        exit(1);
    }

    int shutdown_fd = signalfd(-1, &shutdown_signals, SFD_NONBLOCK);
    add_fd_to_epoll(epoll_fd, shutdown_fd);

    int maxevents = 1024;
    epoll_event events[maxevents];

    bool shutdown_requested = false;

    while (!shutdown_requested) {
        int event_count = epoll_wait(epoll_fd, events, maxevents, -1);

        if (event_count < 0) {
            cerr << "BrokerEpollWaitError: errno=" << errno
                 << " (" << strerror(errno) << ')' << endl;
            return 1;
        } else if (event_count == 0) {
            cerr << "BrokerUnexpectedEpollTimeoutError" << endl;
            return 1;
        }

        for (int event_index = 0; event_index < event_count; ++event_index) {
            int fd = events[event_index].data.fd;

            if (fd == shutdown_fd) {
                signalfd_siginfo signal_info{};
                read(shutdown_fd, &signal_info, sizeof(signal_info));

                shutdown_requested = true;
                break;
            } else if (fd == listen_fd) {
                while (true) {
                    int conn_fd = accept(listen_fd, nullptr, nullptr);
                    if (conn_fd < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            break;
                        }

                        cerr << "BrokerAcceptError: listen_fd=" << listen_fd
                             << ", errno=" << errno << " (" << strerror(errno) << ')' << endl;
                        exit(1);
                    }

                    client_fds.insert(conn_fd);
                    add_fd_to_epoll(epoll_fd, conn_fd);
                    cout << "accept fd is " << conn_fd << endl;
                }
            } else if (events[event_index].events & EPOLLIN) {
                char packet[kMaxMessageSize];
                char control[CMSG_SPACE(sizeof(int))];

                struct iovec iov{};
                iov.iov_base = packet;
                iov.iov_len = kMaxMessageSize;

                struct msghdr msg{};
                msg.msg_iov = &iov;
                msg.msg_iovlen = 1;
                msg.msg_control = control;

                while (true) {
                    msg.msg_controllen = sizeof(control);

                    const ssize_t received_size = recvmsg(fd, &msg, 0);

                    if (received_size == 0) {
                        cerr << "BrokerPeerClosed: fd=" << fd << endl;
                        auto it = role_by_fd.find(fd);
                        if (it == role_by_fd.end()) {
                            cerr << "BrokerClosedFdRoleMissingError: fd=" << fd << endl;
                            exit(1);
                        }

                        auto role = it->second;
                        if (role == BrokerRole::Subscriber) handle_subsciber_disconect(fd);
                        else if (role == BrokerRole::Publisher) handle_publisher_disconnect(fd);
                        break;
                    } else if (received_size < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            break;
                        }

                        cerr << "BrokerControlRecvError: fd=" << fd
                             << ", errno=" << errno << " (" << strerror(errno) << ')' << endl;
                        auto it = role_by_fd.find(fd);
                        if (it == role_by_fd.end()) {
                            cerr << "BrokerRecvFdRoleMissingError: fd=" << fd << endl;
                            exit(1);
                        }

                        auto client_it = client_fds.find(fd);
                        if (client_it == client_fds.end()) {
                            exit(1);
                        }
                        client_fds.erase(client_it);

                        auto role = it->second;
                        if (role == BrokerRole::Subscriber) handle_subsciber_disconect(fd);
                        else if (role == BrokerRole::Publisher) handle_publisher_disconnect(fd);
                        break;
                    }

                    bool current_fd_closed = false;

                    BrokerMessageType message_type;
                    memcpy(&message_type, packet, sizeof(BrokerMessageType));

                    switch (message_type) {
                        case BrokerMessageType::StatusQuery: {
                            if (received_size != kStatusQuerySize) {
                                exit(1);
                            }

                            string payload = build_status_snapshot_payload();
                            if (payload.size() > kMaxStatusSnapshotPayloadSize) {
                                payload = "complete=0\nreason=status_snapshot_too_large\n";
                            }

                            char sent_type = static_cast<char>(BrokerMessageType::StatusSnapshot);

                            array<iovec, 2> iov{};
                            iov[0].iov_base = &sent_type;
                            iov[0].iov_len = sizeof(sent_type);

                            iov[1].iov_base = payload.data();
                            iov[1].iov_len = payload.size();

                            msghdr msg{};
                            msg.msg_iov = iov.data();
                            msg.msg_iovlen = iov.size();

                            const size_t expected_size = sizeof(sent_type) + payload.size();
                            const ssize_t sent_size = sendmsg(fd, &msg, 0);
                            if (sent_size != static_cast<ssize_t>(expected_size)) {
                                if (sent_size < 0) {
                                    cerr << "BrokerStatusSnapshotSendError: fd=" << fd
                                         << ", errno=" << errno << " (" << strerror(errno) << ')' << endl;
                                } else {
                                    cerr << "BrokerStatusSnapshotSendSizeError: fd=" << fd
                                         << ", expected=" << sizeof(expected_size) << ", actual=" << sent_size << endl;
                                }
                            }

                            epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
                            client_fds.erase(fd);
                            close(fd);

                            current_fd_closed = true;

                            break;
                        }
                        case BrokerMessageType::SubscriberTopicRegistration: {
                            if (received_size != kSubscriberTopicRegistrationMessageSize) {
                                cerr << "SubscriberTopicRegistrationMessageSizeError: expected="
                                     << kSubscriberTopicRegistrationMessageSize
                                     << ", actual=" << received_size << ", fd=" << fd << endl;
                                exit(1);
                            }

                            uint32_t topic_count;
                            memcpy(&topic_count, packet + sizeof(BrokerMessageType), sizeof(topic_count));
                            if (topic_count == 0 || topic_count > kMaxSubscriberTopicCount) {
                                cerr << "SubscriberTopicRegistrationCountError: min=1, max="
                                     << kMaxSubscriberTopicCount << ", actual=" << topic_count
                                     << ", fd=" << fd << endl;
                                exit(1);
                            }

                            vector<TopicId> topic_ids;
                            topic_ids.reserve(topic_count);
                            for (uint32_t topic_index = 0; topic_index < topic_count; ++topic_index) {
                                TopicId topic_id;
                                memcpy(&topic_id,
                                    packet + sizeof(BrokerMessageType) + sizeof(topic_count) + topic_index * sizeof(TopicId),
                                    sizeof(topic_id));
                                topic_ids.emplace_back(topic_id);
                            }
                            topic_ids_by_subscriber_fd.insert({fd, topic_ids});

                            role_by_fd.insert({fd, BrokerRole::Subscriber});

                            vector<SubscriberAttachment> subscriber_attachments;
                            vector<int> shm_fds;

                            for (TopicId topic_id : topic_ids) {
                                auto topic_publishers_it = publisher_fds_by_topic_id.find(topic_id);
                                uint32_t publisher_count = topic_publishers_it == publisher_fds_by_topic_id.end()
                                    ? 0
                                    : static_cast<uint32_t>(topic_publishers_it->second.size());

                                subscriber_fds_by_topic_id[topic_id].emplace_back(fd);

                                if (topic_publishers_it == publisher_fds_by_topic_id.end()) continue;

                                auto available_subscriber_slots_by_topic_id_it = available_subscriber_slots_by_topic_id_and_publisher_fd.find(topic_id);
                                if (available_subscriber_slots_by_topic_id_it == available_subscriber_slots_by_topic_id_and_publisher_fd.end()) {
                                    cerr << "SubscriberTopicAvailableTopicMapMissingError: topic="
                                         << static_cast<unsigned>(topic_id) << endl;
                                    exit(1);
                                }

                                subscriber_attachments.reserve(subscriber_attachments.size() + publisher_count);
                                for (int publisher_fd : topic_publishers_it->second) {
                                    auto shm_it = shm_fd_by_publisher_fd.find(publisher_fd);
                                    if (shm_it == shm_fd_by_publisher_fd.end()) {
                                        cerr << "SubscriberTopicShmFdMapMissingError: topic="
                                             << static_cast<unsigned>(topic_id) << ", publisher_fd=" << publisher_fd << endl;
                                        exit(1);
                                    }
                                    int shm_fd = shm_it->second;

                                    auto available_subscriber_slots_by_publisher_fd_it = available_subscriber_slots_by_topic_id_it->second.find(publisher_fd);
                                    if (available_subscriber_slots_by_publisher_fd_it == available_subscriber_slots_by_topic_id_it->second.end()) {
                                        cerr << "SubscriberTopicAvailablePublisherMapMissingError: topic="
                                             << static_cast<unsigned>(topic_id) << ", publisher_fd=" << publisher_fd << endl;
                                        exit(1);
                                    }
                                    auto &available_subscriber_slots = available_subscriber_slots_by_publisher_fd_it->second;
                                    if (available_subscriber_slots.empty()) {
                                        cerr << "BrokerNoFreeSubscriberSlotError: topic="
                                             << static_cast<unsigned>(topic_id)
                                             << ", publisher_fd=" << publisher_fd << endl;
                                        exit(1);
                                    }
                                    int event_fd = eventfd(0, EFD_NONBLOCK);
                                    uint32_t slot_index = available_subscriber_slots.top();
                                    available_subscriber_slots.pop();

                                    send_subscriber_registered_message(publisher_fd, event_fd, slot_index);

                                    shm_fds.emplace_back(shm_fd);
                                    subscriber_attachments.emplace_back(
                                        SubscriberAttachment{next_attachment_id++, topic_id, publisher_fd, event_fd, slot_index});

                                    if (next_attachment_id == kInvalidAttachmentId) {
                                        exit(1);
                                    }
                                }
                            }

                            ssize_t n = send_subscriber_attachment_batch_message(fd, subscriber_attachments, shm_fds);
                            if (n < 0) {
                                exit(1);
                            }

                            if (!subscriber_attachments.empty())
                                cout << "send fd and subscriber_slot_index" << endl;

                            subscriber_attachments_by_subscriber_fd.insert({fd, std::move(subscriber_attachments)});
                            break;
                        }
                        case BrokerMessageType::PublisherTopicRegistration: {
                            struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);

                            if (cmsg == nullptr) {
                                cerr << "PublisherRegistrationShmFdMissingError: fd="
                                     << fd << endl;
                                exit(1);
                            }
                            if (cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SCM_RIGHTS) {
                                cerr << "PublisherRegistrationCmsgError: level="
                                     << cmsg->cmsg_level << ", type=" << cmsg->cmsg_type
                                     << ", fd=" << fd << endl;
                                exit(1);
                            }

                            int shm_fd = *reinterpret_cast<int *>(CMSG_DATA(cmsg));
                            shm_fd_by_publisher_fd.insert({fd, shm_fd});

                            TopicId topic_id;
                            memcpy(&topic_id, packet + sizeof(BrokerMessageType) / sizeof(char), sizeof(topic_id));

                            auto &available_subscriber_slots_by_publisher_fd =
                                available_subscriber_slots_by_topic_id_and_publisher_fd[topic_id];
                            auto [available_subscriber_slots_by_publisher_fd_it, insert_success] =
                                available_subscriber_slots_by_publisher_fd.insert({fd, {}});
                            if (!insert_success) {
                                cerr << "PublisherSlotIndexHeapAlreadyExistsError: topic="
                                     << static_cast<unsigned>(topic_id) << ", fd=" << fd << endl;
                                exit(1);
                            }
                            AvailableSubscriberSlotMinHeap &available_subscriber_slots =
                                available_subscriber_slots_by_publisher_fd_it->second;
                            init_available_subscriber_slots(available_subscriber_slots);

                            role_by_fd.insert({fd, BrokerRole::Publisher});

                            auto topic_subscribers_it = subscriber_fds_by_topic_id.find(topic_id);
                            size_t subscriber_count = topic_subscribers_it == subscriber_fds_by_topic_id.end()
                                ? 0
                                : topic_subscribers_it->second.size();
                            if (subscriber_count > available_subscriber_slots.size()) {
                                cerr << "BrokerSubscriberSlotCapacityError: topic="
                                     << static_cast<unsigned>(topic_id)
                                     << ", subscriber_count=" << subscriber_count
                                     << ", available=" << available_subscriber_slots.size() << endl;
                                exit(1);
                            }

                            uint32_t registered_subscriber_count = static_cast<uint32_t>(subscriber_count);
                            ssize_t n = send(fd, &registered_subscriber_count, sizeof(registered_subscriber_count), 0);
                            if (n < 0) {
                                cerr << "InitialSubscriberCountSendError: fd=" << fd
                                     << ", errno=" << errno << " (" << strerror(errno) << ')' << endl;
                                return 1;
                            }

                            if (topic_subscribers_it != subscriber_fds_by_topic_id.end()) {
                                for (int subscriber_fd : topic_subscribers_it->second) {
                                    int event_fd = eventfd(0, EFD_NONBLOCK);
                                    uint32_t slot_index = available_subscriber_slots.top();
                                    available_subscriber_slots.pop();

                                    send_subscriber_registered_message(fd, event_fd, slot_index);

                                    SubscriberAttachment attachment{next_attachment_id++, topic_id, fd, event_fd, slot_index};
                                    if (next_attachment_id == kInvalidAttachmentId) {
                                        exit(1);
                                    }

                                    ssize_t subscriber_batch_send_result = send_subscriber_attachment_batch_message(subscriber_fd, {attachment}, {shm_fd});
                                    if (subscriber_batch_send_result < 0) {
                                        exit(1);
                                    }

                                    auto subscriber_attachments_it = subscriber_attachments_by_subscriber_fd.find(subscriber_fd);
                                    if (subscriber_attachments_it == subscriber_attachments_by_subscriber_fd.end()) {
                                        cerr << "PublisherRegistrationSubscriberAttachmentsMissingError: topic="
                                             << static_cast<unsigned>(topic_id)
                                             << ", subscriber_fd=" << subscriber_fd << endl;
                                        exit(1);
                                    }
                                    subscriber_attachments_it->second.emplace_back(std::move(attachment));
                                }
                            }
                            publisher_fds_by_topic_id[topic_id].emplace_back(fd);
                        }
                        default: {
                            break;
                        }
                    }

                    if (current_fd_closed) break;
                }
            }
        }
    }

    unlink(unix_path);

    close(listen_fd);
    close(epoll_fd);
    close(shutdown_fd);

    for (int fd : client_fds) {
        close(fd);
    }

    for (auto [fd, role] : role_by_fd) {
        if (role == BrokerRole::Subscriber) {
            auto it = subscriber_attachments_by_subscriber_fd.find(fd);
            if (it != subscriber_attachments_by_subscriber_fd.end()) {
                for (auto &attachment : it->second) {
                    close(attachment.event_fd);
                }
            }
        } else {
            auto it = shm_fd_by_publisher_fd.find(fd);
            if (it != shm_fd_by_publisher_fd.end()) {
                close(it->second);
            }
        }
    }

    return 0;
}
