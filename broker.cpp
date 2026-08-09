#include "include/broker_protocol.hpp"
#include "include/shared_memory_layout.hpp"
#include "include/shared_memory_layout_helpers.hpp"
#include "include/fd_helpers.hpp"
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <cstddef>
#include <iostream>
#include <queue>
#include <thread>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <fcntl.h>
#include <stdint.h>
#include <unordered_map>
#include <vector>
using namespace std;

void send_subscriber_registered_message(int conn_fd, int fd_to_send, uint32_t send_subscriber_slot_index) {
    char packet[kSubscriberEventFdAndSlotMessageSize];
    packet[0] = static_cast<char>(BrokerMessageType::SubscriberEventFdAndSlot);
    memcpy(packet + 1, &send_subscriber_slot_index, sizeof(send_subscriber_slot_index));

    send_packet_with_fd(conn_fd, packet, sizeof(packet), fd_to_send);
}

void send_publisher_topic_shm_fd_message(int conn_fd, TopicId topic_id, int shm_fd) {
    char packet[kPublisherTopicShmFdMessageSize];
    packet[0] = static_cast<char>(BrokerMessageType::PublisherTopicShmFd);
    memcpy(packet + sizeof(BrokerMessageType), &topic_id, sizeof(topic_id));
    send_packet_with_fd(conn_fd, packet, sizeof(packet), shm_fd);
}

void send_subscriber_publisher_count_message(int conn_fd, uint32_t publisher_count) {
    char packet[kSubscriberPublisherCountMessageSize];
    packet[0] = static_cast<char>(BrokerMessageType::SubscriberPublisherCount);
    memcpy(packet + 1, &publisher_count, sizeof(publisher_count));

    send(conn_fd, packet, sizeof(packet), 0);
}

using AvailableSubscriberSlotMinHeap = std::priority_queue<size_t, std::vector<size_t>, std::greater<size_t>>;
std::unordered_map<TopicId, std::unordered_map<int, AvailableSubscriberSlotMinHeap>> available_subscriber_slots_by_topic_id_and_publisher_fd;

void init_available_subscriber_slots(AvailableSubscriberSlotMinHeap &available_subscriber_slots) {
    for (size_t slot_index = 0; slot_index < kMaxSubscribers; ++slot_index) {
        available_subscriber_slots.emplace(slot_index);
    }
}

struct SubscriberAttachment {
    TopicId topic_id;
    int publisher_fd;
    int event_fd;
    uint32_t slot_index;
};

std::unordered_map<int, std::vector<SubscriberAttachment>> subscriber_attachments_by_conn_fd{};
std::unordered_map<int, BrokerRole> role_by_fd;

std::unordered_map<int, int> shm_fd_by_publisher_fd;
std::unordered_map<int, std::vector<TopicId>> topic_ids_by_subscriber_fd;
std::unordered_map<TopicId, std::vector<int>> publisher_fds_by_topic_id;

std::unordered_map<TopicId, std::vector<int>> subscriber_fds_by_topic_id;

void handle_subsciber_disconect(int subscriber_fd) {
    auto subscriber_attachments_it = subscriber_attachments_by_conn_fd.find(subscriber_fd);
    if (subscriber_attachments_it == subscriber_attachments_by_conn_fd.end()) {
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
        packet[0] = static_cast<char>(BrokerMessageType::SubscriberDisconnected);
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

    subscriber_attachments_by_conn_fd.erase(subscriber_attachments_it);
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
            auto subscriber_attachments_it = subscriber_attachments_by_conn_fd.find(subscriber_fd);
            if (subscriber_attachments_it == subscriber_attachments_by_conn_fd.end()) {
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

    int maxevents = 1024;
    epoll_event events[maxevents];

    while (true) {
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
            if (fd == listen_fd) {
                int conn_fd = accept(listen_fd, nullptr, nullptr);
                if (conn_fd < 0) {
                    cerr << "BrokerAcceptError: listen_fd=" << listen_fd
                         << ", errno=" << errno << " (" << strerror(errno) << ')' << endl;
                    exit(1);
                }

                add_fd_to_epoll(epoll_fd, conn_fd);
                cout << "accept fd is " << conn_fd << endl;
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

                        auto role = it->second;
                        if (role == BrokerRole::Subscriber) handle_subsciber_disconect(fd);
                        else if (role == BrokerRole::Publisher) handle_publisher_disconnect(fd);
                        break;
                    }

                    auto message_type = static_cast<BrokerMessageType>(packet[0]);

                    if (message_type == BrokerMessageType::SubscriberTopicRegistration) {
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
                        for (TopicId topic_id : topic_ids) {
                            auto topic_publishers_it = publisher_fds_by_topic_id.find(topic_id);
                            uint32_t publisher_count = topic_publishers_it == publisher_fds_by_topic_id.end()
                                ? 0
                                : static_cast<uint32_t>(topic_publishers_it->second.size());
                            send_subscriber_publisher_count_message(fd, publisher_count);
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

                                send_publisher_topic_shm_fd_message(fd, topic_id, shm_fd);
                                send_subscriber_registered_message(fd, event_fd, slot_index);
                                send_subscriber_registered_message(publisher_fd, event_fd, slot_index);

                                subscriber_attachments.emplace_back(
                                    SubscriberAttachment{topic_id, publisher_fd, event_fd, slot_index});
                            }
                        }
                        subscriber_attachments_by_conn_fd.insert({fd, subscriber_attachments});

                        if (!subscriber_attachments.empty())
                            cout << "send fd and subscriber_slot_index" << endl;
                    } else if (message_type == BrokerMessageType::PublisherTopicRegistration) {
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
                                send_subscriber_publisher_count_message(subscriber_fd, 1);
                                send_publisher_topic_shm_fd_message(subscriber_fd, topic_id, shm_fd);
                                send_subscriber_registered_message(subscriber_fd,
                                    event_fd,
                                    slot_index);

                                auto subscriber_attachments_it = subscriber_attachments_by_conn_fd.find(subscriber_fd);
                                if (subscriber_attachments_it == subscriber_attachments_by_conn_fd.end()) {
                                    cerr << "PublisherRegistrationSubscriberAttachmentsMissingError: topic="
                                         << static_cast<unsigned>(topic_id)
                                         << ", subscriber_fd=" << subscriber_fd << endl;
                                    exit(1);
                                }
                                subscriber_attachments_it->second.emplace_back(
                                    SubscriberAttachment{topic_id, fd, event_fd, slot_index});
                            }
                        }
                        publisher_fds_by_topic_id[topic_id].emplace_back(fd);
                    }
                }
            }
        }
    }

    return 0;
}
