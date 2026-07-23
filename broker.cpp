#include "include/broker_protocol.hpp"
#include "include/shared_memory_layout.hpp"
#include "include/shared_memory_layout_helpers.hpp"
#include "include/fd_helpers.hpp"
#include <algorithm>
#include <atomic>
#include <cstring>
#include <cstddef>
#include <iostream>
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

void send_fd(int conn_fd, int send_fd, uint32_t send_subscriber_slot_index) {
    struct msghdr msg{};

    struct iovec iov;
    iov.iov_base = &send_subscriber_slot_index;
    iov.iov_len = sizeof(send_subscriber_slot_index);

    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;

    char control[CMSG_SPACE(sizeof(int))];
    memset(control, 0, sizeof(control));

    msg.msg_control = control;
    msg.msg_controllen = sizeof(control);

    struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(sizeof(int));

    *reinterpret_cast<int *>(CMSG_DATA(cmsg)) = send_fd;
    sendmsg(conn_fd, &msg, 0);
}

void chunk_usage_tracker_init(ChunkUsageTracker &tracker) {
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
    pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
    pthread_mutex_init(&tracker.mutex, &attr);
    pthread_mutexattr_destroy(&attr);

    memset(tracker.is_in_use, 0, sizeof(tracker.is_in_use));
}

void shm_init(SharedData &shm) {
    for (int i = 0; i < kMaxSubscribers; ++i) {
        shm.descriptor_read_indices[i].store(kInvalidIndex, std::memory_order_relaxed);
    }
    shm.descriptor_write_index.store(0, std::memory_order_relaxed);

    shm.head.offset[0].store(0, std::memory_order_relaxed);
    for (int i = 1; i < kClassCount; ++i) {
        shm.head.offset[i].store(shm.head.offset[i - 1].load(std::memory_order_relaxed) + kClassSizeBytes[i - 1] * kChunkCountBySizeClass[i - 1], std::memory_order_relaxed);
    }

    for (int i = 0; i < kClassCount - 1; ++i) {
        shm.tail.offset[i].store(shm.head.offset[i + 1].load(std::memory_order_relaxed) - kClassSizeBytes[i], std::memory_order_relaxed);
    }
    shm.tail.offset[kClassCount - 1].store(sizeof(shm.data) - kClassSizeBytes[kClassCount - 1], std::memory_order_relaxed);

    for (int i = 0; i < kTotalChunkCount; ++i) {
        shm.chunk_reference_counts[i].store(0, std::memory_order_relaxed);
    }

    auto init = [&](uint32_t chunk_size_bytes, const uint32_t chunk_count, uint32_t offset) {
        for (uint32_t i = 0; i < chunk_count - 1; ++i) {
            uint32_t next_offset = offset + chunk_size_bytes;
            memcpy(shm.data + offset, &next_offset, sizeof(uint32_t));
            offset = next_offset;
        }
        memcpy(shm.data + offset, &kInvalidOffset, sizeof(uint32_t));
    };
    for (int i = 0; i < kClassCount; ++i) {
        init(kClassSizeBytes[i], kChunkCountBySizeClass[i], shm.head.offset[i]);
    }
}

size_t subscriber_slot_index = 0;
std::unordered_map<int, size_t> subscriber_slot_index_by_event_fd{};
std::unordered_map<int, size_t> subscriber_slot_index_by_conn_fd{};

vector<int> event_fds;
vector<int> publisher_fds;

void handle_subsciber_disconect(int subscriber_conn_fd) {
    auto it = subscriber_slot_index_by_conn_fd.find(subscriber_conn_fd);
    if (it == subscriber_slot_index_by_conn_fd.end()) {
        cout << "not find subscriber_slot_index" << endl;
        exit(1);
    }
    uint32_t subscriber_slot_index = it->second;
    for (int publisher_fd : publisher_fds) {
        char packet[kSubscriberDisconnectedMessageSize];
        packet[0] = static_cast<char>(BrokerMessageType::SubscriberDisconnected);
        memcpy(packet + 1, &subscriber_slot_index, sizeof(subscriber_slot_index));

        ssize_t n = send(publisher_fd, packet, kSubscriberDisconnectedMessageSize, 0);
        if (n < 0) exit(1);
    }

    close(subscriber_conn_fd);
}

int main() {
    const char *shm_name = "/shm";
    shm_unlink(shm_name);

    int shm_fd = shm_open(shm_name, O_CREAT | O_RDWR, 0666);
    ftruncate(shm_fd, sizeof(SharedData));
    SharedData *p = reinterpret_cast<SharedData *>(mmap(nullptr, sizeof(SharedData), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0));

    shm_init(*p);
#ifdef ENABLE_DEBUG_CHECKS
    chunk_usage_tracker_init(p->chunk_usage_tracker);
#endif

    const char *unix_path = "/tmp/broker.sock";
    unlink(unix_path);

    int listen_fd = socket(AF_UNIX, SOCK_SEQPACKET, 0);
    struct sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, unix_path);

    int bind_ret = bind(listen_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
    if (bind_ret < 0) return 1;

    int listen_ret = listen(listen_fd, 1024);
    if (listen_ret < 0) return 1;

    std::cout << "receiver listening...\n";

    int epoll_fd = epoll_create1(0);

    add_fd_to_epoll(epoll_fd, listen_fd);

    int maxevents = 1024;
    epoll_event events[maxevents];

    std::unordered_map<int, BrokerRole> role_by_fd;

    while (true) {
        int event_count = epoll_wait(epoll_fd, events, maxevents, -1);

        if (event_count < 0) {
            return 1;
        } else if (event_count == 0) {
            std::cout << "epoll_wait time out" << std::endl;
            return 1;
        }

        for (int event_index = 0; event_index < event_count; ++event_index) {
            int fd = events[event_index].data.fd;
            if (fd == listen_fd) {
                int conn_fd = accept(listen_fd, nullptr, nullptr);
                if (conn_fd < 0) exit(1);

                add_fd_to_epoll(epoll_fd, conn_fd);
                cout << "accept fd is " << conn_fd << endl;
            } else if (events[event_index].events & EPOLLIN) {
                BrokerMessageType message_type;
                ssize_t n = recv(fd, &message_type, sizeof(BrokerMessageType), 0);
                if (n < 0) {
                    if (!(errno == EAGAIN || errno == EWOULDBLOCK)) {
                        std::cout << "fd = " << fd << ", recv error" << std::endl;
                        auto it = role_by_fd.find(fd);
                        if (it == role_by_fd.end()) {
                            cout << "role_by_fd find error" << endl;
                            exit(1);
                        }
                        if (it->second == BrokerRole::Subscriber) handle_subsciber_disconect(fd);
                    }
                } else if (n == 0) {
                    cout << "fd = " << fd << ", close writing" << endl;
                    auto it = role_by_fd.find(fd);
                    if (it == role_by_fd.end()) {
                        cout << "role_by_fd find error" << endl;
                        exit(1);
                    }
                    if (it->second == BrokerRole::Subscriber) handle_subsciber_disconect(fd);
                } else {
                    if (message_type == BrokerMessageType::SubscriberRegistration) {
                        role_by_fd.insert({fd, BrokerRole::Subscriber});

                        int event_fd = eventfd(0, EFD_NONBLOCK);
                        event_fds.emplace_back(event_fd);

                        send_fd(fd, event_fd, subscriber_slot_index);
                        subscriber_slot_index_by_event_fd.insert({event_fd, subscriber_slot_index});
                        subscriber_slot_index_by_conn_fd.insert({fd, subscriber_slot_index});
                        ++subscriber_slot_index;

                        cout << "send fd and subscriber_slot_index" << endl;
                    } else if (message_type == BrokerMessageType::PublisherRegistration) {
                        role_by_fd.insert({fd, BrokerRole::Publisher});

                        cout << "publisher need subscriber data" << endl;
                        publisher_fds.emplace_back(fd);
                        uint32_t subscriber_counts = subscriber_slot_index;
                        ssize_t n = send(fd, &subscriber_counts, sizeof(subscriber_counts), 0);
                        if (n < 0) {
                            cout << "send error" << endl;
                            return 1;
                        }

                        for (int event_fd : event_fds) {
                            auto it = subscriber_slot_index_by_event_fd.find(event_fd);
                            if (it == subscriber_slot_index_by_event_fd.end()) {
                                cout << "not find subscriber_slot_index" << endl;
                                exit(1);
                            }
                            send_fd(fd, event_fd, it->second);
                            cout << "send event_fd " << event_fd << ", subscriber_slot_index " << subscriber_slot_index << " to publisher" << endl;
                        }
                    }
                }
            }
        }
    }

    return 0;
}