#include "include/broker_protocol.hpp"
#include "include/shared_memory_layout.hpp"
#include "include/shared_memory_layout_helpers.hpp"
#include "include/fd_helpers.hpp"
#include <atomic>
#include <cerrno>
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
using namespace std;

struct PublisherAttachment {
    TopicId topic_id = TopicId::Invalid;
    int shm_fd = -1;
    SharedData *shared_data = nullptr;
    int event_fd = -1;
    uint32_t slot_index = kInvalidIndex;
    uint32_t subscriber_read_index = kInvalidIndex;
    int expected_seq = -1;
};

enum class SubscriberAttachmentState {
    WaitingForPublisherCount,
    WaitingForPublisherTopicShmFd,
    WaitingForSubscriberEventFdAndSlot,
};

ssize_t receive_broker_packet(int broker_fd, char packet[], size_t packet_size, int &received_fd) {
    struct iovec iov{};
    iov.iov_base = packet;
    iov.iov_len = packet_size;

    struct msghdr msg{};
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;

    char control[CMSG_SPACE(sizeof(int))];
    memset(control, 0, sizeof(control));

    msg.msg_control = control;
    msg.msg_controllen = sizeof(control);

    received_fd = -1;
    ssize_t received_size = recvmsg(broker_fd, &msg, 0);
    if (received_size <= 0) return received_size;

    struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    if (cmsg == nullptr) return received_size;
    if (cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SCM_RIGHTS) {
        errno = EPROTO;
        return -1;
    }

    received_fd = *reinterpret_cast<int *>(CMSG_DATA(cmsg));
    return received_size;
}

void read_data(PublisherAttachment &publisher_attachment, MessageDescriptor desc_ring[], const size_t descriptor_count) {
    SharedData *p = publisher_attachment.shared_data;
    for (size_t descriptor_index = 0; descriptor_index < descriptor_count; ++descriptor_index) {
        uint32_t offset = desc_ring[descriptor_index].offset;
        uint32_t message_size_bytes = desc_ring[descriptor_index].len;

        int size_class = find_size_class(message_size_bytes);
        if (size_class < 0) {
            cerr << "SubscriberMessageSizeClassError: seq=" << publisher_attachment.expected_seq
                 << ", descriptor=" << descriptor_index
                 << ", size=" << message_size_bytes << endl;
            exit(1);
        }

        int magic, seq;
        memcpy(&magic, p->data + offset, sizeof(int));
        memcpy(&seq, p->data + offset + sizeof(int), sizeof(int));
        cout << "read " << message_size_bytes << " byte, offset is " << offset << ", seq is " << seq << endl;

        if (publisher_attachment.expected_seq == -1) publisher_attachment.expected_seq = seq;

        bool is_error = false;
        if (magic != kMagic) {
            cerr << "SubscriberChunkMagicError: expected=" << kMagic
                 << ", actual=" << magic << ", descriptor=" << descriptor_index
                 << ", offset=" << offset << endl;
            is_error = true;
        }
        if (seq != publisher_attachment.expected_seq) {
            cerr << "SubscriberSequenceError: expected=" << publisher_attachment.expected_seq
                 << ", actual=" << seq << ", descriptor=" << descriptor_index
                 << ", offset=" << offset << endl;
            is_error = true;
        }
        if (is_error) exit(1);
        if (message_size_bytes > 2 * sizeof(int)) {
            write(1, p->data + offset + 2 * sizeof(int), message_size_bytes - 2 * sizeof(int));
        }
        cout << endl;

        ++publisher_attachment.expected_seq;

#ifdef ENABLE_DEBUG_CHECKS
        if (offset < kFirstOffset[size_class] || offset > kLastOffset[size_class]) {
            cerr << "SubscriberChunkOffsetError: class=" << size_class
                 << ", offset=" << offset << endl;
            pthread_mutex_unlock(&p->chunk_usage_tracker.mutex);
            exit(1);
        }
#endif

        uint32_t local_chunk_index = (offset - kFirstOffset[size_class]) / kClassSizeBytes[size_class];
        uint32_t chunk_index = kChunkIndexBaseBySizeClass[size_class] + local_chunk_index;
        uint32_t subscriber_reference_bit = 1 << publisher_attachment.slot_index;
        uint32_t previous_reference_count = p->chunk_reference_counts[chunk_index].fetch_and(~subscriber_reference_bit, std::memory_order_acq_rel);

        if (++publisher_attachment.subscriber_read_index >= kDescriptorSlotCount)
            publisher_attachment.subscriber_read_index %= kDescriptorSlotCount;
        p->descriptor_read_indices[publisher_attachment.slot_index].store(publisher_attachment.subscriber_read_index, std::memory_order_release);
        cout << "subscriber_read_index is " << publisher_attachment.subscriber_read_index << endl;

        if (previous_reference_count == subscriber_reference_bit) {
#ifdef ENABLE_DEBUG_CHECKS
            {
                pthread_mutex_lock(&p->chunk_usage_tracker.mutex);

                size_t chunk_index = (offset - kFirstOffset[size_class]) / kClassSizeBytes[size_class];
                cout << "the " << chunk_index << " chunk free" << endl;
                if (p->chunk_usage_tracker.is_in_use[size_class][chunk_index] == false) {
                    cerr << "SubscriberChunkDoubleFreeError: class=" << size_class
                         << ", index=" << chunk_index << ", offset=" << offset << endl;
                    pthread_mutex_unlock(&p->chunk_usage_tracker.mutex);
                    exit(1);
                }
                p->chunk_usage_tracker.is_in_use[size_class][chunk_index] = false;

                pthread_mutex_unlock(&p->chunk_usage_tracker.mutex);
            }
#endif

            uint32_t last_tail_off = p->tail.offset[size_class].load(memory_order_relaxed);
            memcpy(p->data + last_tail_off, &offset, sizeof(uint32_t));
            p->tail.offset[size_class].store(offset, std::memory_order_release);
        }
    }
}

void initialize_subscriber_read_index(PublisherAttachment &publisher_attachment) {
    if (publisher_attachment.subscriber_read_index == kInvalidIndex)
        publisher_attachment.subscriber_read_index = publisher_attachment.shared_data
                                                         ->descriptor_read_indices[publisher_attachment.slot_index]
                                                         .load(memory_order_relaxed);
}

void consume_contiguous_messages(PublisherAttachment &publisher_attachment) {
    SharedData *p = publisher_attachment.shared_data;
    uint32_t wr = p->descriptor_write_index.load(std::memory_order_acquire);
    while (wr > publisher_attachment.subscriber_read_index) {
        const size_t descriptor_count = static_cast<size_t>(wr - publisher_attachment.subscriber_read_index);
        MessageDescriptor desc_ring[descriptor_count];
        copy(p->desc_ring + publisher_attachment.subscriber_read_index, p->desc_ring + wr, desc_ring);

        read_data(publisher_attachment, desc_ring, descriptor_count);
        wr = p->descriptor_write_index.load(std::memory_order_acquire);
    }
}

int main(int argc, char *argv[]) {
    if (argc < 2 || argc > static_cast<int>(kMaxSubscriberTopicCount) + 1) {
        cerr << "SubscriberTopicArgumentError: usage=" << argv[0]
             << " <topic_id> [topic_id...]"
             << ", max_topics=" << kMaxSubscriberTopicCount << endl;
        return 1;
    }
    uint32_t topic_count = static_cast<uint32_t>(argc - 1);

    const string path = "/tmp/broker.sock";

    int broker_fd = socket(AF_UNIX, SOCK_SEQPACKET, 0);

    struct sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, path.c_str());

    int ret = connect(broker_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
    int fail_num = 0;
    while (ret < 0 && fail_num < 3) {
        sleep(1);
        ++fail_num;
        ret = connect(broker_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
    }
    if (ret < 0) {
        cerr << "SubscriberBrokerConnectError: attempts=" << fail_num
             << ", errno=" << errno << " (" << strerror(errno) << ')' << endl;
        return -1;
    }

    PublisherAttachment pending_publisher_attachment;
    unordered_map<int, PublisherAttachment> publisher_attachments_by_event_fd;
    SubscriberAttachmentState subscriber_attachment_state = SubscriberAttachmentState::WaitingForPublisherCount;
    uint32_t remaining_publisher_count = 0;

    char packet[kSubscriberTopicRegistrationMessageSize]{};
    packet[0] = static_cast<char>(BrokerMessageType::SubscriberTopicRegistration);
    memcpy(packet + sizeof(BrokerMessageType), &topic_count, sizeof(topic_count));
    for (uint32_t topic_index = 0; topic_index < topic_count; ++topic_index) {
        TopicId topic_id = static_cast<TopicId>(stoul(argv[topic_index + 1]));
        memcpy(packet + sizeof(BrokerMessageType) + sizeof(topic_count) + topic_index * sizeof(TopicId),
            &topic_id, sizeof(topic_id));
    }

    send(broker_fd, packet, sizeof(packet), 0);

    int epoll_fd = epoll_create1(0);
    add_fd_to_epoll(epoll_fd, broker_fd);

    int maxevents = 1024;
    epoll_event events[maxevents];

    while (true) {
        int event_count = epoll_wait(epoll_fd, events, maxevents, -1);
        cout << "event_cout = " << event_count << endl;

        for (int i = 0; i < event_count; ++i) {
            int fd = events[i].data.fd;
            if (fd != broker_fd) {
                auto publisher_attachment_it = publisher_attachments_by_event_fd.find(fd);
                if (publisher_attachment_it == publisher_attachments_by_event_fd.end()) {
                    cerr << "SubscriberEventFdAttachmentMissingError: event_fd=" << fd << endl;
                    return 1;
                }
                PublisherAttachment &publisher_attachment = publisher_attachment_it->second;

                uint64_t val;
                read(fd, &val, sizeof(val));

                initialize_subscriber_read_index(publisher_attachment);

                uint32_t wr = publisher_attachment.shared_data->descriptor_write_index.load(std::memory_order_acquire);
                cout << "wr is " << wr << ", rd is " << publisher_attachment.subscriber_read_index << endl;
                if (publisher_attachment.subscriber_read_index > wr) {
                    const size_t descriptor_count = static_cast<size_t>(kDescriptorSlotCount - publisher_attachment.subscriber_read_index);
                    MessageDescriptor desc_ring[descriptor_count];
                    copy(publisher_attachment.shared_data->desc_ring + publisher_attachment.subscriber_read_index,
                        publisher_attachment.shared_data->desc_ring + kDescriptorSlotCount,
                        desc_ring);

                    read_data(publisher_attachment, desc_ring, descriptor_count);
                    wr = publisher_attachment.shared_data->descriptor_write_index.load(std::memory_order_acquire);
                }

                consume_contiguous_messages(publisher_attachment);
                cout << "read over" << endl;
            } else if (fd == broker_fd) {
                while (true) {
                    char broker_packet[kMaxMessageSize];
                    int received_fd;
                    ssize_t received_size = receive_broker_packet(broker_fd, broker_packet, sizeof(broker_packet), received_fd);
                    if (received_size == 0) {
                        cerr << "SubscriberBrokerClosed: broker_fd=" << broker_fd << endl;
                        return 1;
                    }
                    if (received_size < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;

                        cerr << "SubscriberBrokerReceiveError: broker_fd=" << broker_fd
                             << ", errno=" << errno << " (" << strerror(errno) << ')' << endl;
                        return 1;
                    }

                    BrokerMessageType message_type = static_cast<BrokerMessageType>(broker_packet[0]);
                    if (subscriber_attachment_state == SubscriberAttachmentState::WaitingForPublisherCount) {
                        if (message_type != BrokerMessageType::SubscriberPublisherCount) {
                            cerr << "SubscriberPublisherCountTypeError: expected="
                                 << static_cast<unsigned>(BrokerMessageType::SubscriberPublisherCount)
                                 << ", actual=" << static_cast<unsigned>(static_cast<unsigned char>(broker_packet[0]))
                                 << ", broker_fd=" << broker_fd << endl;
                            return 1;
                        }
                        if (received_size != kSubscriberPublisherCountMessageSize) {
                            cerr << "SubscriberPublisherCountMessageSizeError: expected="
                                 << kSubscriberPublisherCountMessageSize
                                 << ", actual=" << received_size
                                 << ", broker_fd=" << broker_fd << endl;
                            return 1;
                        }

                        uint32_t publisher_count;
                        memcpy(&publisher_count, broker_packet + sizeof(BrokerMessageType), sizeof(publisher_count));
                        remaining_publisher_count = publisher_count;
                        if (remaining_publisher_count > 0)
                            subscriber_attachment_state = SubscriberAttachmentState::WaitingForPublisherTopicShmFd;
                        continue;
                    }

                    if (subscriber_attachment_state == SubscriberAttachmentState::WaitingForPublisherTopicShmFd) {
                        if (message_type != BrokerMessageType::PublisherTopicShmFd) {
                            cerr << "SubscriberPublisherTopicShmFdTypeError: expected="
                                 << static_cast<unsigned>(BrokerMessageType::PublisherTopicShmFd)
                                 << ", actual=" << static_cast<unsigned>(static_cast<unsigned char>(broker_packet[0]))
                                 << ", broker_fd=" << broker_fd << endl;
                            return 1;
                        }
                        if (received_size != kPublisherTopicShmFdMessageSize) {
                            cerr << "SubscriberPublisherTopicShmFdMessageSizeError: expected="
                                 << kPublisherTopicShmFdMessageSize << ", actual=" << received_size
                                 << ", broker_fd=" << broker_fd << endl;
                            return 1;
                        }
                        if (received_fd < 0) {
                            cerr << "SubscriberPublisherTopicShmFdMissingError: broker_fd=" << broker_fd << endl;
                            return 1;
                        }

                        pending_publisher_attachment = PublisherAttachment{};
                        memcpy(&pending_publisher_attachment.topic_id,
                            broker_packet + sizeof(BrokerMessageType),
                            sizeof(pending_publisher_attachment.topic_id));
                        pending_publisher_attachment.shm_fd = received_fd;
                        pending_publisher_attachment.shared_data = reinterpret_cast<SharedData *>(mmap(nullptr,
                            sizeof(SharedData),
                            PROT_READ | PROT_WRITE,
                            MAP_SHARED,
                            pending_publisher_attachment.shm_fd,
                            0));
                        subscriber_attachment_state = SubscriberAttachmentState::WaitingForSubscriberEventFdAndSlot;
                        continue;
                    }

                    if (subscriber_attachment_state == SubscriberAttachmentState::WaitingForSubscriberEventFdAndSlot) {
                        if (message_type != BrokerMessageType::SubscriberEventFdAndSlot) {
                            cerr << "SubscriberRegistrationResponseTypeError: expected="
                                 << static_cast<unsigned>(BrokerMessageType::SubscriberEventFdAndSlot)
                                 << ", actual=" << static_cast<unsigned>(static_cast<unsigned char>(broker_packet[0]))
                                 << ", broker_fd=" << broker_fd << endl;
                            return 1;
                        }
                        if (received_size != kSubscriberEventFdAndSlotMessageSize) {
                            cerr << "SubscriberRegisteredMessageSizeError: expected="
                                 << kSubscriberEventFdAndSlotMessageSize
                                 << ", actual=" << received_size
                                 << ", broker_fd=" << broker_fd << endl;
                            return 1;
                        }
                        if (received_fd < 0) {
                            cerr << "SubscriberRegisteredEventFdMissingError: broker_fd=" << broker_fd << endl;
                            return 1;
                        }

                        pending_publisher_attachment.event_fd = received_fd;
                        memcpy(&pending_publisher_attachment.slot_index,
                            broker_packet + sizeof(BrokerMessageType),
                            sizeof(pending_publisher_attachment.slot_index));
                        auto [publisher_attachment_it, insert_success] = publisher_attachments_by_event_fd.emplace(
                            pending_publisher_attachment.event_fd,
                            pending_publisher_attachment);
                        if (!insert_success) {
                            cerr << "SubscriberEventFdAttachmentAlreadyExistsError: event_fd="
                                 << pending_publisher_attachment.event_fd << endl;
                            return 1;
                        }
                        PublisherAttachment &publisher_attachment = publisher_attachment_it->second;
                        cout << "subscriber_slot_index is " << publisher_attachment.slot_index << endl;

                        add_fd_to_epoll(epoll_fd, publisher_attachment.event_fd);
                        --remaining_publisher_count;
                        subscriber_attachment_state = remaining_publisher_count > 0
                            ? SubscriberAttachmentState::WaitingForPublisherTopicShmFd
                            : SubscriberAttachmentState::WaitingForPublisherCount;

                        uint64_t val;
                        ssize_t n = read(publisher_attachment.event_fd, &val, sizeof(val));
                        if (n > 0) {
                            initialize_subscriber_read_index(publisher_attachment);
                            consume_contiguous_messages(publisher_attachment);
                            cout << "read over" << endl;
                        }
                        continue;
                    }

                    cerr << "SubscriberAttachmentStateError: broker_fd=" << broker_fd << endl;
                    return 1;
                }
            }
        }
        this_thread::sleep_for(chrono::milliseconds(1));
    }

#ifdef ENABLE_DEBUG_CHECKS
    for (auto &[event_fd, publisher_attachment] : publisher_attachments_by_event_fd) {
        SharedData *p = publisher_attachment.shared_data;
        pthread_mutex_lock(&p->chunk_usage_tracker.mutex);

        for (int i = 0; i < kClassCount; ++i) {
            for (int j = 0; j < kMaxChunkCountPerSizeClass; ++j) {
                if (p->chunk_usage_tracker.is_in_use[i][j] == true) {
                    cerr << "SubscriberChunkStillInUseError: event_fd=" << event_fd
                         << ", class=" << i
                         << ", index=" << j << endl;
                }
            }
        }

        pthread_mutex_unlock(&p->chunk_usage_tracker.mutex);
        pthread_mutex_destroy(&p->chunk_usage_tracker.mutex);
    }
#endif

    for (auto &[event_fd, publisher_attachment] : publisher_attachments_by_event_fd) {
        munmap(publisher_attachment.shared_data, sizeof(SharedData));
        close(publisher_attachment.shm_fd);
        close(event_fd);
    }

    return 0;
}
