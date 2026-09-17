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

int epoll_fd = -1;
int broker_fd = -1;

unordered_map<int, PublisherAttachment> publisher_attachment_by_event_fd;
unordered_map<AttachmentId, int> event_fd_by_attachment_id;

void process_broker_control_messages() {
    char packet[kMaxMessageSize];
    char control[CMSG_SPACE(kMaxAttachmentsPerBatch * 2 * sizeof(int))];

    struct iovec iov{};
    iov.iov_base = packet;
    iov.iov_len = kMaxMessageSize;

    struct msghdr msg{};
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control;

    while (true) {
        msg.msg_controllen = sizeof(control);
        msg.msg_flags = 0;

        const ssize_t received_size = recvmsg(broker_fd, &msg, 0);

        if (received_size == 0) {
            cerr << "PublisherBrokerClosed: broker_fd=" << broker_fd << endl;
            break;
        } else if (received_size < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            } else {
                cerr << "PublisherControlRecvError: broker_fd=" << broker_fd
                     << ", errno=" << errno << " (" << strerror(errno) << ')' << endl;
                exit(1);
            }
        } else if (msg.msg_flags & MSG_CTRUNC) {
            cerr << "MsgTruncatedError\n";
            exit(1);
        }

        size_t body_len = received_size - sizeof(BrokerMessageType);
        BrokerMessageType message_type;
        memcpy(&message_type, packet, sizeof(BrokerMessageType));

        switch (message_type) {
            case BrokerMessageType::SubscriberAttachmentBatch: {
                /*
                [type]
                重复直到普通数据结束：
                    [topic_id]
                    [slot_index]

                SCM_RIGHTS:
                    [shm_fd_0, event_fd_0, shm_fd_1, event_fd_1, ...]
                */

                if (body_len % kSubscriberAttachmentMetadataSize != 0) {
                    cerr << "SubscriberAttachmentBatchPayloadSizeError\n";
                    exit(1);
                }

                const size_t attachment_count = body_len / kSubscriberAttachmentMetadataSize;

                cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);

                if (attachment_count == 0) {
                    if (cmsg != nullptr) {
                        cerr << "SubscriberAttachmentBatchUnexpectedFdError\n";
                        exit(1);
                    }
                    break;
                }

                if (is_valid_scm_rights_cmsg(cmsg) == false) {
                    cerr << "SubscriberAttachmentBatchCmsgError\n";
                    exit(1);
                }

                const size_t fd_data_size = cmsg->cmsg_len - CMSG_LEN(0);
                if (fd_data_size % sizeof(int) != 0) {
                    cerr << "SubscriberAttachmentBatchFdSizeError\n";
                    exit(1);
                }

                const size_t fd_count = fd_data_size / sizeof(int);
                if (fd_count != attachment_count * 2) {
                    cerr << "SubscriberAttachmentBatchFdCountError\n";
                    exit(1);
                }

                const int *fds = reinterpret_cast<const int *>(CMSG_DATA(cmsg));

                size_t packet_offset = sizeof(BrokerMessageType);

                for (size_t attachment_index = 0; attachment_index < attachment_count; ++attachment_index) {
                    AttachmentId attachment_id = kInvalidAttachmentId;
                    memcpy(&attachment_id, packet + packet_offset, sizeof(AttachmentId));
                    packet_offset += sizeof(AttachmentId);

                    PublisherAttachment attachment;

                    memcpy(&attachment.topic_id, packet + packet_offset, sizeof(attachment.topic_id));
                    packet_offset += sizeof(attachment.topic_id);

                    memcpy(&attachment.slot_index, packet + packet_offset, sizeof(attachment.slot_index));
                    packet_offset += sizeof(attachment.slot_index);

                    attachment.shm_fd = fds[2 * attachment_index];
                    attachment.event_fd = fds[2 * attachment_index + 1];

                    attachment.shared_data = static_cast<SharedData *>(mmap(nullptr, sizeof(SharedData), PROT_READ | PROT_WRITE, MAP_SHARED, attachment.shm_fd, 0));

                    if (attachment.shared_data == MAP_FAILED) {
                        cerr << "SubscriberAttachmentBatchMmapError\n";
                        exit(1);
                    }

                    event_fd_by_attachment_id.insert({attachment_id, attachment.event_fd});

                    add_fd_to_epoll(epoll_fd, attachment.event_fd);
                    publisher_attachment_by_event_fd.insert({attachment.event_fd, std::move(attachment)});
                }
                break;
            }
            case BrokerMessageType::PublisherDisconnected: {
                if (body_len != sizeof(AttachmentId)) {
                    exit(1);
                }

                AttachmentId attachment_id = kInvalidAttachmentId;
                memcpy(&attachment_id, packet + sizeof(BrokerMessageType), sizeof(attachment_id));

                auto event_fd_it = event_fd_by_attachment_id.find(attachment_id);
                if (event_fd_it == event_fd_by_attachment_id.end()) {
                    exit(1);
                }

                int event_fd = event_fd_it->second;

                auto publisher_attachment_it = publisher_attachment_by_event_fd.find(event_fd);
                if (publisher_attachment_it == publisher_attachment_by_event_fd.end()) {
                    exit(1);
                }

                epoll_ctl(epoll_fd, EPOLL_CTL_DEL, event_fd, nullptr);
                close(event_fd);

                munmap(publisher_attachment_it->second.shared_data, sizeof(SharedData));
                close(publisher_attachment_it->second.shm_fd);

                event_fd_by_attachment_id.erase(event_fd_it);
                publisher_attachment_by_event_fd.erase(publisher_attachment_it);
                break;
            }
            default: {
                cerr << "UnexpectedBrokerMessageTypeError: type="
                     << static_cast<unsigned>(static_cast<unsigned char>(packet[0]))
                     << ", bytes=" << received_size << ", broker_fd=" << broker_fd << endl;
                exit(1);
            }
        }
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

    broker_fd = socket(AF_UNIX, SOCK_SEQPACKET, 0);

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

    char packet[kSubscriberTopicRegistrationMessageSize]{};
    packet[0] = static_cast<char>(BrokerMessageType::SubscriberTopicRegistration);
    memcpy(packet + sizeof(BrokerMessageType), &topic_count, sizeof(topic_count));
    for (uint32_t topic_index = 0; topic_index < topic_count; ++topic_index) {
        TopicId topic_id = static_cast<TopicId>(stoul(argv[topic_index + 1]));
        memcpy(packet + sizeof(BrokerMessageType) + sizeof(topic_count) + topic_index * sizeof(TopicId),
            &topic_id,
            sizeof(topic_id));
    }

    send(broker_fd, packet, sizeof(packet), 0);

    epoll_fd = epoll_create1(0);
    add_fd_to_epoll(epoll_fd, broker_fd);

    int maxevents = 1024;
    epoll_event events[maxevents];

    while (true) {
        int event_count = epoll_wait(epoll_fd, events, maxevents, -1);
        cout << "event_cout = " << event_count << endl;

        for (int i = 0; i < event_count; ++i) {
            int fd = events[i].data.fd;
            if (fd != broker_fd) {
                auto publisher_attachment_it = publisher_attachment_by_event_fd.find(fd);
                if (publisher_attachment_it == publisher_attachment_by_event_fd.end()) {
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
                process_broker_control_messages();
            }
        }
        this_thread::sleep_for(chrono::milliseconds(1));
    }

#ifdef ENABLE_DEBUG_CHECKS
    for (auto &[event_fd, publisher_attachment] : publisher_attachment_by_event_fd) {
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

    for (auto &[event_fd, publisher_attachment] : publisher_attachment_by_event_fd) {
        munmap(publisher_attachment.shared_data, sizeof(SharedData));
        close(publisher_attachment.shm_fd);
        close(event_fd);
    }

    return 0;
}
