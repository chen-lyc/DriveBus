#pragma once

#include <cerrno>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

bool set_fd_nonblocking(int fd);
bool add_fd_to_epoll(int epoll_fd, int fd);

inline bool signal_eventfd(const int event_fd) {
    while (eventfd_write(event_fd, 1) < 0) {
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN) {
            return true;
        }
        return false;
    }
    return true;
}

inline bool drain_eventfd(const int event_fd) {
    eventfd_t ignored;
    while (eventfd_read(event_fd, &ignored) < 0) {
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN) {
            return true;
        }
        return false;
    }
    return true;
}

inline void close_fd(int &fd) {
    if (fd >= 0) {
        close(fd);
        fd = -1;
    }
}

// 跟踪外部 FD 变量；该变量在守卫未解除期间必须保持有效。
// 未解除时，析构会关闭该变量中的 FD，并将变量置为 -1。
class FdGuard {
  public:
    explicit FdGuard(int &fd) noexcept : fd_(&fd) {}
    ~FdGuard() noexcept;

    FdGuard(const FdGuard &) = delete;
    FdGuard &operator=(const FdGuard &) = delete;

    // 返回 FD、将原变量置为 -1 并解除守护，不关闭 FD；已解除时返回 -1。
    int release() noexcept;

  private:
    // nullptr 表示已解除，避免原变量复用后误关新 FD；引用无法表示空绑定。
    int *fd_;
};

// Transferred SCM_RIGHTS entries must be set to -1 before this guard expires.
class ReceivedFdsGuard {
  public:
    explicit ReceivedFdsGuard(msghdr &message) noexcept : message_(message) {}
    ~ReceivedFdsGuard() noexcept;

    ReceivedFdsGuard(const ReceivedFdsGuard &) = delete;
    ReceivedFdsGuard &operator=(const ReceivedFdsGuard &) = delete;

  private:
    msghdr &message_;
};

void close_received_fds(msghdr &message) noexcept;
bool is_valid_scm_rights_cmsg(cmsghdr *cmsg);

ssize_t send_packet_with_fd(int conn_fd, void *packet, size_t packet_size, int fd_to_send);
ssize_t send_packet_with_fds(int conn_fd, void *packet, size_t packet_size, const std::vector<int> &fds_to_send);
ssize_t receive_packet_with_fd(int conn_fd, void *packet, size_t packet_size, int &received_fd);
