#include "../include/fd_helpers.hpp"
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/socket.h>

bool set_fd_nonblocking(int fd) {
    int old_option = fcntl(fd, F_GETFL);
    if (old_option < 0) return false;

    int new_option = old_option | O_NONBLOCK;
    return fcntl(fd, F_SETFL, new_option) == 0;
}

bool add_fd_to_epoll(int epoll_fd, int fd) {
    epoll_event event{};
    event.data.fd = fd;
    event.events = EPOLLET | EPOLLIN;
    if (!set_fd_nonblocking(fd)) return false;

    return epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &event) == 0;
}

bool is_valid_scm_rights_cmsg(cmsghdr *cmsg) {
    return cmsg != nullptr &&
        cmsg->cmsg_level == SOL_SOCKET &&
        cmsg->cmsg_type == SCM_RIGHTS &&
        cmsg->cmsg_len >= CMSG_LEN(0);
}

ssize_t send_packet_with_fd(int conn_fd, void *packet, size_t packet_size, int fd_to_send) {
    struct iovec iov;
    iov.iov_base = packet;
    iov.iov_len = packet_size;

    struct msghdr msg{};
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

    *reinterpret_cast<int *>(CMSG_DATA(cmsg)) = fd_to_send;

    return sendmsg(conn_fd, &msg, 0);
}

ssize_t send_packet_with_fds(int conn_fd, void *packet, size_t packet_size, const std::vector<int> &fds_to_send) {
    struct iovec iov;
    iov.iov_base = packet;
    iov.iov_len = packet_size;

    struct msghdr msg{};
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;

    size_t len = sizeof(int) * fds_to_send.size();
    std::vector<char> control(CMSG_SPACE(len), 0);

    if (!fds_to_send.empty()) {
        msg.msg_control = control.data();
        msg.msg_controllen = control.size();

        struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
        cmsg->cmsg_level = SOL_SOCKET;
        cmsg->cmsg_type = SCM_RIGHTS;
        cmsg->cmsg_len = CMSG_LEN(len);

        memcpy(CMSG_DATA(cmsg), fds_to_send.data(), len);
    }

    return sendmsg(conn_fd, &msg, 0);
}

ssize_t receive_packet_with_fd(int conn_fd, void *packet, size_t packet_size, int &received_fd) {
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

    ssize_t n = recvmsg(conn_fd, &msg, 0);
    if (n <= 0) return n;
    if (msg.msg_flags & MSG_CTRUNC) {
        errno = EMSGSIZE;
        return -1;
    }

    struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    if (cmsg == nullptr ||
        cmsg->cmsg_level != SOL_SOCKET ||
        cmsg->cmsg_type != SCM_RIGHTS ||
        cmsg->cmsg_len != CMSG_LEN(sizeof(received_fd))) {
        errno = EPROTO;
        return -1;
    }

    memcpy(&received_fd, CMSG_DATA(cmsg), sizeof(received_fd));
    return n;
}
