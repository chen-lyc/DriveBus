#include <cstring>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/socket.h>

void set_fd_nonblocking(int fd) {
    int old_option = fcntl(fd, F_GETFL);
    int new_option = old_option | O_NONBLOCK;
    fcntl(fd, F_SETFL, new_option);
}

void add_fd_to_epoll(int epoll_fd, int fd) {
    epoll_event event{};
    event.data.fd = fd;
    event.events = EPOLLET | EPOLLIN;
    set_fd_nonblocking(fd);
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &event);
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
    if (n < 0) return -1;

    struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    if (cmsg == nullptr) return -1;
    if (cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SCM_RIGHTS) return -1;

    received_fd = *reinterpret_cast<int *>(CMSG_DATA(cmsg));
    return n;
}