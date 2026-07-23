#include <fcntl.h>
#include <sys/epoll.h>

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