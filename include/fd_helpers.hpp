#pragma once

#include <unistd.h>

void set_fd_nonblocking(int fd);
void add_fd_to_epoll(int epoll_fd, int fd);

ssize_t send_packet_with_fd(int conn_fd, void *packet, size_t packet_size, int fd_to_send);
ssize_t receive_packet_with_fd(int conn_fd, void *packet, size_t packet_size, int &received_fd);