#pragma once

#include <sys/socket.h>
#include <unistd.h>
#include <vector>

bool set_fd_nonblocking(int fd);
bool add_fd_to_epoll(int epoll_fd, int fd);

bool is_valid_scm_rights_cmsg(cmsghdr *cmsg);

ssize_t send_packet_with_fd(int conn_fd, void *packet, size_t packet_size, int fd_to_send);
ssize_t send_packet_with_fds(int conn_fd, void *packet, size_t packet_size, const std::vector<int> &fds_to_send);
ssize_t receive_packet_with_fd(int conn_fd, void *packet, size_t packet_size, int &received_fd);
