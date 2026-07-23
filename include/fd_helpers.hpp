#pragma once

void set_fd_nonblocking(int fd);
void add_fd_to_epoll(int epoll_fd, int fd);