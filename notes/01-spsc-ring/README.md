# 跨进程无锁 SPSC 环

共享内存 + eventfd + epoll 的双进程 IPC Demo。

## 阅读顺序

| # | 文件 | 主题 |
|---|------|------|
| 1 | [01-data-race-vs-bounds-overflow.md](01-data-race-vs-bounds-overflow.md) | 两条独立的线：越界（单线程）vs Data Race（并发） |
| **2** | **[02-atomic-memory-order.md](02-atomic-memory-order.md)** ⭐ | **核心：闸门判据 — 什么时候用 relaxed，什么时候用 acquire/release** |
| 3 | [03-ring-buffer-design.md](03-ring-buffer-design.md) | 满/空歧义，留一格设计，反压条件的数学推导 |
| 4 | [04-epoll-preliminary-read.md](04-epoll-preliminary-read.md) | EPOLLET 初始化时序问题，预读解法，eventfd 计数器语义 |
| 5 | [05-local-variables-and-while-vs-if.md](05-local-variables-and-while-vs-if.md) | 局部变量固定值（正确性前提）+ while vs if 选择 |
| 6 | [06-unix-socket-shm-foundation.md](06-unix-socket-shm-foundation.md) | 📖 Linux IPC 语法基础：共享内存、Unix Socket、sendmsg/recvmsg 结构体体系 |

## 技术栈

Unix domain socket (SCM_RIGHTS) · eventfd · epoll (EPOLLET) · POSIX shared memory (shm_open/mmap) · C++ atomic

## 项目文件

- [a.cpp](../../a.cpp) — 消费者：接收 eventfd，epoll 等待，从共享内存 ring buffer 读取到 stdout
- [b.cpp](../../b.cpp) — 生产者：创建共享内存，写入 ring buffer，通过 eventfd 通知消费者
