# 跨进程无锁 SPSC 环

共享内存 + eventfd + epoll 的双进程 IPC Demo。

## 阅读顺序

| # | 文件 | 主题 |
|---|------|------|
| 1 | [01-data-race-vs-bounds-overflow.md](01-data-race-vs-bounds-overflow.md) | 两条独立的线：越界（单线程）vs Data Race（并发） |
| **2** | **[02-atomic-memory-order.md](02-atomic-memory-order.md)** ⭐ | **核心：闸门判据 — 什么时候用 relaxed，什么时候用 acquire/release** |
| **3** | **[03-release-acquire-what-it-guards.md](03-release-acquire-what-it-guards.md)** ⭐ | **深度：release/acquire 到底保护什么 — 不是原子值的新鲜度，而是别的内存的顺序** |
| 4 | [04-ring-buffer-design.md](04-ring-buffer-design.md) | 满/空歧义，留一格设计，反压条件的数学推导 |
| 5 | [05-epoll-preliminary-read.md](05-epoll-preliminary-read.md) | EPOLLET 初始化时序问题，预读解法，eventfd 计数器语义 |
| 6 | [06-local-variables-and-while-vs-if.md](06-local-variables-and-while-vs-if.md) | 局部变量固定值（正确性前提）+ while vs if 选择 |
| 7 | [07-unix-socket-shm-foundation.md](07-unix-socket-shm-foundation.md) | 📖 Linux IPC 语法基础：共享内存、Unix Socket、sendmsg/recvmsg 结构体体系 |
| 8 | [08-pthread-mutex-in-shm.md](08-pthread-mutex-in-shm.md) | 共享内存中嵌入 pthread_mutex_t：PTHREAD_PROCESS_SHARED、API 流程、与 atomic 的配合 |
| 9 | [09-type-boundaries-and-naming.md](09-type-boundaries-and-naming.md) | 共享布局、本地对象范围、接口类型与变量命名 |

## 技术栈

Unix domain socket (SCM_RIGHTS) · eventfd · epoll (EPOLLET) · POSIX shared memory (shm_open/mmap) · C++ atomic

## 项目文件

- [a.cpp](../../a.cpp) — 消费者：接收 eventfd，epoll 等待，从共享内存 ring buffer 读取到 stdout
- [b.cpp](../../b.cpp) — 生产者：创建共享内存，写入 ring buffer，通过 eventfd 通知消费者
