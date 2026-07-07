# EPOLLET + 预读：解决边缘触发竞态

## 问题

`a.out`（消费者）和 `b.out`（生产者）通过 eventfd + EPOLLET 通知。初始化时序存在竞态：

```
b.out:  send_fd(efd) ── 进入循环 ── write(efd, 1) ── write(efd, 1) ── ... ── exit
                            ↑ 边沿: 0→1  ... 边沿: 1→2（不是从 0 开始，不触发新边沿）

a.out:  recv_fd(efd) ── epoll_ctl(ADD, efd, EPOLLET) ── shm_open ── mmap ── epoll_wait(-1)
                                                                                  ↑
                                                              如果所有 write(efd) 在此之前完成：
                                                              counter > 0，但没有"0→正数"的边沿
                                                              → 永久阻塞
```

**EPOLLET 的语义**：只在 fd 状态发生**变化**时通知。对 eventfd 来说，变化 = 计数器从 0 变为正数。

## 尝试一：去掉 EPOLLET（水平触发）

```cpp
event.events = EPOLLIN;  // 不设 EPOLLET
```

水平触发：只要 fd 可读，就一直通知。没有竞态问题。

**缺点**：如果消费者没读完就返回 epoll_wait，会立刻再次被唤醒（忙等），需要配合非阻塞读 + 读空才返回的 discipline。

## 采用的解法：保留 EPOLLET + 预读

不是放弃 EPOLLET，而是补上边缘触发标准模式中缺失的一步——**在 epoll_wait 前先尝试读一次**。

[a.cpp:85-96](a.cpp#L85-L96)：

```cpp
// 1. 注册到 epoll
epoll_ctl(epollfd, EPOLL_CTL_ADD, efd, &event);  // EPOLLET | EPOLLIN

// 2. 预读：消费可能已经到达的数据
uint64_t val;
ssize_t n = read(efd, &val, sizeof(val));
if (n > 0) {
    // eventfd 有数据 → 先处理掉
    int writed = p->offset_write.load(memory_order_acquire);
    int read   = p->offset_read.load(memory_order_relaxed);
    while (writed > read) {
        write(1, p->data + read, writed - read);
        read = writed;
        p->offset_read.store(writed, std::memory_order_relaxed);
        writed = p->offset_write.load(memory_order_acquire);
    }
}

// 3. 进入 epoll 循环：此后只有"新的"0→正数边沿会唤醒
while (true) {
    int num = epoll_wait(epollfd, events, maxevents, timeout);
    ...
}
```

**逻辑**：

```
预读时如果有数据 → 消费掉 → eventfd counter 归零
                 → 后续 b.out 每次 write(efd) 都会产生新的 0→1 边沿
                 → epoll_wait 可靠唤醒

预读时如果没数据 → read 返回 EAGAIN（eventfd 是非阻塞的）
                 → epoll_wait 等第一条边沿
                 → 可靠唤醒
```

两种分支都安全。

## 为什么 read(efd) 在 a.out 中是非阻塞的

`b.out` 创建 eventfd 时指定了 `EFD_NONBLOCK`：

```cpp
int efd = eventfd(0, EFD_NONBLOCK);
```

通过 Unix socket 的 `SCM_RIGHTS` 传给 `a.out` 后，新 fd 指向**同一个 `struct file`**。`O_NONBLOCK` 是文件状态标志（file status flag），存储在 `struct file->f_flags` 中，不是 per-fd 的描述符标志。所以非阻塞语义被保留。

## eventfd 的计数器语义

```
write(efd, &val, sizeof(val))   → counter += val
read(efd, &val, sizeof(val))    → 返回 counter 的当前值，counter 归零

如果 counter = 0 且非阻塞：read 返回 EAGAIN
如果 counter = 0 且阻塞：  read 阻塞直到 counter > 0
```

**关键**：一次 `read` 消费全部累积的写入次数。所以 eventfd 是"通知累加器"，不是"事件队列"。配合 EPOLLET：只需一次唤醒，消费者用 while 循环处理共享内存中的所有数据。

## 小结

| 方案 | 做法 | 代价 |
|------|------|------|
| 水平触发 | 去掉 EPOLLET | 未读完返回时重复唤醒 |
| 预读（本项目） | epoll_wait 前 read 一次 | 多一次初始 read |

预读本质是 EPOLLET 标准模式——对任何边缘触发的 fd，都应先做非阻塞 I/O，读不到再等 epoll。
