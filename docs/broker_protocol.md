# DriveBus broker 控制协议

本文记录当前 `broker.out`、`b.out`（publisher）、`a.out`（subscriber）和
`broker-status`（状态 CLI）之间的本机控制面协议。共享内存的数据面协议见
[shared_memory_protocol.md](shared_memory_protocol.md)。

## 传输与公共规则

- 地址是 AF_UNIX 路径 `/tmp/broker.sock`，socket 类型是 `SOCK_SEQPACKET`。
- 一次 `send()` 或 `sendmsg()` 是一条完整控制记录；接收缓冲区不足会丢弃该记录
  的尾部，因此接收端必须为协议最大包预留足够空间并检查截断。
- 普通记录的二进制布局为：

  ```text
  message = message_type + payload
  ```

  `message_type` 是 `BrokerMessageType`，底层类型为 `uint8_t`；payload 的含义只由
  该类型决定。
- 携带文件描述符时，普通字节仍在 payload，文件描述符放在 `SCM_RIGHTS` 辅助数据中。
  它们必须由同一次 `recvmsg()` 接收，不能先 `recv()` 普通字节再接收 FD。
- 枚举值已作为裸字节上网。新增消息类型只能追加到末尾，或显式固定所有数值；
  不能在中间插入后只重启其中一个端点。

## topic 与槽位

`TopicId` 当前值如下：

| 值 | topic |
|---:|---|
| 1 | `Camera` |
| 2 | `Lidar` |
| 3 | `VehicleState` |

每个 publisher 当前只注册一个 topic；一个 topic 可有多个 publisher。每个
`(topic, publisher)` 有 16 个独立 subscriber 槽位，槽位下标是 `uint32_t`。

## 控制记录

| 类型 | 方向 | payload | 辅助数据 |
|---|---|---|---|
| `StatusQuery` | CLI → broker | 空，记录总长为 1 字节 | 无 |
| `StatusSnapshot` | broker → CLI | UTF-8 状态文本，最大 4095 字节 | 无 |
| `SubscriberTopicRegistration` | subscriber → broker | `uint32_t topic_count` + 固定 5 个 `TopicId` 槽；仅前 `topic_count` 个有效 | 无 |
| `PublisherTopicRegistration` | publisher → broker | 一个 `TopicId` | 一个 publisher-owned `shm_fd` |
| `SubscriberEventFdAndSlot` | broker → publisher | 一个 `uint32_t slot_index` | 一个对应槽位的 `event_fd` |
| `SubscriberDisconnected` | broker → publisher | 一个 `uint32_t slot_index` | 无 |
| `SubscriberAttachmentBatch` | broker → subscriber | 重复的 `[AttachmentId][TopicId][uint32_t slot_index]` | 每个 attachment 依次携带 `[shm_fd, event_fd]` |
| `PublisherDisconnected` | broker → subscriber | 一个 `AttachmentId` | 无 |

### `SubscriberTopicRegistration`

subscriber 启动时发送。`topic_count` 的有效范围是 1–5；记录总长始终是
`sizeof(BrokerMessageType) + sizeof(uint32_t) + 5 * sizeof(TopicId)`，未使用的 topic
槽不参与解释。

### `PublisherTopicRegistration`

publisher 创建并初始化 `SharedData` 后发送。payload 的 `TopicId` 与 `SCM_RIGHTS`
中的共享内存 FD 共同描述一条 publisher 数据源。broker 保留自己的 FD 副本；
subscriber 只映射收到的副本，不调用 `shm_unlink()`。

### 初始订阅者计数（历史兼容帧）

publisher 注册成功后，broker 会先发送一个**裸** `uint32_t subscriber_count`，随后对每个
已存在 subscriber 发送一条 `SubscriberEventFdAndSlot`。这个计数帧不带
`BrokerMessageType`，是当前 publisher 启动序列的特例，不应与上表的普通记录混淆。

### `SubscriberAttachmentBatch`

batch 的 attachment 数量从普通 payload 长度推导：

```text
attachment_count = payload_bytes / (sizeof(AttachmentId) + sizeof(TopicId) + sizeof(uint32_t))
```

接收端要求 payload 恰好整除元数据大小，且 FD 数必须等于 `2 * attachment_count`。
第 `i` 组元数据对应第 `2*i` 个 `shm_fd` 与第 `2*i+1` 个 `event_fd`。`AttachmentId` 是
broker 单调分配的逻辑身份；FD 号在不同进程中不相同，不能用作 attachment 身份。

### 断连记录

subscriber 断连时，broker 向每个关联 publisher 发送 `SubscriberDisconnected(slot_index)`；
publisher 归还该槽尚未消费消息的引用并退休该槽。

publisher 断连时，broker 向每个关联 subscriber 发送
`PublisherDisconnected(attachment_id)`；subscriber 据此删除 epoll 监听、解除映射并关闭
本地 FD。

### 状态查询

`broker-status` 连接到同一控制 socket，发送一字节 `StatusQuery`，接收一条
`StatusSnapshot` 后退出。snapshot 包含：

- 已注册连接的 fd、角色、topic 与 subscriber attachment 数；
- 每个 topic 的 publisher/subscriber 路由；
- 每个 `(topic, publisher_fd)` 的已占用 `slot -> subscriber_fd` 和空闲槽位。

`registered` 只表示 broker 当前仍持有该控制连接的注册状态；它不是心跳，也不证明
对端进程正在调度或业务处理正常。若状态文本超出 4095 字节，broker 返回
`complete=0` 与 `reason=status_snapshot_too_large`，不会静默截断。

## 当前边界

本控制协议尚未定义版本协商、认证/访问控制、心跳、跨主机传输、QoS、持久会话或网络分区恢复。
协议字段也是本机构建产物的 ABI，所有端点必须使用相同的头文件、C++ ABI 和共享内存布局。
