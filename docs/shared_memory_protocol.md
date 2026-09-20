# DriveBus 共享内存数据面协议

本文记录 publisher 与 subscriber 通过 `SharedData` 交换数据的当前内存布局和发布/消费约定。
它不是网络协议：双方通过 broker 转交的 `shm_fd` 映射同一段本机共享内存，并由配对的
`event_fd` 得到“可能有新数据”的通知。控制记录与 FD 交接见
[broker_protocol.md](broker_protocol.md)。

## ABI 边界

- 映射长度是 `sizeof(SharedData)`；publisher 创建并初始化，subscriber 以
  `MAP_SHARED | PROT_READ | PROT_WRITE` 映射。
- 该布局包含 `std::atomic` 和可选的 `ChunkUsageTracker`。所有进程必须使用相同的
  `shared_memory_layout.hpp`、编译器 ABI 以及 `ENABLE_DEBUG_CHECKS` 开关；它没有跨版本、
  跨架构或持久化兼容承诺。
- 一个 publisher 有一段独立 `SharedData`；同一 topic 的多个 publisher 不共享 ring，
  也没有 topic 全局消息顺序。

## 固定布局

| 字段 | 含义 |
|---|---|
| `descriptor_read_indices[16]` | 每个 subscriber 槽的下一个未消费 descriptor 索引；`kInvalidIndex` 表示该槽未启用 |
| `descriptor_write_index` | publisher 的下一个未写 descriptor 索引 |
| `desc_ring[16]` | `MessageDescriptor { uint32_t offset; uint32_t len; }` 的环 |
| `head.offset[5]` / `tail.offset[5]` | 五个大小类别的空闲 chunk 链表端点 |
| `chunk_reference_counts[128]` | 按 subscriber 槽位组成的位掩码，不是普通计数器 |
| `data[21504]` | chunk payload 池，同时保存空闲链表的 next offset |

大小类别依次为 16、128、256、512、1024 字节；chunk 数量依次为 64、32、16、8、8。
`find_size_class(len)` 选择第一个可容纳该长度的类别。空闲 chunk 的开头四字节保存下一个
空闲 offset；`kInvalidOffset` 表示链尾。

## 发布协议

publisher 对每条消息执行：

1. 根据长度选择大小类别，从对应 free list 的 `head` 取一个 chunk。
2. 在 `data[offset...]` 写入内容；当前示例格式是 `int magic`、`int sequence`、其余测试字节。
3. 在 `desc_ring[wr]` 写入该 chunk 的 `offset` 与消息 `len`。
4. 为当前所有 subscriber 槽组合 `chunk_reference_counts[chunk]` 的位掩码。
5. 对 `descriptor_write_index` 做 release store，发布 descriptor 与 payload。
6. 向每个 subscriber 的 `event_fd` 写入 `uint64_t 1`，触发其 epoll 通知。

`event_fd` 只是唤醒通知；subscriber 必须读取 `descriptor_write_index` 后决定实际可消费区间。

## 消费与归还协议

subscriber 收到 eventfd 通知后：

1. acquire load `descriptor_write_index`，读取自己 `[read_index, write_index)` 范围的 descriptor 和 payload。
2. 对消息所属 chunk 执行 `fetch_and(~subscriber_slot_bit, acq_rel)`，清除自己的引用位。
3. 前进本地读索引，并对 `descriptor_read_indices[slot]` 做 release store。
4. 若清位前掩码只包含自己的 bit，说明它是最后一个消费者；将该 chunk 追加回对应 free list 的 tail。

subscriber 注册到 publisher 时，publisher 将该槽的读索引初始化为当时的写索引，因此晚加入者只看见
加入后的消息。

## 断连补偿

broker 通知 publisher 某 subscriber 槽断连后，publisher 释放该槽从 read index 到 write index
之间尚未消费 descriptor 的引用位；最后引用同样回到 free list。随后该槽读索引设为
`kInvalidIndex`，eventfd 从 publisher 的跟踪表删除并关闭。

## 当前内存序与边界

当前发布路径用 release store 发布 write index，subscriber 用 acquire load 取得已发布数据；
subscriber 用 release store 发布 read index。free-list tail 的发布使用 release，publisher 读取 tail
使用 acquire。

未定义的边界包括：布局版本迁移、跨机器共享、持久化恢复、恶意输入隔离，以及对所有异常中途退出的
完全一致快照。调试模式中的跨进程 robust mutex 仅用于 chunk 使用检查，不是数据面主同步机制。
