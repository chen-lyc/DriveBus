# Bug：payload 头部魔数被 free list 覆盖

> **快速识别线索**：读端发现 `magic` 错，但紧随其后的 `seq` 仍对；`actual magic` 又恰好等于同一 size class 的下一个 chunk 偏移量。先判断“魔数被改写”，再追踪谁能写 payload 的头部。

## 这次的现象

本笔记记录的是工作区在 `5f7f9c5` 之后的未提交多读端位图改造，不代表后续版本仍含同一问题。

```text
read 557 byte, offset is 14336, seq is 3
error magic: expected is 21354532, actual is 15360
```

两个读端映射的是同一段共享内存，但它们不是在同一时刻读取。一个读端可以先读到正确 payload；如果 chunk 被过早归还，另一个读端随后读取同一 `offset` 时，看到的就是被 free list 改写后的字节。

## 先从数字判断：15360 不是随机脏数据

`557` 落在 `512 < len <= 1024`，所以它属于 1024-byte 的 size class。该类中：

```text
chunk offset = 14336
chunk size   = 1024
next offset  = 14336 + 1024 = 15360
```

`actual magic == 15360` 正好是这个 chunk 的后继偏移量。更关键的是 `seq == 3` 仍然正确：生产者曾经写入过 `{magic, seq}`，后来只改写了前 4 字节。

```text
data[14336 + 0] = 15360  // free-list 的 next offset，覆盖了 magic
data[14336 + 4] = 3      // 原消息 seq 仍在
```

这条“值恰好等于同一 size class 的相邻偏移”的关系，构成了定位 free list 的直接证据；它不是仅凭“魔数错了”猜出来的。

## 为什么 free list 会改写 magic 所在位置

一个空闲 chunk 的头部复用为链表节点：前 4 字节存放下一个空闲 chunk 的 offset。

```cpp
// 初始 free list：在每个空闲 chunk 头部写 next_offset。
uint32_t next_offset = offset + chunk_size_bytes;
memcpy(shm.data + offset, &next_offset, sizeof(uint32_t));
```

分配给消息后，生产者复用同一位置写 payload 头：

```cpp
memcpy(p->data + head_off, &kMagic, sizeof(int));
memcpy(p->data + head_off + sizeof(int), &seq, sizeof(int));
```

chunk 被归还时，free list 又会把某个已归还 chunk 的头部写成下一个节点的 offset：

```cpp
memcpy(p->data + last_tail_off, &offset, sizeof(uint32_t));
p->tail.offset[size_class].store(offset, std::memory_order_release);
```

因此，**chunk 一旦已安全归还，原 magic 被覆盖是正常现象**；本事件的问题不是“free list 写错了”，而是“仍有读端持有该 chunk 时，它已经被归还”。

## 本次提前归还的因果链

这次把 `chunk_reference_counts` 从“计数”改成了“订阅者位图（bitset）”：每个读端拥有一个 bit，只有所有 bit 都清除后，chunk 才能归还。

两个 slot 为 `0`、`1` 时，正确的持有集合应是：

```text
slot 0 = 0b01
slot 1 = 0b10
两个读端同时持有 = 0b11
```

但当时写端给新消息写入的是：

```cpp
1 << (conn_fds.size() - 1)  // 两个连接时为 0b10
```

它只登记了 slot 1，漏掉 slot 0。于是：

```text
slot 0 清 0b01：0b10 -> 0b10，自己的持有权本来就不存在
slot 1 清 0b10：0b10 -> 0b00，被误判为最后一个持有者
                         ↓
                     chunk 被归还到 free list
                         ↓
slot 0 之后再按 descriptor 的 offset 读 payload，看到 15360
```

这里的 `fetch_and(~subscriber_reference_bit)` 是“清除某个订阅者 bit”的幂等操作：同一个 bit 清两次不会继续减少其他 bit。它解决的是重复清理同一订阅者的问题；前提仍然是写端最初登记了**全部**活跃订阅者的 bit。

仅凭终端里的“读端 1 / 读端 2”标签，不能断言哪一个恰好是 slot 0；应同时打印 `subscriber_slot_index`。但无论标签如何对应，先归还的读端和后读 payload 的读端会形成上面的生命周期链。

## 下次看到 magic 错时的排查顺序

1. 记录 `expected`、`actual`、消息长度和 chunk `offset`，不要只看“错了”。
2. 看 `actual` 是否等于 `offset ± chunk_size`、某个 size class 边界、或已知链表节点偏移。
3. 列出所有会写 `data + offset` 前 4 字节的路径：payload 写入、free-list 初始化、chunk 归还、以及任何 allocator 元数据写入。
4. 区分 descriptor 和 payload：descriptor 可以先复制到本地数组，但它保存的 `offset` 仍指向共享 payload；payload 生命周期提前结束，局部 descriptor 也救不了它。
5. 若使用位图引用，逐位写出“发布时有哪些 holder bit、每个读端清哪个 bit、何时变成零”。不要把“连接数”当成“活跃位集合”。

## 使用边界

`magic` 不匹配不必然等于 free-list 覆盖；本次结论依赖三段证据同时成立：`15360 == 14336 + 1024`、源码确实会把 next offset 写进 chunk 头、并且位图遗漏了一个活跃读端。缺少其中任一段时，只能把 free list 当作待验证假设。

## 代码锚点（本次工作区快照）

- [size class、chunk 大小与首偏移](../../include/shared_memory_layout.hpp#L12-L44)
- [free-list 初始化：chunk 头写 `next_offset`](../../b.cpp#L155-L165)
- [生产者写 `{magic, seq}`](../../b.cpp#L293-L300)
- [写端发布的订阅者位图](../../b.cpp#L303-L318)
- [读端清自己的 bit](../../a.cpp#L90-L99)
- [最后持有者归还 chunk，并在 chunk 头写链表链接](../../a.cpp#L117-L119)
- [读端先复制 descriptor、再读取共享 payload](../../a.cpp#L124-L131)
