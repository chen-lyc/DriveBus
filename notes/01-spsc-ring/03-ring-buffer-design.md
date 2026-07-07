# Ring Buffer：满/空歧义与留一格设计

## 核心问题

Ring buffer 使用两个指针 `read` 和 `write`，都单调递增（取模后即为实际位置）。当 `read == write` 时，有两种完全不同的含义：

| 情况 | read == write 含义 | 数据量 |
|------|--------------------|--------|
| 空缓冲区 | 没有数据可读 | 0 |
| 满缓冲区 | 写者绕了一圈追上读者 | N（缓冲区大小） |

**这就是"满/空歧义"。**

## 为什么会出现

```
初始状态:                         绕回后 满:
[ _ _ _ _ _ _ _ _ ]              [ D D D D D D D D ]
  R/W                              W/R
                                   (二者指向同一位置)
read=0, write=0                  read=0, write=8 (mod 8 = 0)
→ 空                             → 满的但看起来和"空"一样
```

无额外信息时，消费者看到 `read == write` 无法判断该不该读。

## 标准解法：留一个空位

缓冲区大小 N，可用容量 N-1。**写者永远不填满最后一个空位**：

```
写者可以写到这里:
  [ D D D D D D _ ]
    R           W
    read=0, write=6 → 6 个数据可读

写者等待条件:
  (write + 1) % N == read → 满了，等读者消费
  
read == write → 永远只表示"空"
```

**代价**：浪费一个槽位。**收益**：消除了歧义，不需要额外标志位。

## 本项目的实现

[b.cpp:82](b.cpp#L82) 的反压条件：

```cpp
while ((rd > writed && rd <= writed + sizeof(temp))
    || writed + sizeof(temp) >= rd + sizeof(p->data))
{
    this_thread::sleep_for(chrono::milliseconds(1));
    rd = p->offset_read.load(memory_order_relaxed);
    writed = p->offset_write.load(memory_order_relaxed);
}
```

条件二 `writed + 64 >= rd + 1664` 展开：
```
writed >= rd + 1600
```

含义：写者最多领先读者 `1664 - 64 = 1600` 字节。**写者永远保留至少 64 字节（一块）的间隙，不会绕回追上读者。**

```
缓冲区 1664 字节，每块 64 字节，26 块

读者位置: rd                               写者位置: writed
         ↓                                          ↓
[ D D D D | _ _ _ ... _ _ _ | D D D ... D D D D D D ]
            ← 至少 64 字节间隙 →
            
writed 永远不会到达 rd-1 的位置
→ writed == rd 永远只表示"空"
```

## 为什么预读曾经失败

当 b.out 在 a.out 预读之前跑完所有 52 次迭代：

```
b.out 写了两整圈（52 × 64 = 3328 = 2 × 1664）
→ offset_write 恰好绕回 0
→ a.out 看到：offset_read=0, offset_write=0
→ while(0 > 0) → 跳过
→ 数据被忽略
```

但这不是满/空歧义——因为反压的存在，b.out 根本不应该在 a.out 没读的情况下写满整圈。**之所以发生了，是因为 a.out 还没启动读（`offset_read` 一直是 0），b.out 的忙碌等待条件 `0 > writed` 永远为假，反压从未触发。**

这是**初始化时序**问题，不是环形缓冲区设计问题。读者必须尽快启动消费，或者在写者侧加超时/最大领先量限制。

## 小结

| 设计选择 | 效果 |
|----------|------|
| N 个槽位全用 | `read == write` 歧义，需要额外标志 |
| 留 1 个空位 | `read == write` ≡ 空，不需要标志 |
| 本项目 | 反压保证 writed 最多领先 rd + 1600，等价于留 64 字节空位 |
