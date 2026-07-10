# release/acquire 到底保护什么

> 前置阅读：[02-atomic-memory-order.md](02-atomic-memory-order.md)（闸门判据）

## 核心：release/acquire 保护的不是原子变量本身的新鲜度

这是一个很容易滑进去的误解。拆开看：

```
原子变量本身的值 → atomic 的原子性就保证了，relaxed 也读到某次真实 store 写入的值
release/acquire → 保护的是"别的普通内存"的访问顺序
```

**acquire 不保证"读到的是最新的值"。** 它只保证"如果读到了某个值，那么这个值对应的 release 之前的所有普通内存写入，对 acquire 之后的代码一定可见"。

## 两条独立的线

以本项目 `desc_ring` 的并发访问为例。

### 线一：满检查 — 只需要值，relaxed 够

```cpp
// b.cpp 忙碌等待
int rd = p->offset_read.load(std::memory_order_acquire);   // relaxed 也够
int wr = p->offset_write.load(memory_order_relaxed);
while (满) { ... }
```

这里的 `rd` 只用来判断"能不能写"。最坏情况读到偏旧偏小的 `rd` → 以为环比实际更满 → 多自旋一圈。**保守，但绝不越界。** 它不可能读到一个"比现实更超前的 `rd`"——因为 `rd` 的值是 a.out 真实 store 过的，而 a.out 不会在没读完时就超前更新。

**这个检查需要的是值的正确性，不需要任何 happens-before。`relaxed` 成立。**

### 线二：槽位复用 — 需要顺序，要 release/acquire

两个线程访问**同一块 `desc_ring[i]`**：

```
a.out:  copy(...)  // ① 普通读 desc_ring[i]，把描述符拷走
         ↓ 程序顺序
        p->offset_read.store(..., release)  // ② 开闸——"这个槽位可以覆盖了"
                                              ↓ happens-before
b.out:                                     p->offset_read.load(acquire)  // ③ 过闸
                                              ↓ 程序顺序
                                           memcpy(p->desc_ring + wr, ...)  // ④ 普通写 desc_ring[i]
```

① 和 ④ 是两次**普通内存访问**（一读一写，同一个 `desc_ring` 槽位）。它们之间没有任何同步保护的话就是 data race。

② 和 ③（`offset_read` 的 `release/acquire`）夹在 ① 和 ④ 之间，建立了 ① happens-before ④ —— 消费者拷走 happens-before 生产者覆盖。

**这条边保护的不是 `offset_read` 的值新不新——它保护的是 `desc_ring` 的槽位复用。**

## tail 顶包：为什么原始代码里 TSan 不报

原始代码有内存池。a.out 归还空闲块时会写 `tail.offset_16`：

```cpp
// a.out read_data 中
copy(...)  // ① 读 desc_ring[i]，把描述符拷走
...
memcpy(p->data + last_tail, &offset, sizeof(int));             // 写 next_offset
p->tail.offset_16.store(offset, std::memory_order_release);   // ② tail release
                                                                 ↓ happens-before  ← 这条边是 tail 的！
// b.out 下一轮开头
p->tail.offset_16.load(std::memory_order_acquire);             // ③ tail acquire
...
memcpy(p->desc_ring + wr, &desc, sizeof(MessageDesc));         // ④ 写 desc_ring[i]
```

**tail.offset_16 的 release/acquire 恰好夹在 ① 和 ④ 之间。** `desc_ring` 槽位复用的 happens-before 已经被 tail 偷偷建立好了，`offset_read` 的边被"代班"了。

这就是为什么把 `offset_read` 全改 `relaxed`、缩 `kDescNum`、加 sleep、改 `if` 跑十分钟——**TSan 都不报。不是窗口窄，是那条边根本不需要 `offset_read` 来提供。**

## 验证：拿走顶包的

[ring_harness.cpp](../../ring_harness.cpp) 去掉了所有池子代码。两个线程之间只剩环：

```
a.out:  copy(p->desc_ring + rd, ...)  // ①
        p->offset_read.store(..., release/acq_rel)  // ②
                                                       ↓  ← 唯一的 happens-before 候选
b.out:                                                p->offset_read.load(acquire/relaxed)  // ③
                                                       memcpy(p->desc_ring + wr, ...)  // ④
```

把 ②③ 全部改 `relaxed` → ① 和 ④ 之间没有任何同步 → **跑不过 20 次就红。**

改回 `release/acquire` → ① happens-before ④ → **绿。**

## 错误认知对照

> "tail.acquire 确保读到最新的 offset_read。"

这个表述混淆了两件完全不同的事：

| | 值的新鲜度 | 别的内存的顺序 |
|---|---|---|
| 谁保证 | `atomic` 的原子性（relaxed 也够） | `release/acquire` |
| 偏旧会怎样 | 偏旧 → 多自旋一圈，安全 | 没有 → data race |

tail 的 acquire **和 offset_read 的值新不新鲜一点关系都没有。** 它只是碰巧在程序顺序上排在 `copy` 之后、`memcpy` 之前，于是成了 ①→④ 的那条 happens-before 边——保护的是 `desc_ring[i]`，不是 `offset_read`。

## 通用判法

以后判断某个数据竞态是否被覆盖：

```
1. 锁定冲突的两次普通内存访问（一读一写，同一地址）
2. 看这两次访问之间，在程序顺序上有没有一对 release→acquire 夹着
3. 有 → 竞态被覆盖。没有 → data race
```

**和原子变量本身的值新不新鲜完全无关。**

## 验证一条边的方法

> 要验证一条 happens-before 边是必要的，必须拿走所有可能"代班"的其他边。

x86 强序遮过、eventfd syscall 遮过、tail 代班遮过——这个模式值得记住：每次都恰好有别的东西提供了你没显式建立的保证。隔离测试（ring_harness）就是把所有可能兜底的因素拿掉，让唯一候选边独力承担 happens-before——如果它撑不住，TSan 会告诉你。
