# 局部变量固定值与 while/if 选择：并发读取的正确性保证

本笔记覆盖两个独立但相关的正确性问题：

1. **为什么要用局部变量固定 `atomic.load()` 的结果**——这不是性能优化，是正确性的前提
2. **`while` vs `if`**——一次事件唤醒后，消费者应该读一轮还是追平写入者

---

## 一、前提：为什么要用局部变量固定值

写者（另一进程）在持续修改 `offset_write`。如果每次都直接 `load()` 而不存到局部变量，**同一次判断 / 同一次处理中的多次 load 会拿到不同的快照**。

### 最致命的场景

假设你把预读的 while 循环写成这样——每次用到 `wr` 或 `rd` 就当场 load，不固定：

```cpp
// 危险写法：每次用都直接 load，同一个 while 循环体内有 6 次独立的 load
while (p->offset_write.load(std::memory_order_acquire) > p->offset_read.load(memory_order_relaxed)) {
    write(1, p->data + p->offset_read.load(memory_order_relaxed),           // load #1
          p->offset_write.load(std::memory_order_acquire)                    // load #2
              - p->offset_read.load(memory_order_relaxed));                  // load #3
    p->offset_read.store(p->offset_write.load(std::memory_order_acquire),   // load #4
                         std::memory_order_release);
}
// load #1 和 load #3 的 rd 可能不同 —— 条件里判断的 wr/rd 和循环体里用的 wr/rd 没有绑定关系
// load #2 和 load #4 的 wr 可能不同 —— store 了一个比上面 write 更大的值，但那些字节没被输出
```

时间线：

```
a.out（本线程）:                               b.out（另一进程）:

while 条件:
  wr_load() = 64  ✓ 没数据就进不来
  rd_load() = 0

write(data + rd_load(),                           offset_write = 64 → write(efd)
      wr_load() - rd_load())
  → rd_load() = 0
  → wr_load() = 64                                offset_write ← 128    ← 写者在另一核前进了
  → 写了 data[0..63]，共 64 字节
       ↑ 用的是 wr=64                               
                                                   offset_write ← 192

store(wr_load())
  → wr_load() = 192                               ← 独立 load，拿到的是 192，不是 64！
  → offset_read = 192                             ← 宣称读到了 192，但只输出了 64 字节

// data[64..191] 永久丢失 //                        b.out 继续写，已无法补救
```

**根因**：`write()` 用的是一个 `wr_load()` 的值（64），`store()` 用的是另一个 `wr_load()` 的值（192）。两者之间没有绑定——它们是从共享内存独立读取的快照，写者在两次读取之间前进了。`offset_read` 被设为一个从未被输出过的位置，中间的数据永久丢失。

**这不是低概率的竞态 bug。** 只要 `write(1)`（syscall，~1-2μs）执行期间写者有任何前进，就会触发。在多核系统上这是常态，不是例外。

### 正确做法：局部变量固定

```cpp
int wr = p->offset_write.load(std::memory_order_acquire);  // wr = 64，已固定
int rd = p->offset_read.load(memory_order_relaxed);         // rd = 0，已固定
while (wr > rd) {
    write(1, p->data + rd, wr - rd);                        // 同一个 wr 和 rd
    rd = wr;
    p->offset_read.store(wr, std::memory_order_release);    // 同一个 wr
    wr = p->offset_write.load(std::memory_order_acquire);   // 明确重新加载，进入下一轮
}
```

`wr` 在一个迭代内是定值：写多少字节 → 就存多少偏移 → 三者一致。需要新值时显式 `load()`，进入下一轮。`rd` 用局部变量追踪（只有本线程写它，值已知），不需要重新 load。

### 条件判断同理

```cpp
// b.cpp 的忙碌等待——如果直接 load 而不固定
while ((p->offset_read.load(X) > p->offset_write.load(X)       // load #1, #2
        && p->offset_read.load(X) <= p->offset_write.load(X) + 64)  // load #3, #4
    || p->offset_write.load(X) + 64 >= p->offset_read.load(X) + 1664)  // load #5, #6
//  ↑ 6 次独立的 atomic load，基于不一致的 (rd, wr) 做判断
//  → 该等的时候可能没等（溢出风险）
//  → 或不该等的时候等了（无意义休眠）

// 实际代码的做法——先用局部变量固定一对值
int rd = p->offset_read.load(std::memory_order_acquire);
int wr = p->offset_write.load(memory_order_relaxed);
while ((rd > wr && rd <= wr + sizeof(temp))
    || wr + sizeof(temp) >= rd + sizeof(p->data))
{
    this_thread::sleep_for(chrono::milliseconds(1));
    rd = p->offset_read.load(std::memory_order_acquire);
    wr = p->offset_write.load(memory_order_relaxed);
}
```

## 二、while vs if 选择

### 场景

消费者被 eventfd 唤醒后，从共享内存 ring buffer 中读取数据。用局部变量固定了 `rd` 和 `wr` 之后，问题是：**读一轮就停（`if`），还是一直读直到追上写者（`while`）？**

### `if`：快照一次

```cpp
// a.cpp 预读的 if 版本（不推荐）
int wr = p->offset_write.load(std::memory_order_acquire);
int rd = p->offset_read.load(memory_order_relaxed);
if (wr > rd) {
    write(1, p->data + rd, wr - rd);
    p->offset_read.store(wr, std::memory_order_release);
}
// 结束 — 即使 wr 在 write(1) 期间又前进了
```

2 次 load，1 次 store。但 `write(1)` 是 syscall（~1-2μs），在此期间写者可以继续写入。`if` 不会再检查，漏掉的数据要靠下一次 eventfd 唤醒。

### `while`：追上为止（实际采用的写法）

```cpp
// a.cpp:88-94（预读）和 a.cpp:123-129（epoll handler）
int wr = p->offset_write.load(std::memory_order_acquire);
int rd = p->offset_read.load(memory_order_relaxed);
while (wr > rd) {
    write(1, p->data + rd, wr - rd);
    rd = wr;                                           // 局部变量追踪，不需要 load
    p->offset_read.store(wr, std::memory_order_release);
    wr = p->offset_write.load(std::memory_order_acquire);   // 只 reload wr
}
```

首次：3 次 load（比 `if` 多 1 次），1 次 store。此后每多一轮：1 次 load，1 次 store。

**关键**：`rd` 用局部变量追踪，不需要重新 load `offset_read`——因为只有本线程修改它，值已知；`wr` 在循环体末尾显式重新加载，进入下一轮前拿到最新值。

### 性能分析

| 操作 | 近似延迟 |
|------|---------|
| atomic load (x86 `mov`) | ~0.3ns |
| `write(1, stdout)` | ~1-2μs |
| `epoll_wait` + `read(efd)` | ~2-5μs |

一个额外的 atomic load 比一次多余的 epoll 往返便宜 **~10000 倍**。

`while` 多花的那个 load 成本，远小于 `if` 漏数据后需要的新一轮 epoll_wait 成本。

### 不变式

> 每次消费 eventfd 通知后，`offset_read` 必须追上消费时刻的 `offset_write`。

`while` 满足这个不变式——消费者确保"读到没有新数据为止"。`if` 不满足——`write(1)` 的耗时本身就是一个窗口，写者可以在窗口中继续前进。

### 实际发生过的时序

```
a.out（CPU 0）:                     b.out（CPU 1）:
load wr = 64                        offset_write = 64 → write(efd)
load rd = 0
                                    offset_write = 128 → write(efd)   ← a.out 的 write(1) 期间
write(1, 64 bytes) ← 慢            offset_write = 192
store offset_read = 64
// if 结束，128、192 漏掉了        offset_write = 256
                                     ...
```

用 `while`：`write(1)` 返回后 reload `wr` → 发现 128 > 64 → 继续读 → 继续追 → 直到 `wr == rd`。

### 结论

**用 `while`**。多花的 load 成本可以忽略，但它保证了消费完整性，消除了依赖下一次 epoll 唤醒的隐含假设。局部变量固定值和 `while` 循环配合，正确性可证明。

### 代码最终形态

[a.cpp:88-94](a.cpp#L88-L94)（预读）：

```cpp
int wr = p->offset_write.load(std::memory_order_acquire);
int rd = p->offset_read.load(memory_order_relaxed);
while (wr > rd) {
    write(1, p->data + rd, wr - rd);
    rd = wr;
    p->offset_read.store(wr, std::memory_order_release);
    wr = p->offset_write.load(std::memory_order_acquire);
}
```

[a.cpp:116-130](a.cpp#L116-L130)（epoll handler，含 wrap-around 处理）：

```cpp
int rd = p->offset_read.load(memory_order_relaxed);
int wr = p->offset_write.load(std::memory_order_acquire);
if (rd > wr) {
    write(1, p->data + rd, sizeof(p->data) - rd);
    p->offset_read.store(0, std::memory_order_release);
}

wr = p->offset_write.load(std::memory_order_acquire);
rd = p->offset_read.load(memory_order_relaxed);
while (wr > rd) {
    write(1, p->data + rd, wr - rd);
    rd = wr;
    p->offset_read.store(wr, std::memory_order_release);
    wr = p->offset_write.load(std::memory_order_acquire);
}
```
