# Atomic、Memory Order、以及为什么 volatile 不够

## 两个独立的问题

在多进程/多线程共享内存场景下，有两个独立的问题需要解决：

| 问题 | 根源 | 解决 |
|------|------|------|
| **寄存器缓存** | 编译器把变量缓存到寄存器，不重新从内存读 | `std::atomic`（任何 memory order） |
| **CPU 乱序** | CPU 的 store buffer 和投机执行打乱指令顺序 | `memory_order_release/acquire` |

## `std::atomic` 管寄存器缓存

编译器优化有权把非 atomic 变量的值缓存在寄存器里：

```cpp
// 非 atomic：编译器可能生成这样的代码
int writed = p->offset_write;   // 从内存加载到 rax
while (writed > read) {         // 每次检查都用 rax 中的旧值！
    write(...);
    read = writed;
    // p->offset_write 可能已经被 b.out 更新了，
    // 但编译器不会再从内存读取
}
```

`std::atomic` 的任何操作（包括 `memory_order_relaxed`）都强制从内存重新加载：

```cpp
// atomic：每次 load 都生成真正的 mov 指令
writed = p->offset_write.load(memory_order_relaxed);  // 每次执行都从内存读
```

x86 上 `memory_order_relaxed` 和 `memory_order_acquire` 编译出来是**同一条 mov 指令**，性能完全相同。区别只在编译器层面的 ordering 保证不同。

---

## 核心判据：一条问题决定用 relaxed 还是 acquire/release

**所有 atomic memory order 的选择，都可以归结为一句话：**

> 别的线程读到这个原子的值，会不会拿它当"某块**别的**内存现在**可以**碰了"的信号？

```
会 → 它就是一道闸门，守着别的数据
     写方要 release（开闸前先把那块数据发布出去）
     读方要 acquire（开闸后才保证看得到那块数据）

不会 → 这个数字本身就是全部意义
       没有任何别的内存的安全挂在它的先后上
       relaxed 够了
```

### 闸门：数字守着别的内存 → acquire/release

```
b.out（写方）:                           a.out（读方）:
  memcpy(p->data + wr, temp, 64)  // ① 先准备好数据
  p->offset_write.store(..., release)  // ② 开闸——"数据就绪了"
                                          wr = p->offset_write.load(acquire)  // ③ 等开闸
                                          write(1, p->data + rd, wr - rd)      // ④ 过了闸，数据安全
```

**`offset_write` 不是一道普通的数字。** 读端读到它变大，含义是"到这儿的数据字节写好了、能读了"——它守着 `data[]` 那块内存，是闸门。必须 `release/acquire`。

**实践提示——先找 release。** `acquire` 和 `release` 只是配对，但 `release` 有明确的动作时刻——"我把某块内存写好了，现在宣布出去"。这个时刻在代码里一眼看得见：store 之前的那行 `memcpy`、`memset`、`write` 就是被守着的数据。而 `acquire` 侧是被动的，你只需要保证"凡有 `release`，对面读这个变量时配一个 `acquire`"——不会漏，因为写方的闸门时刻反过来定义了读方必须在哪儿过闸。

### 不守门：数字本身就是全部 → relaxed

```cpp
atomic<int> total_processed{0};   // 纯统计计数器

// 写方
total_processed.fetch_add(1, memory_order_relaxed);

// 读方
int n = total_processed.load(memory_order_relaxed);
// n 大一点小一点都没关系，没有任何别的内存的正确性挂在它上面
```

这个计数器的值就是全部意义——你只想知道"处理了多少条"，不拿这个数字去决定能不能碰别的内存。`relaxed` 足够。

### 回到项目的空闲链表

```cpp
// b.cpp:163-170
int off_16 = p->head.offset_16.load(std::memory_order_acquire);  // 闸门
int next_off_16;
memcpy(&next_off_16, p->data + off_16, sizeof(int));             // 块里的 next 指针
p->head.offset_16.store(next_off_16, memory_order_relaxed);      // 只更新数字
```

- **load acquire**：读到 `off_16` → 含义是"这块空闲内存现在归我了，可以去读里面的 `next_offset` 了"——闸门，守着节点内容 → `acquire`
- **store relaxed**：`next_off_16` 的值在 `shm_init` 阶段就已经写好在 `data[]` 里了，这条 store 只是更新一个整数，不绑定任何新写入的共享内存 → `relaxed` 够

**不是因为"改不改内存"，而是因为"守不守别的内存"。** 如果反过来，你拿一个计数当闸门——"count > 0 就去摘一个节点读它的内容"——那它就在守节点内存了，`relaxed` 会让你看到 count 变了、却读到消费者还没写完的 `next_offset`，链表当场烂掉。这时它必须 `acquire/release`。

---

## `release/acquire` 管顺序（判据的实现机制）

即使每次 load/store 都到达内存，CPU 的 store buffer 也可能让不同核心看到不同的写入顺序：

```
b.out（CPU 0）:                       a.out（CPU 1）:
  p->data[0] = 'a';  // 写数据
  p->offset_write = 64;  // 写偏移    int writed = p->offset_write; // ← 可能看到 64
  write(efd, 1);         // 发信号    char c = p->data[0];          // ← 可能读到旧值！
```

CPU 0 的 store buffer 可能把 `offset_write = 64` 先刷到 cache，而 `data[0] = 'a'` 还在 store buffer 里。CPU 1 看到 `offset_write = 64` 后立即读 `data[0]`，可能读到旧数据。

**修复**：

```cpp
// b.out（写者）
p->data[writed] = 'a';                                        // 普通写
p->offset_write.store(candidate, std::memory_order_release);  // release store
write(efd, &val, sizeof(val));                                 // syscall（隐含屏障）

// a.out（读者）
read(efd, &val, sizeof(val));                                  // syscall
int writed = p->offset_write.load(memory_order_acquire);      // acquire load
char c = p->data[...];                                         // 普通读 ← 保证看到最新值
```

- **`memory_order_release`**：保证这条 store 之前的所有内存操作（`data[0] = 'a'`）对其他线程可见**之前**，`offset_write` 的新值先行可见。即：看到 `offset_write` 新值 = 数据一定已就绪。

- **`memory_order_acquire`**：保证这条 load 之后的所有内存操作（`c = p->data[...]`）看到的是 release store 之前的状态。即：读到 `offset_write` 后，读数据一定是最新的。

- **eventfd 的 `read`/`write` syscall**：内核代码中有内存屏障（spinlock、wake_up 等），在 x86 上是 full barrier，但在 C++ 抽象机层面，仍应依赖 `release/acquire` 而非隐含行为。

```
release/acquire 链条:

a.out:   ① data 写入 ──→ ② offset_read.store(release)
                              │
                              │ happens-before
                              ↓
b.out:                     ③ offset_read.load(acquire) ──→ ④ 读 data
```

**③ 看到 ②，就一定也看到 ①。** 这就是闸门的工作方式。

---

## 实例：b.cpp 忙碌等待中 rd 要 acquire，wr 可以 relaxed

### 看实际代码

```cpp
// b.cpp:104-109 忙碌等待
int rd = p->offset_read.load(std::memory_order_acquire);   // acquire — 闸门
int wr = p->offset_write.load(memory_order_relaxed);        // relaxed — 数字本身
while ((rd > wr && rd <= wr + len) || wr + len >= rd + sizeof(p->desc_ring)) {
    this_thread::sleep_for(chrono::milliseconds(1));
    rd = p->offset_read.load(std::memory_order_acquire);   // acquire
    wr = p->offset_write.load(memory_order_relaxed);        // relaxed
}
```

### `wr` 为什么 relaxed：同一线程的 as-if 规则

`offset_write` 的写者就是 `b.out` 自己——同一个线程。C++ 语言有一条基本保证：**编译器/CPU 可以乱序执行，但单线程的"可观察行为"必须和顺序执行一致。** 这叫 as-if 规则。

```
b.out（同一线程）:
  p->offset_write.store(64, release);   // 我写的
  ...
  wr = p->offset_write.load(relaxed);   // 我读
  // → 必然读到 ≥ 64，因为"在同一个线程里，自己刚写的值自己还没看到"违反 as-if 规则
```

编译器即使做了各种优化和重排，也不能让同一个线程在 `store(64)` 之后读到比 64 更旧的值。这就是 `relaxed` 在这里够用的原因——**as-if 规则本身不会因 memory order 而改变**，它是对编译器的最低要求。

### `rd` 为什么必须 acquire：闸门守着数据

`offset_read` 的写者是 `a.out`，**这是另一个线程（另一个进程）**。C++ 的 as-if 规则只对单个线程成立，跨线程没有这种保证。

```
a.out（线程 A）:                         b.out（线程 B）:
  write(1, p->data + rd, ...)   // ① 消费数据，释放缓冲区空间
  p->offset_read.store(..., release)  // ② 开闸——"空间释放了"

                                        rd = p->offset_read.load(acquire)  // ③ 过闸
                                        if (空间够) {
                                            memcpy(p->data + wr, ...)  // ④ 可以安全写了
                                        }
```

**如果没有 acquire**，CPU 的 store buffer 可能让 ② 先对其他核心可见，而 ① 还在 store buffer 里排队。结果：`b.out` 看到 `rd` 前进了，以为数据已经被消费了，但 `p->data[]` 里的内容在 `b.out` 看来还是旧的——闸门开了，但门后的东西还没准备好。

**`acquire` 保证**：③ 看到了 ② 的这个值，就一定也看到了 ① 及之前的所有内存操作。`rd` 前进了 → 对应的缓冲区空间确实已经释放了。

### 总结

| 变量 | 读方 | 闸门？ | memory order |
|------|------|--------|-------------|
| `offset_write` | b.out 自己 | ❌ 同线程 | `relaxed` |
| `offset_read` | b.out 读 a.out 的 | ✅ 守着 `data[]` 空间 | `acquire` |
| `offset_16` load | b.out 读 a.out 的 | ✅ 守着节点内容 | `acquire` |
| `offset_16` store | b.out 自己写 | ❌ 只更新数字 | `relaxed` |

---

## 为什么 `volatile` 不够

`volatile` 在 C++ 中只做一件事：**禁止编译器优化掉对该变量的读写**。它不提供：

| | volatile | std::atomic |
|---|---|---|
| 禁止编译器缓存 | ✅ | ✅ |
| 原子性（read-modify-write） | ❌ | ✅ |
| CPU 乱序保护 | ❌ | ✅（release/acquire） |
| happens-before 关系 | ❌ | ✅ |
| 多线程合法（UB-free） | ❌（data race 仍是 UB） | ✅ |

```cpp
volatile int offset_write;  // 编译器每次都读，但 CPU 可能看到乱序
                            // 在 C++ 标准中，并发写 volatile 仍是 undefined behavior

atomic<int> offset_write;   // 编译器每次都读 + CPU 顺序保证 + 无 UB
```

**一句话**：`volatile` 是为硬件寄存器设计的（memory-mapped I/O），不是为多线程设计的。

---

## 本项目的选择

- `offset_read`、`offset_write`、`offset_16`（load 方向）：`atomic<int>`
- 写者跨线程 store：`memory_order_release`
- 读者跨线程 load：`memory_order_acquire`
- 同线程 load / 纯数字 store：`memory_order_relaxed`
- x86 上所有 memory order 编译为同一条 mov，零运行时开销差异

---

## 一句话拎走

> **`relaxed` 用在"这个数字本身就是全部"；`acquire/release` 用在"读到这个值，等于另一块内存已经就绪"。**

以后每个原子变量都拿这句套，不用再猜。
