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

## `release/acquire` 管顺序

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

## 为什么 b.cpp 的忙碌等待中 rd 要 acquire，wr 可以 relaxed

### 看实际代码

```cpp
// b.cpp:104-109 忙碌等待
int rd = p->offset_read.load(std::memory_order_acquire);   // acquire
int wr = p->offset_write.load(memory_order_relaxed);        // relaxed
while ((rd > wr && rd <= wr + len) || wr + len >= rd + sizeof(p->desc_ring)) {
    this_thread::sleep_for(chrono::milliseconds(1));
    rd = p->offset_read.load(std::memory_order_acquire);   // acquire
    wr = p->offset_write.load(memory_order_relaxed);        // relaxed
}
```

核心问题不是"谁写了这个变量"，也不是"同一进程还是不同进程"。而是 **同一线程内部的编译器保证重排之后的运行效果和代码逻辑一致 vs 跨线程不存在这种保证**。

### `wr` 为什么可以 relaxed：同一线程的 as-if 规则

`offset_write` 的写者就是 `b.out` 自己——同一个线程。C++ 语言有一条基本保证：**编译器/CPU 可以乱序执行，但单线程的"可观察行为"必须和顺序执行一致。** 这叫 as-if 规则。

```
b.out（同一线程）:
  p->offset_write.store(64, release);   // 我写的
  ...
  wr = p->offset_write.load(relaxed);   // 我读
  // → 必然读到 ≥ 64，因为"在同一个线程里，自己刚写的值自己还没看到"违反 as-if 规则
```

编译器即使做了各种优化和重排，也不能让同一个线程在 `store(64)` 之后读到比 64 更旧的值。这就是 `relaxed` 在这里够用的原因——**as-if 规则本身不会因 memory order 而改变**，它是对编译器的最低要求。

### `rd` 为什么必须 acquire：跨线程没有 as-if 规则

`offset_read` 的写者是 `a.out`，**这是另一个线程（另一个进程）**。C++ 的 as-if 规则只对单个线程成立，跨线程没有这种保证。

```
a.out（线程 A）:                         b.out（线程 B）:
  write(1, p->data + rd, ...)   // ① 消费数据，释放缓冲区空间
  p->offset_read.store(..., release)  // ② 公布新位置

                                        rd = p->offset_read.load(acquire)  // ③
                                        if (空间够) {
                                            memcpy(p->data + wr, ...)  // ④
                                        }
```

**如果没有 acquire**，CPU 的 store buffer 可能让 ② 先对其他核心可见，而 ① 还在 store buffer 里排队。结果：`b.out` 看到 `rd` 前进了，以为数据已经被消费了，但 `p->data[]` 里的内容在 `b.out` 看来还是旧的——③ 看到了 ②，却**没有**看到 ①。

**`acquire` 保证**：③ 看到了 ② 的这个值，就一定也看到了 ① 及之前的所有内存操作。即 `rd` 前进了 → 对应的缓冲区空间确实已经释放了。

```
release/acquire 链条:

a.out:   ① data 写入 ──→ ② offset_read.store(release)
                              │
                              │ happens-before
                              ↓
b.out:                     ③ offset_read.load(acquire) ──→ ④ 读 data
```

### 规律

不是"同一进程用啥、不同进程用啥"。真正的判断标准：

| 情况 | memory order |
|------|-------------|
| 读的是**别的线程**写的值 + 需要看见写者的其他内存操作 | `acquire` |
| 读的是**自己线程**写的值 | `relaxed` 够用（as-if 规则兜底） |

在 b.cpp 的忙碌等待中：
- `rd`：a.out 写的，跨线程 → `acquire`，确保看到 a.out 写 `offset_read` 之前的所有内存操作（释放的缓冲区空间）
- `wr`：自己写的，同线程 → `relaxed`，as-if 规则保证自己写的一定能看到

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

## 本项目的选择

- `offset_read`、`offset_write`：`atomic<int>`
- 写者 store：`memory_order_release`
- 读者 load：`memory_order_acquire`
- 忙碌等待中的 load：`memory_order_relaxed`（只检查条件，不依赖数据可见性）
- x86 上所有 memory order 编译为同一条 mov，零运行时开销差异
