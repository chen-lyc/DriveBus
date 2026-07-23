# 共享内存中的 pthread_mutex_t

> 适用场景：共享内存内嵌入锁，跨进程互斥访问同一块数据。

## 一、API 使用流程

```
┌─ 准备工作 ──────────────────────────────────────────────┐
│ 1. 在 SharedData 里嵌入 pthread_mutex_t                  │
│ 2. 在 mmap 之后原地初始化                                   │
└─────────────────────────────────────────────────────────┘

┌─ 初始化 ────────────────────────────────────────────────┐
│ pthread_mutexattr_t attr;                                  │
│ pthread_mutexattr_init(&attr);                             │
│ pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);│
│ pthread_mutex_init(&shm.mutex, &attr);                     │
│ pthread_mutexattr_destroy(&attr);                          │
└─────────────────────────────────────────────────────────┘

┌─ 使用 ──────────────────────────────────────────────────┐
│ pthread_mutex_lock(&shm.mutex);                           │
│ // 临界区 — 读写共享数据                                    │
│ pthread_mutex_unlock(&shm.mutex);                         │
└─────────────────────────────────────────────────────────┘

┌─ 销毁 ──────────────────────────────────────────────────┐
│ pthread_mutex_destroy(&shm.mutex);  // 最后一个进程退出前      │
└─────────────────────────────────────────────────────────┘
```

### 本项目代码

```cpp
// SharedData 中嵌入
struct Outstanding {
    pthread_mutex_t mutex;
    bool chunk_16[16];
};

struct SharedData {
    // ...
    Outstanding outstanding;  // ← mutex 在这里，属于 mmap 映射的内存
};

// 初始化（b.cpp outstanding_init）
void outstanding_init(Outstanding &outstanding) {
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);  // ★ 关键
    pthread_mutex_init(&outstanding.mutex, &attr);
    pthread_mutexattr_destroy(&attr);

    memset(outstanding.chunk_16, 0, sizeof(outstanding.chunk_16));
}

// 使用（b.cpp main）
pthread_mutex_lock(&p->outstanding.mutex);
p->outstanding.chunk_16[wr] = true;
pthread_mutex_unlock(&p->outstanding.mutex);
```

---

## 二、API 说明

按流程顺序逐个解释。

### 2.1 `pthread_mutexattr_init` — 创建属性对象

```cpp
int pthread_mutexattr_init(pthread_mutexattr_t *attr);
```

在栈上声明一个空的属性对象，初始化为默认值。所有默认属性都等同于传 `nullptr` 给 `pthread_mutex_init`——包括 `PTHREAD_PROCESS_PRIVATE`（仅进程内可用）。

调用后 `attr` 是可用的属性对象，必须在之后用 `pthread_mutexattr_destroy` 释放其内部资源。

### 2.2 `pthread_mutexattr_setpshared` — 设为跨进程共享

```cpp
int pthread_mutexattr_setpshared(pthread_mutexattr_t *attr, int pshared);
// pshared = PTHREAD_PROCESS_SHARED  或  PTHREAD_PROCESS_PRIVATE
```

把属性对象中的共享标志设为跨进程。这一个调用是**共享内存中使用 mutex 的核心**——不设它，mutex 无法跨进程正常工作。

底层区别：`PTHREAD_PROCESS_PRIVATE` 时，实现可以用用户态自旋锁等技巧；`PTHREAD_PROCESS_SHARED` 强制走内核 `futex`，保证跨进程可见。

### 2.3 `pthread_mutex_init` — 原地构造 mutex

```cpp
int pthread_mutex_init(pthread_mutex_t *mutex, const pthread_mutexattr_t *attr);
```

**不分配内存。** 在传入的地址 `mutex` 上原地构造一个 mutex 对象——格式化这块内存（写入初始状态、类型标记等），使其能被 `lock`/`unlock` 使用。构造完成后，这块内存对于所有映射了同一块共享内存的进程来说就是一个有效的锁。

传入 `nullptr` 作为 attr 等同于默认属性（进程内、默认类型）。跨进程必须传入带 `PTHREAD_PROCESS_SHARED` 的 attr。

### 2.4 `pthread_mutexattr_destroy` — 销毁属性对象

```cpp
int pthread_mutexattr_destroy(pthread_mutexattr_t *attr);
```

属性对象在 `init` 时内部分配了少量资源（Linux/glibc 上通常在用户态，极轻量）。`destroy` 负责释放这些资源。调用后 `attr` 回到未初始化状态。

**跟 mutex 本身无关**——销毁属性对象不会影响已经用该属性初始化的 mutex。mutex 已经把属性"吸收"进了自己的内部状态，之后属性对象就没用了。所以常见写法是 init mutex 之后立即 destroy attr。

### 2.5 `pthread_mutex_lock` — 加锁

```cpp
int pthread_mutex_lock(pthread_mutex_t *mutex);
```

如果 mutex 当前无人持有：立即获取，继续执行。如果 mutex 被其他线程/进程持有：**阻塞**，直到持有者调用 `unlock`。底层通过 `futex(FUTEX_WAIT)` 进入内核等待队列，由内核在被唤醒后重新调度。

跨进程时：持有者可能在另一个进程中 `unlock`，内核负责唤醒当前进程。

### 2.6 `pthread_mutex_unlock` — 解锁

```cpp
int pthread_mutex_unlock(pthread_mutex_t *mutex);
```

释放 mutex。如果有其他线程/进程在 `lock` 上阻塞，内核会唤醒其中一个（公平性取决于 mutex 类型和实现）。底层通过 `futex(FUTEX_WAKE)` 实现。

必须由持有者调用，对未持有或未初始化的 mutex 调用 unlock 行为未定义。

### 2.7 `pthread_mutex_destroy` — 销毁 mutex

```cpp
int pthread_mutex_destroy(pthread_mutex_t *mutex);
```

释放 mutex 持有的所有内核资源。对跨进程 mutex 尤其重要：

- mutex 内部可能持有内核 `futex` 对象——它在共享内存之外，存在于内核空间，不会随 `munmap` 自动清理
- 不 `destroy` 直接 `munmap`：在 Linux 上内核最终会回收（进程退出时清掉 futex），但 POSIX 不保证，且如果共享内存由 `shm_open` 创建、多个进程反复 attach/detach，残留的 futex 状态可能导致下次 `pthread_mutex_init` 遇到未定义行为
- 同一个 mutex 内存不能重复 `init` 而不先 `destroy`——这是 UB

调用前必须确保：没有线程/进程持有锁，没有线程/进程在等待锁。调用后 mutex 回到未初始化状态，不可再使用。

---

## 三、常见问题

### 3.1 普通 mutex 为什么不能跨进程

`pthread_mutex_t` 的默认类型（`PTHREAD_MUTEX_DEFAULT`）在 Linux/NPTL 上可能碰巧能跨进程工作（因为底层也是 futex）。但这**不保证**：

- POSIX 标准不要求默认 mutex 跨进程可用
- 不同实现（glibc/musl）行为不同
- 某些优化（如用户态自旋）在跨进程时失效或出错

**必须显式设 `PTHREAD_PROCESS_SHARED`**，这是唯一保证可移植性的做法。

### 3.2 mutex 必须放在 mmap 映射的区域内

```
✅ 路径：SharedData 在 mmap 区域内 → 所有进程映射同一物理内存
❌ 路径：栈/堆上 new → 仅当前进程可见 → 其他进程访问到的是自己地址空间的垃圾
```

验证方法：两个进程分别打印 `&p->outstanding.mutex` 的**物理地址偏移**（相对于 `p`），它们应该一致。

### 3.3 lock 和 unlock 可以在不同进程中

POSIX 标准要求：对 `PTHREAD_PROCESS_SHARED` 的 mutex，lock 和 unlock 可以在不同进程/线程中调用。但**destroy 必须由最后一个使用者调用**，且调用前确保没有其他进程在等锁。

### 3.4 什么时候用锁，什么时候用无锁原子变量

| | pthread_mutex | atomic |
|---|---|---|
| 保护范围 | 任意大小的临界区 | 单个变量或小结构体 |
| 阻塞行为 | 阻塞等待 | 自旋或非阻塞 |
| 内核参与 | 有（futex） | 通常无（纯用户态） |
| 跨进程 | 需要 PTHREAD_PROCESS_SHARED | 天然跨进程（在共享内存里） |
| 适合场景 | 复杂状态更新、多变量一致性 | 单个计数器、指针、flag |

本项目中 `offset_read/offset_write` 用 atomic（只需要单变量级别的 happens-before），`chunk_16[]` 数组用 mutex（需要保护一整段临界区的原子性），两者配合使用。

### 3.5 持有锁的进程崩溃了怎么办 — `PTHREAD_MUTEX_ROBUST`

跨进程 mutex 的一个致命场景：进程 A `lock` 之后、`unlock` 之前崩溃（或被 `kill -9`）。此时 mutex 被永久锁死——进程 B 再 `lock` 会**永远阻塞**。

`PTHREAD_MUTEX_ROBUST` 就是解决这个问题的。

#### 设置方式

在 mutex 属性中同时设两个 flag：

```cpp
pthread_mutexattr_t attr;
pthread_mutexattr_init(&attr);
pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);  // ★
pthread_mutex_init(&mutex, &attr);
pthread_mutexattr_destroy(&attr);
```

#### 行为

正常时 `lock` 返回 0，和普通 mutex 一样。如果**上一个持有者死了**（进程终止时未 unlock），下一个 `lock` 不会阻塞，而是返回 `EOWNERDEAD`：

```cpp
int ret = pthread_mutex_lock(&mutex);
if (ret == EOWNERDEAD) {
    // 上一个持有者崩溃了——mutex 被标记为"脏"
    // 此时你已持有锁，但被保护的数据可能处于不一致状态
    // 必须执行恢复：清理/重建共享数据到一致状态
    // 然后调用:
    pthread_mutex_consistent(&mutex);  // 声明"数据已恢复，锁恢复健康"
}
```

`pthread_mutex_consistent` 把 mutex 从不一致状态标记回一致状态。调用后 mutex 恢复正常。如果不调用，下一次 `unlock` 后 mutex 仍然是不一致状态——下一个人 `lock` 会再次收到 `EOWNERDEAD`。

#### 恢复流程

```
lock 返回 0       → 正常，直接用
lock 返回 EOWNERDEAD → 上一个持有者死了
  → 检查被保护的数据（可能部分更新、不一致）
  → 恢复到一致状态（重置、重算、或从其他数据重建）
  → pthread_mutex_consistent(&mutex)
  → 正常使用
  → unlock
```

#### 不设 ROBUST 会怎样

进程 B 永久阻塞在 `lock`。对于共享内存场景，这意味着整个系统卡死——其他进程无法恢复，只能 kill 全部进程、unlink 共享内存、重新初始化。

#### 与 atomic 的区别

`atomic` 天然不受进程死亡影响——没有"持有"概念，store 之后进程死了，load 的人读到最后一个值。mutex 则不同：持有者死了，锁还在它"手上"。`ROBUST` 让内核替你检测持有者死亡，把控制权交回来。

#### 本项目中的适用场景

`ChunkUsageTracker::mutex` 保护 `is_in_use[][]` 数组。如果持有锁的读端被 `kill -9`：

- **不设 ROBUST**：写端下次 `lock` 时永久阻塞 → 整个系统卡死
- **设 ROBUST**：写端收到 `EOWNERDEAD` → 遍历 `is_in_use` 清理该读端的标记 → `pthread_mutex_consistent` → 继续

同时配合现有的死亡检测和引用位清理（`fetch_and`），形成完整的崩溃恢复链路。

---

## 四、结构体指针 vs 实例

### `SharedData *p` — 指向共享内存

```cpp
SharedData *p = static_cast<SharedData *>(
    mmap(nullptr, sizeof(SharedData), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));
```

`p` 指向的是一块所有进程公用的物理内存。对 `p->xxx` 的修改，其他进程立即可见（受内存序约束）。

### `SharedData shm{}` — 栈上实例

```cpp
SharedData shm{};
ftruncate(fd, sizeof(shm));
```

`shm` 是当前进程栈上的局部变量。`sizeof(shm)` 用来告诉内核共享内存对象的大小——这里的 `shm` 只是模板，**不是**用于 `mmap` 映射的那个对象。实际数据在 `p` 指向的共享内存里。

### 为什么初始化在指针上做

```cpp
// ✅ 正确
outstanding_init(p->outstanding);  // 初始化的是共享内存里的 mutex

// ❌ 错误
SharedData local{};
outstanding_init(local.outstanding);  // 初始化的是栈上的 mutex，跟共享内存无关
```

`pthread_mutex_init` 在**传入的地址**上构造锁。如果传的是栈变量的地址，那么锁就只在当前进程的栈上，其他进程访问共享内存里对应位置的 mutex 时看到的是未初始化的字节。

---

> **一句口诀**：`PTHREAD_PROCESS_SHARED` 是让锁跨进程可见的唯一保证；mutex 必须在 `mmap` 区域内原地构造；`destroy` 是释放内核 futex，不能省略。
