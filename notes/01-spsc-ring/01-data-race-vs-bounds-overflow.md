# Data Race vs 越界：两条独立的线

## 起点

这个项目最初有两个完全不同的 bug，同时存在于代码中。把它们分开理解很重要。

## 越界（Bounds Overflow）

**单线程问题**，和并发无关。

```cpp
// a.cpp / b.cpp 原始代码
mmap(nullptr, 7, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
//              ↑ 只映射了 7 字节

SharedData *p = static_cast<SharedData *>(...);
// sizeof(SharedData) = 4 + 4 + 1024 = 1032 字节

p->offset_write;  // 访问第 5-8 字节 → 没问题（在 7 字节内）
p->data[0];       // 访问第 9 字节     → 段错误！超出了 7 字节
```

**现象**：`段错误 (核心已转储)`——OS 只告诉你"访问了不该访问的内存"，不告诉你为什么。

**诊断工具**：
| 工具 | 用法 | 输出 |
|------|------|------|
| GDB | `gdb ./a.out` → `run` → `bt` | 精确到行号的调用栈 |
| AddressSanitizer | `g++ -fsanitize=address -g` | heap-buffer-overflow at line X |
| Valgrind | `valgrind ./a.out` | invalid read/write of size N |

**修复**：`mmap(nullptr, sizeof(SharedData), ...)`。

**本质**：指针指向了未映射的虚拟地址。CPU 的 MMU 查页表发现缺页 → 内核判断该地址不属于任何 VMA → SIGSEGV。

## Data Race

**多线程/多进程问题**，两个执行流并发访问同一块内存，至少一个是写，没有 happens-before 关系。

```cpp
// b.out（写者）:
p->offset_write.store(candidate, std::memory_order_release);  // 写
write(efd, &val, sizeof(val));                                  // 信号

// a.out（读者）:
read(efd, &val, sizeof(val));                                   // 等信号
int writed = p->offset_write.load(memory_order_acquire);       // 读
```

如果没有 `atomic` + `release/acquire`，读者的 load 可能看不到写者的 store（编译器缓存到寄存器），或者看到乱序的结果（CPU store buffer）。

**现象**：数据"莫名其妙"不对，且每次运行结果不同（比段错误更难排查）。

**诊断**：ThreadSanitizer（`g++ -fsanitize=thread -g`）。

**修复**：`std::atomic` + 正确的 memory order。

## 关键区别

| | 越界 | Data Race |
|---|---|---|
| 调试信号 | 段错误（崩溃） | 数据错乱（不一定崩溃） |
| 原因 | 指针/大小算错 | 同步缺失 |
| 单线程能复现？ | 能 | 不能 |
| 工具 | ASan / GDB / Valgrind | TSan |
| 修法 | 改 size 参数 | atomic + memory order |

## 为什么两条线要分开看

修复越界不会消除 data race，反之亦然。这个项目最初两个问题同时存在：
1. `mmap(7)` → 偶尔崩溃（取决于访问是否越界）
2. 没有 atomic → 偶尔读到过期数据（取决于 CPU 调度和缓存状态）

两个问题都修掉才是安全的。
