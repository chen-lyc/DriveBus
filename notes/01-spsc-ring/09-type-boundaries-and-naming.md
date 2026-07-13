# 共享内存 SPSC：变量类型与命名的边界

> 复习目标：面对一个整数变量，先判断它跨越或守护的是哪一种边界，再从名称读出它描述的对象、角色和单位。
>
> 范围：本文解释当前 SPSC 中 `uint32_t`、`size_t`、`int`、`ssize_t` 及接口类型如何协作；不做完整跨 ABI 二进制协议设计，也不逐项裁决所有配置常量。

## 核心模型：类型表达边界，名称表达角色

这里的“边界”不是代码文件的边界，而是一个值需要被谁、以什么规则解释：共享内存的另一进程、当前进程中的对象与数组，还是 POSIX 接口。先分边界，再谈有无符号和位宽。

| 值遵守的契约 | 当前项目中的例子 | 当前类型 | 类型在保护什么 |
| --- | --- | --- | --- |
| 共享布局中的字段或坐标 | `MessageDescriptor::{offset, len}`、ring cursor、free-list offset | `uint32_t` | 写端和读端以同一固定宽度解释该字段。 |
| 本地对象的字节数、元素数量或下标 | `descriptor_count`、`descriptor_index`、`block_index` | `size_t` | 能表示当前实现中对象、数组和标准库/系统接口所使用的本地范围。 |
| 分类结果还要表达“未找到” | `find_size_class()` 的返回值、`size_class` | `int` | `0..kClassCount - 1` 是有效 class，`-1` 是失败状态。 |
| POSIX 调用的结果或参数 | `recvmsg` / `read` 的结果，`fd`，eventfd 计数值 | `ssize_t` / `int` / `uint64_t` | 遵从接口已经定义好的成功、失败和数据格式。 |

因此，非负只是值域事实，不是选 `uint32_t` 的充分理由；`count` 只是角色词，也不是选无符号类型的充分理由。同一条处理路径中出现不同类型，只有在值进入了不同契约时才是合理的。

## 为什么本地对象范围使用 `size_t`

【来源事实】C++ 将 `std::size_t` 定义为“足以容纳任意对象以字节计大小”的实现定义无符号整数类型；`sizeof` 的结果就是 `std::size_t`。它会随实现和 ABI 改变，这不是缺陷：它描述的是**本机可表示的对象范围**，而不是一份要由另一台机器稳定读取的协议格式。

这也是 `memcpy`、`memset`、分配接口等把长度参数设计为 `size_t` 的原因：这些接口面对的是当前实现中的对象与内存范围，必须能接收任意合法对象的大小。泛型容器通常公开相同角色的 `size_type`；常见实现中它往往是 `size_t`，但泛型接口不需要承诺两者字面上永远相同。

`int`、`uint32_t` 当然可以在调用前转换为 `size_t`，但这不能替代 `size_t` 的语义保证：它们没有“可表示任意本机对象大小”的语言契约。`uint64_t` 也不是本地范围的通用替代品；它适合协议明确规定 64 位字段的情形，却不能替代标准库、分配器和系统接口所表达的本机对象范围。

这并不推出“任何叫长度的变量都声明为 `size_t`”。`MessageDescriptor::len` 首先是共享描述符的 32 位字段，直接镜像它的 `message_size_bytes` 保持 `uint32_t` 更能说明来源。它传给 `write` 或 `memset` 时，才作为一次**本地字节计数**进入接口；转换应留在这个真实边界，而不是倒灌回共享结构。

`b.cpp:187` 的 `message_size_bytes - 2 * sizeof(int)` 正好说明这点：`sizeof(int)` 的类型为 `size_t`，表达式需要按本地对象范围参加运算；这不改变 `message_size_bytes` 作为描述符字段镜像的声明类型。`memset` 接受 `size_t` 是接口事实，不是给 `MessageDescriptor::len` 选类型的依据。

## 当前 SPSC：三条不能强行合并的路线

### 1. 消息长度保持描述符身份

```text
b.cpp:get_msg_size()                  uint32_t
        ↓
b.cpp:message_size_bytes              uint32_t
        ↓ 写入 MessageDescriptor{head_off, message_size_bytes}
SharedData::desc_ring[].len           uint32_t
        ↓
a.cpp:message_size_bytes              uint32_t
        ├── find_size_class(...) ───→ int size_class
        └── write / memset 参数 ────→ 作为本地字节计数传入 size_t 形参
```

`get_msg_size()`（[b.cpp](../../b.cpp):19–36）的结果在当前设计中会原样写入 `MessageDescriptor::len`（`b.cpp:180`；[共享布局](../../include/shared_memory_layout.hpp):41–44），所以这个局部值有意保持与协议字段同宽。它不推出“所有本地长度都应为 `uint32_t`”；消费者在 `a.cpp:52` 把它读回 `uint32_t message_size_bytes`，仍是在读取同一个字段的本地镜像。

随后才分成两种使用：`a.cpp:54` / `b.cpp:134` 把长度交给分类函数，得到可失败的 `int size_class`；`a.cpp:72–74` / `b.cpp:186–188` 把它用于本地 I/O 或内存操作。后者只要求传入的参数能转换为接口的 `size_t`，不要求把前面的共享字段改写为 `size_t`。

### 2. ring 游标先证明区间，再成为本地批量

```text
共享 cursor rd / wr                  uint32_t
        ↓ 仅在 wr > rd 的连续区间
wr - rd                              非负的描述符数量
        ↓ static_cast<size_t>
descriptor_count / descriptor_index size_t
```

`rd`、`wr` 是共享 ring 的坐标，因此保持 `uint32_t`。在 `a.cpp:144–153` 和 `a.cpp:189–198`，只有 `wr > rd` 才计算 `wr - rd`，并把它作为本地临时批次的 `descriptor_count` 和循环上界。绕回时，`a.cpp:177–185` 先单独处理尾部连续段 `kDescriptorSlotCount - rd`，不能把跨绕回的游标直接相减后假装它仍是本地数量。

这里的 `static_cast<size_t>` 只标出“现在开始把它用作本地元素数量”；它不证明 ring 算术、范围或同步本身正确。前面的分支条件才是这次转换成立的前提。

### 3. byte offset 派生出本地下标

`MessageDescriptor::offset` 与 free-list 的 `head_off` / `tail_off` 表示相对于 payload 区的共享 byte offset，所以是 `uint32_t`。在 `a.cpp:87` 和 `b.cpp:163`，它除以对应 class 的 block 大小后，所得值的角色变为 `is_in_use` 中的本地数组下标，于是使用 `size_t block_index`。

这不是“把 offset 统一成了另一种类型”：`offset` 仍是坐标，`block_index` 是从它推导出的另一个量。把两者命名区分开，正是为了防止把 byte offset 当成元素下标。

## 为什么 `int` 和 `ssize_t` 不该为了统一而消失

`find_size_class()`（[辅助函数](../../include/shared_memory_layout_helpers.hpp):3–9）必须返回有效 class 或“没有可容纳的 class”。当前接口用 `-1` 表示第二种状态，因此返回 `int`，调用者也先检查 `size_class < 0`。这里有符号性来自**状态编码**，不是来自“class count 可能为负”。

`ssize_t` 则来自 POSIX，而不是标准 C++。POSIX 规定它是有符号整数类型，至少能表示 `[-1, SSIZE_MAX]`，用于“字节计数或错误指示”；它不是规范意义上的“有符号 `size_t`”，也不保证能覆盖全部 `size_t`。因此 `recvmsg` 的 `ret`（`a.cpp:36`）和 `read` 的 `n`（`a.cpp:140`）应保留接口声明的 `ssize_t`：成功时是实际字节数，错误时可为 `-1`。

同理，文件描述符、`connect` / `bind` / `epoll_wait` 的状态结果使用 `int`，因为 POSIX 就这样定义；`b.cpp` / `a.cpp` 的 `uint64_t val` 对应 Linux eventfd 的 64 位计数值。接口类型不是“内部类型没有统一好”，而是外部合同的一部分。

## 配置常量也要看它服务的域

`kClassCount` 和 `kDescriptorSlotCount` 的名字都含“数量”，但当前代码不应只因这一点让它们同型：

- `kClassCount` 是 `int`，与 `int size_class`、`-1` 哨兵以及 `int i` 的 class 分类域配合。若要重做这套分类表示，必须连同返回约定、比较和数组访问一起审视，不能只改常量。
- `kDescriptorSlotCount` 是 `uint32_t`，当前让它与共享 ring cursor 的固定宽度算术处在同一域。它是容量常量，不是 `SharedData` 内的字段；这里的理由是参与的表示域，而不是“所有容量常量都必须是 `uint32_t`”。

【业界惯例】`count` 通常表示一个集合中元素的基数，`number` 更容易表示编号或数值本身，`n` 则隐藏对象和单位。因此 `descriptor_count` 比 `descriptor_num` 或 `n` 更容易复原含义；但命名只说明角色，仍不决定它必须是 `int`、`uint32_t` 或 `size_t`。

## 命名让类型判断不必依赖上下文

一个好名称至少让读者看见“什么东西”“它扮演什么角色”；涉及可混淆单位时再写出单位。

| 命名形态 | 应表达的意思 | 当前例子 |
| --- | --- | --- |
| `*_bytes` | 以字节计的大小或长度 | `message_size_bytes`、`kClassSizeBytes` |
| `*_count` | 某个集合中的元素数量 | `descriptor_count`、`kBlockCountBySizeClass` |
| `*_index` | 某个集合内的位置 | `descriptor_index`、`block_index` |
| `*_offset` / 明确约定的 `offset` | 相对某个已知基址的位移；本项目是 byte offset | `MessageDescriptor::offset` |
| `*_fd` 或既有 POSIX 缩写 `*fd` | POSIX 文件描述符 | `listen_fd`、`epollfd` |

`message_size_bytes` 是一个完整示范：对象是 message，测量的是 size，单位是 bytes。相反，`offset` 本身还需要由结构的注释或上下文明确“相对 `data` 的 byte offset”，否则读者仍可能把它误作数组 index。

## 外部源码对照：观察写法，不把它当规则出处

| 项目 | 源码中可直接观察到的写法 | 对本笔记的启发 |
| --- | --- | --- |
| Fast DDS | `SerializedPayload_t` 同时有 `uint32_t length`、`max_size`、`pos`，以及 `size_t representation_header_size`。 | 同一结构中可以同时存在固定宽度字段和本地大小常量；必须看每个成员的合同。 |
| Boost.Lockfree `spsc_queue` | 进程内泛型队列用 `std::size_t` 表示容量、read/write index 与批量数量。 | 本地容器的元素范围可以自然以 `size_t` 为中心，而不承担共享布局字段的限制。 |

这些项目只能证明“它们在各自约束下这样实现”。它们不是把某种类型变成普适规则的证据；本项目仍需从自己的共享布局、对象范围和接口合同出发。

## 适用边界

固定宽度只固定标量字段的宽度，不能自动让整个 `SharedData` 成为可跨任意机器、编译器或 ABI 直接共享的二进制格式。该结构还包含 `std::atomic`、`pthread_mutex_t`、数组和可能的填充；进程间使用仍依赖双方的 ABI、构建配置和初始化协议。

本文也不决定当前临时快照应采用哪种存储形式，更不把一次显式转换当成范围、下标或并发正确性的证明。这些是另外的实现与协议问题。

## 30 秒回顾

1. `MessageDescriptor::len` 为什么是 `uint32_t`，而它传给 `write` / `memset` 时又可以进入 `size_t` 参数？
2. `rd`、`wr` 在什么前提下才能导出 `size_t descriptor_count`？绕回为什么要单独处理？
3. `find_size_class()` 为什么需要 `int`，`read()` 为什么需要 `ssize_t`？
4. `count`、`index`、`bytes` 在名称中各自说明什么，又为什么都不能单独决定类型？

## 来源

- 当前实现：[共享布局](../../include/shared_memory_layout.hpp)、[分类辅助函数](../../include/shared_memory_layout_helpers.hpp)、[生产者](../../b.cpp)、[消费者](../../a.cpp)。
- C++ 标准草案：[`std::size_t`](https://eel.is/c++draft/support.types.layout)、[`sizeof`](https://eel.is/c++draft/expr.sizeof)、[`<cstring>` 的长度形参](https://eel.is/c++draft/cstring.syn)。
- POSIX：[`ssize_t` 定义](https://pubs.opengroup.org/onlinepubs/9799919799.2024edition/basedefs/sys_types.h.html)、[`read`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/read.html)、[`recvmsg`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/recvmsg.html)；Linux：[eventfd](https://man7.org/linux/man-pages/man2/eventfd.2.html)。
- GitHub 源码对照：[Fast DDS `SerializedPayload_t`](https://github.com/eProsima/Fast-DDS/blob/master/include/fastdds/rtps/common/SerializedPayload.hpp)、[Boost.Lockfree `spsc_queue`](https://github.com/boostorg/lockfree/blob/develop/include/boost/lockfree/spsc_queue.hpp)。
