# DriveBus

> 一个 Linux / C++ 跨进程发布订阅实现：`broker.out` 按 topic 建立发布者与订阅者的连接关系，`b.out` 创建并写入共享内存，`a.out` 接收通知后从该共享内存读取。当前构建目标只有这三个程序。 [源码](Makefile#L16-L35) [broker](broker.cpp#L235-L299) [发布者](b.cpp#L302-L348) [订阅者](a.cpp#L176-L223)

## 它把什么放在哪条路径

控制面负责注册、slot 与文件描述符交接：publisher 把 topic 和共享内存 FD 交给 broker；subscriber 把 topic 列表交给 broker；broker 创建 `eventfd`，将共享内存 FD 发给 subscriber，并将 `eventfd` 和 slot 发给 publisher 与 subscriber。 [发布注册](b.cpp#L343-L348) [订阅注册](a.cpp#L211-L220) [broker 分配与交接](broker.cpp#L352-L435) [FD 传递](src/fd_helpers.cpp#L20-L68)

数据面在 `SharedData` 中：它保存 descriptor ring、最多 16 个 slot 的读下标、分档 free list、每个 chunk 的引用位集合和 `data` 区。publisher 写 descriptor/data；subscriber 映射同一对象后读取它。 [共享布局](include/shared_memory_layout.hpp#L8-L77) [发布写入](b.cpp#L435-L468) [订阅端映射](a.cpp#L323-L334) [订阅端读取](a.cpp#L69-L153)

当前 broker 的接收分支处理注册、FD 交接和断连；`data` 写入发生在 `b.cpp`，`data` 读取发生在 `a.cpp`。 [broker 的注册/断连处理](broker.cpp#L74-L145) [publisher 的消息循环](b.cpp#L374-L474) [订阅端读取](a.cpp#L69-L153)

## 一条消息的当前路径

1. publisher 按消息长度选择分档，从对应 free-list head 取一个 chunk；当前布局有 5 档，chunk 大小为 16 / 128 / 256 / 512 / 1024 B。 [分档](include/shared_memory_layout.hpp#L12-L27) [选择与分配](include/shared_memory_layout_helpers.hpp#L3-L9) [head 移动](b.cpp#L400-L447)
2. publisher 写 `{offset, len}` descriptor、`kMagic` 和递增 sequence，然后按已登记 slot 组成该 chunk 的持有位集合。 [写入](b.cpp#L439-L464)
3. publisher 用 release store 发布写下标，再向每个已登记的 `eventfd` 写入 1。subscriber 在 `epoll` 收到 `eventfd` 后 acquire-load 写下标，复制 descriptor，再读取 `data`。 [发布与通知](b.cpp#L466-L473) [唤醒与消费](a.cpp#L242-L260) [连续区间读取](a.cpp#L163-L174)
4. subscriber 清除自己的 slot bit，随后发布自己的读下标；只有 `fetch_and` 返回的旧值等于该 bit 时，代码才把 chunk 接回对应 size class 的 free-list tail。 [消费后的归还](a.cpp#L120-L152)
5. 当 broker 在 subscriber FD 的 `recvmsg()` 返回 0 分支处理关闭时，它向关联 publisher 发 `SubscriberDisconnected(slot)`；publisher 清除该 slot 从 read index 到 write index 的 bit（绕回时拆为两段），将 read index 设为 `kInvalidIndex`，再移除并关闭对应 `eventfd`。 [broker 断连入口](broker.cpp#L317-L347) [断连转发](broker.cpp#L74-L145) [publisher 回收](b.cpp#L182-L198) [控制消息处理](b.cpp#L216-L262)

## 这些机制为什么不是更简单的版本

- ring 的写前代码先扫描有效的 subscriber read index，并在 ring 空间条件不满足时等待，而不是无条件推进写下标。 [最慢读下标](b.cpp#L122-L140) [写前等待](b.cpp#L386-L394)
- 在 broker 正常分配唯一 slot 的路径下，`chunk_reference_counts` 按 slot bit mask 操作：发布端按 slot 构造 mask；正常消费和死亡回收都清除指定 bit，只有 `fetch_and` 返回的旧值等于该 bit 时，代码才归还 chunk。死亡回收据此按 slot 清除其 descriptor 范围内的持有位。 [slot 分配](broker.cpp#L393-L519) [发布 mask](b.cpp#L454-L464) [正常清位](a.cpp#L120-L152) [死亡清位](b.cpp#L142-L197)
- publisher 执行 `track_subscriber()` 时，把新 subscriber 的共享读下标设为它当时读取的 write index；订阅端首次消费前会从该共享下标初始化本地 cursor。 [注册起点](b.cpp#L200-L212) [本地 cursor 初始化](a.cpp#L156-L161)
- `eventfd` 在当前实现中用于唤醒；payload 仍由 subscriber 从共享内存读取。 [eventfd 创建与交接](broker.cpp#L423-L432) [写端通知](b.cpp#L470-L473) [读端等待](a.cpp#L222-L260)

## 当前不能宣称什么

- 当前 `kill -9` 生命周期脚本不能按默认调用进入其断言：脚本无参数启动 `a.out` / `b.out`，而当前程序分别要求 topic 参数、以及 topic 和共享内存名。 [脚本启动](tests/test_two_subscribers_kill9.sh#L482-L500) [publisher 参数](b.cpp#L302-L307) [subscriber 参数](a.cpp#L176-L182)
- 即使补齐启动参数，脚本第一个 replacement case 固定期待 slot 1；当前 broker 会把断连 slot 放回最小堆，随后优先重新分配 slot 0。 [脚本断言](tests/test_two_subscribers_kill9.sh#L579-L581) [slot 初始化/归还/分配](broker.cpp#L52-L56) [归还](broker.cpp#L102-L119) [分配](broker.cpp#L423-L425)
- `b.out` 的 `main()` 把 `argv[1]` 用作 topic、`argv[2]` 用作共享内存名；发布循环自己生成随机长度、`magic`、sequence 和填充字符，没有从这两个 CLI 字段读取调用者给出的消息体。 [参数](b.cpp#L302-L310) [生成与写入](b.cpp#L26-L43) [主循环](b.cpp#L374-L477)
- `BrokerMessageType` 中没有 unsubscribe 消息类型，`a.cpp` 中也没有发送 unsubscribe 的路径；当前 subscriber 清理入口是 broker 对 socket EOF 或接收错误的处理。 [协议枚举](include/broker_protocol.hpp#L6-L13) [subscriber 发送路径](a.cpp#L211-L220) [broker 清理入口](broker.cpp#L317-L347)
- 在正常 subscriber 归还和 publisher 死亡回收的两段 tail 更新中，代码均为 `load → 写 next → store`；这两段更新处未见 CAS 或覆盖该 tail 更新的锁，因此 README 不把多订阅者 allocator 回收写成已验证安全。 [正常归还](a.cpp#L149-L152) [死亡回收归还](b.cpp#L171-L174)
- `tests/tsan/` 是两个独立 harness 源文件；当前 Makefile 没有 TSan 或 test target。 [harness](tests/tsan/harness.cpp#L29-L73) [ring harness](tests/tsan/ring_harness.cpp#L9-L21) [Makefile](Makefile#L1-L35)

## 继续阅读（不在这里重写）

- [控制协议](docs/broker_protocol.md)
- [共享内存、ring、memory order 与 eventfd 学习/设计笔记](notes/01-spsc-ring/README.md)
- [历史 bug 与调试复盘索引](notes/README.md)
- [动态订阅者测试设计说明](tests/README.md)

详细的模块边界、内存池、引用位图、测试固定值和未完成项见 [FACTS.md](FACTS.md)。
