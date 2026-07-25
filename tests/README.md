# 动态订阅者 `kill -9` 回归测试

[`test_two_subscribers_kill9.sh`](test_two_subscribers_kill9.sh) 验证当前 DriveBus 原型中，订阅者可以在 publisher 已运行后加入、在持有未完成引用时异常退出，以及“最后一个订阅者退出后又有新订阅者加入”这几条生命周期路径。

它是黑盒回归测试：只根据进程存活、输出序号和错误日志判断，不依赖某个内部函数刚好被调用。

## 调用

在仓库根目录执行：

```bash
bash tests/test_two_subscribers_kill9.sh
```

默认会运行四个独立 case：

```bash
READY_SEQUENCE=64 \
JOIN_SEQUENCE_STRIDE=17 \
JOINED_SUBSCRIBER_SEQUENCE_DELTA=32 \
POST_KILL_SEQUENCE_DELTA=128 \
QUIESCENT_OBSERVATIONS=25 \
PHASE_TIMEOUT_SECONDS=30 \
CASE_COUNT=4 \
CXX=g++ \
bash tests/test_two_subscribers_kill9.sh
```

脚本每次都会用 `make -B` 以 `DEBUG_CHECKS=1 DEBUG_SYMBOLS=1` 重编译仓库根目录的 `a.out`、`b.out` 和 `broker.out`。`ENABLE_DEBUG_CHECKS` 会改变 `SharedData` 布局，因此 broker、publisher 和 subscriber 必须使用同一次构建的二进制。

## 每轮覆盖的状态边界

每个 case 都重启 broker，且至多注册两个 subscriber。这是刻意的：当前 broker 的 slot 单调递增而不复用，不能把同一个 broker 生命周期当成无限 join/exit 压测。

默认四轮分别是：

```text
1. broker → publisher（初始 0 个 sub）→ sub1
   → 暂停 sub1，等 publisher 产生一条 sub1 尚未来得及清引用的消息
   → kill -9 sub1 → publisher 安全静默 → sub2 加入并恢复推进

2. publisher 运行中：sub2 晚加入
   → 暂停并 kill -9 原有 sub1
   → publisher 与 sub2 继续跨一整圈 descriptor ring

3. publisher 运行中：sub2 晚加入
   → 暂停并 kill -9 晚加入的 sub2
   → publisher 与 sub1 继续跨一整圈 descriptor ring

4. 暂停 publisher
   → sub2 注册成功后立即 kill -9 sub2
   → 恢复 publisher，验证相邻的“注册 / 断连”控制消息不会破坏 sub1 的继续推进
```

`SIGSTOP` 只是一种测试注入：它把目标订阅读端固定在某一时刻，再让 publisher 至少发布一条新消息，因此该读端确定还持有该消息的引用位。真正的退出仍是 `SIGKILL`（`kill -9`）。

## 最关键的动态加入断言

启动 `sub2` 前，脚本记录已观察到的 publisher 序号 `P`。`sub2` 的第一条读取必须满足：

```text
first_seq(sub2) > P
```

这不只是检查 `magic` 有没有报错。它直接验证晚加入者没有从自己加入前的历史 descriptor 开始消费；即使历史 chunk 此刻尚未被 free list 覆盖，错误起点也会被这个断言抓住。

每次 `kill -9` 后，publisher 和幸存订阅者都必须各自再推进至少 `POST_KILL_SEQUENCE_DELTA` 条，且该值必须大于 16 个 descriptor slot。这样才能排除“只读完 kill 前残留消息，实际上死亡订阅者的读下标或引用位没有清理”的假成功。

最后一个订阅者退出的 case 不要求 publisher 继续写；当前实现没有活跃订阅者时会安全等待。脚本改为等待一个连续的无新增序号窗口，并要求 broker/publisher 仍存活、没有错误；新订阅者加入后再要求两端恢复跨 ring 推进。

## 参数

| 参数 | 默认值 | 作用 |
|---|---:|---|
| `READY_SEQUENCE` | `64` | publisher 与第一个动态加入的 sub 都至少推进到该序号，才进入该轮的退出/再加入阶段；必须大于 16。 |
| `JOIN_SEQUENCE_STRIDE` | `17` | 每个 case 提高初始推进目标的步长。17 不是 ring 长度 16 的倍数，重复 case 会改变晚加入时相对 descriptor ring 的位置。 |
| `JOINED_SUBSCRIBER_SEQUENCE_DELTA` | `32` | 晚加入 sub 在首条消息后必须继续消费的数量；必须大于 16。 |
| `POST_KILL_SEQUENCE_DELTA` | `128` | 退出后 publisher 与幸存 sub 必须额外推进的数量；必须大于 16。 |
| `QUIESCENT_OBSERVATIONS` | `25` | 最后一个 sub 退出后，publisher 日志连续不增长的轮数；每轮约 20 ms，默认约 0.5 秒。 |
| `PHASE_TIMEOUT_SECONDS` | `30` | 每个可观测等待阶段的最长秒数。 |
| `CASE_COUNT` | `4` | 执行轮数；大于 4 时按上述四种路径循环，增加时序压力。 |
| `CXX` | `g++` | C++ 编译器路径或名称，不包含额外编译选项。 |

例如，强化压力运行：

```bash
CASE_COUNT=20 POST_KILL_SEQUENCE_DELTA=512 \
  bash tests/test_two_subscribers_kill9.sh
```

## 通过与边界

通过表示上述状态转换中：进程没有提前退出，日志中没有 `error magic`、`error seq`、重复归还、越界或超时证据，且要求的跨 ring 推进均已发生。

失败时脚本保留日志目录，例如：

```text
/tmp/drivebus-two-subscribers-kill9.xxxxxx
```

这个脚本覆盖的是生命周期**状态边界**，不能证明任意 CPU 指令点的所有交错都安全。若要稳定命中“已注册但尚未消费第一条”或 publish 某条具体指令之间的窗口，需要在 `a.cpp` / `b.cpp` 增加显式测试 gate 或确认协议。

目前程序也没有协议级的 graceful unsubscribe：`SIGKILL` 和默认 `SIGTERM` 都依赖内核关闭 socket，broker 再做 EOF/断连处理。因此本测试验证的是异常进程退出后的回收与恢复，而不是一个已经定义好的“正常注销”协议。

运行前需 Linux、Bash、`g++`（或 `CXX`）、`flock`、`grep`、`awk`、`mkfifo` 和 `/proc`。程序固定使用 `/tmp/broker.sock` 与 `/dev/shm/shm`；脚本会拒绝覆盖已存在的同名 IPC 资源，也不能与手工启动的 DriveBus 实例并行运行。
