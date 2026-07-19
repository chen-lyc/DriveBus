# 双读端 `kill -9` 回归测试

[`test_two_subscribers_kill9.sh`](test_two_subscribers_kill9.sh) 用于验证当前工作区的 DriveBus 原型在下面这个场景中仍能继续工作：

```text
1 个 publisher（发布写端） + 2 个 subscriber（订阅读端）
→ 两个读端都开始消费
→ kill -9 强制杀死其中一个读端
→ 写端和幸存读端继续跨多轮 ring（环）推进
```

它是**黑盒回归测试**：每次修改读端死亡清理、引用位或空闲链表逻辑后运行它，防止已经修过的问题重新出现。

## 调用

在仓库根目录执行：

```bash
bash tests/test_two_subscribers_kill9.sh
```

脚本已有可执行权限，也可以这样执行：

```bash
./tests/test_two_subscribers_kill9.sh
```

脚本会临时编译当前的 `a.cpp` 和 `b.cpp`，不会覆盖根目录已有的 `a.out`、`b.out`。编译时两个程序都会使用 `-DENABLE_DEBUG_CHECKS`；这个宏会改变 `SharedData` 的布局，所以两端必须保持一致，不能一端开启、一端关闭。

## 默认行为

默认命令等价于：

```bash
READY_SEQUENCE=64 \
POST_KILL_SEQUENCE_DELTA=128 \
PHASE_TIMEOUT_SECONDS=30 \
CASE_COUNT=2 \
CXX=g++ \
bash tests/test_two_subscribers_kill9.sh
```

| 参数 | 默认值 | 作用 |
|---|---:|---|
| `READY_SEQUENCE` | `64` | 两个读端和写端都至少推进到该 `seq` 后，脚本才会执行 `kill -9`。`seq` 是 **sequence number**，消息序号。它证明两个读端不是只连接成功，而是真的已经消费消息。 |
| `POST_KILL_SEQUENCE_DELTA` | `128` | kill 后，写端和幸存读端都必须在各自 kill 前的序号基础上再推进至少这么多条消息。该值必须大于 16，因为当前描述符 ring 有 16 个槽位；否则可能只是把 kill 前已经发布的旧消息读完，产生“写端其实已卡住”的假成功。 |
| `PHASE_TIMEOUT_SECONDS` | `30` | 每个等待阶段的最长秒数：等待 Unix domain socket（Unix 域套接字）建立、等待两个读端进入工作状态、等待 kill 后的继续推进。超时即失败。 |
| `CASE_COUNT` | `2` | 测试轮数。奇数轮杀第一个启动的读端，偶数轮杀第二个启动的读端；默认两轮因此会覆盖两个读端各一次。 |
| `CXX` | `g++` | C++ 编译器变量，填写编译器可执行文件，例如 `clang++`。这里只能填编译器路径或名称，不能把额外编译选项一起放进该变量。 |

Shell 中的 `NAME=value command` 表示“只给这一次命令设置参数”，不会永久修改当前终端环境。

## 常用命令

快速执行默认两轮：

```bash
bash tests/test_two_subscribers_kill9.sh
```

增加轮数和 kill 后进度，作为更强的压力回归：

```bash
CASE_COUNT=20 POST_KILL_SEQUENCE_DELTA=512 \
  bash tests/test_two_subscribers_kill9.sh
```

机器较慢时增加每个阶段的等待时间：

```bash
PHASE_TIMEOUT_SECONDS=60 \
  bash tests/test_two_subscribers_kill9.sh
```

使用 Clang 编译：

```bash
CXX=clang++ bash tests/test_two_subscribers_kill9.sh
```

## 通过与失败的含义

成功时会出现类似输出：

```text
Case 1: SIGKILL subscriber 1 at publisher seq=...
Case 1: PASS (subscriber 2 and publisher crossed 128 post-kill messages)
Case 2: SIGKILL subscriber 2 at publisher seq=...
PASS: 2 two-subscriber SIGKILL recovery case(s) completed.
```

每轮的通过条件是：

1. 两个读端都已读到消息；
2. 脚本向其中一个读端发送 `SIGKILL`；
3. 写端和另一个读端都继续推进至少 `POST_KILL_SEQUENCE_DELTA` 条消息；
4. 输出中没有 `error magic`、`error seq`、`time out`、重复归还 chunk 或越界等错误。

`SIGKILL` 是 **Signal Kill**，强制终止信号；`kill -9` 是它的命令行写法。目标读端无法捕获或自行清理这个信号，因此这个测试正是在检查写端能否发现并处理这种非正常退出。

脚本达到上述条件后会主动结束它自己启动的进程。它不等待 `all write over`，因此它验证的是“读端死亡后的恢复进展”，不是完整消息量的吞吐测试。

失败时脚本会保留日志目录，例如：

```text
/tmp/drivebus-two-subscribers-kill9.xxxxxx
```

其中包含写端、两个读端及各自标准错误输出的日志尾部，便于继续定位。

## 运行前提与边界

- 需要 Linux、Bash、`g++`（或通过 `CXX` 指定的编译器）、`flock`、`grep`、`awk` 和 `mkfifo`。
- 当前程序固定使用 `/tmp/demo.sock` 和 POSIX（Portable Operating System Interface，可移植操作系统接口）shared memory 名称 `/shm`（Linux 上对应 `/dev/shm/shm`）。因此不能与手工启动的 `a.out`、`b.out` 或另一份此脚本并行运行。
- 如果这些 IPC（Inter-Process Communication，进程间通信）资源已经存在，脚本会拒绝覆盖它们；先确认没有其他 DriveBus 实例，再处理遗留环境。
- `FIFO` 是 **First In, First Out**，先进先出；脚本用命名 FIFO 过滤大量测试输出，只保留序号和错误证据。
- 此脚本提高了发现时序问题的概率，但不能数学证明所有纳秒级并发交错都安全。需要覆盖更多时序时，增加 `CASE_COUNT`；若要稳定命中特定指令间隙，则仍需要专门的测试钩子。
