# DriveBus 使用指南

[返回 README](../README.md#接口与使用样例)

本指南对应当前 `Publisher`、`Subscriber` 接口。根目录的 [pub_node.cpp](../pub_node.cpp) 与 [sub_node.cpp](../sub_node.cpp) 是同步收发样例；下文补充异步完整程序及其他 API 用法。

- [同步发布与订阅](#同步发布与订阅)
- [异步发布](#异步发布)
- [异步订阅](#异步订阅)
- [编译与运行完整示例](#编译与运行完整示例)
- [多 topic 与多发布者](#多-topic-与多发布者)
- [数据与生命周期](#数据与生命周期)
- [后台线程与错误处理](#后台线程与错误处理)

## 同步发布与订阅

`Publisher::create()` 和 `Subscriber::create()` 均默认使用同步模式。两者的创建结果都在 `.value` 中存放对象或各自的 `CreateError`，应先检查错误再取出对象。

| 操作 | 成功结果 | 失败结果 |
| --- | --- | --- |
| `publisher.publish(payload, timeout)` | `PublishTicket`，其 `disposition` 为 `Published`，完成 future 已就绪 | `PublishError` |
| `subscriber.subscribe(timeout)` | `MessageBatch`，每条消息是自有的字节数组 | `SubscribeError` |

同步发布会在当前调用线程中处理发布与等待，不启动发布 worker。没有订阅者、描述符环满或对应大小的内存池不足时，调用可能等到 `Timeout`。同步订阅在本次等待没有消息时返回 `Timeout`。

同步发布使用带超时的重载；异步发布使用不带超时的重载，混用会返回 `PublishError::ModeMismatch`。异步订阅使用回调接收消息，不应再从外部调用同一对象的 `subscribe()`，否则会与后台线程竞争消费状态。

同步样例的命令行接口为：

```bash
./build/pub_node                         # 每秒发布默认文本
./build/pub_node "camera frame ready"    # 每秒发布自定义文本
./build/pub_node --help                  # 查看参数
./build/sub_node                         # 订阅源码中配置的 Camera topic
```

更换模式或 topic 需要使用 C++ API，现有命令行样例没有对应的切换参数。启动与退出顺序见 [快速上手](../README.md#快速上手)。

## 异步发布

以下是完整程序，可保存为 `build/async_pub.cpp`。它提交一条消息，等待最终发布状态，并在成功后等待 Enter，方便另一终端观察接收结果。

```cpp
#include "publisher.hpp"
#include <chrono>
#include <future>
#include <iostream>
#include <span>
#include <string_view>
#include <variant>

int main() {
    auto created = Publisher::create(
        TopicId::Camera, "/drivebus_async_camera", Publisher::ASYNCHRONOUS);
    if (const auto *error = std::get_if<Publisher::CreateError>(&created.value)) {
        std::cerr << "publisher create error=" << static_cast<int>(*error) << '\n';
        return 1;
    }
    auto &publisher = std::get<Publisher>(created.value);

    constexpr std::string_view message = "Hello from async publisher!";
    const auto payload = std::as_bytes(std::span{message.data(), message.size()});
    const auto result = publisher.publish(payload);
    if (const auto *error = std::get_if<Publisher::PublishError>(&result)) {
        std::cerr << "enqueue error=" << static_cast<int>(*error) << '\n';
        return 2;
    }

    const auto &ticket = std::get<Publisher::PublishTicket>(result);
    if (ticket.future.wait_for(std::chrono::seconds{10}) != std::future_status::ready) {
        std::cerr << "publication is still pending; this demo will stop the publisher\n";
        return 3;
    }
    const auto completion = ticket.future.get();
    if (completion != Publisher::PublishCompletion::Published) {
        std::cerr << "publication completion=" << static_cast<int>(completion) << '\n';
        return 4;
    }

    std::cout << "published; press Enter to exit" << std::endl;
    std::cin.get();
    return 0;
}
```

### 入队结果与最终结果

`publish(payload)` 将 payload 复制到本地待发布队列，返回时可能还没有写入共享内存。此时调用者可以复用原始数据缓冲区；是否发布成功，应检查凭据中的 future。

| `PublishDisposition` | 本次调用确认了什么 |
| --- | --- |
| `Published` | 同步发布已完成写入 |
| `Enqueued` | 异步消息已入队 |
| `EnqueuedAfterOverwritingOldest` | 异步消息已入队，并覆盖了一条旧待发布消息 |

| `PublishCompletion` | 当前这条消息的最终状态 |
| --- | --- |
| `Published` | 已写入共享内存并完成数据通知路径，尚未确认订阅者业务处理完成 |
| `Overwritten` | 等待发布期间被后续提交覆盖 |
| `WorkerStopped` | 后台线程停止，该消息未获成功完成确认 |
| `WorkerFailed` | 后台线程因错误退出，该消息未获成功完成确认 |

当前待发布容量上限为 64 条，计入正在处理的消息。达到上限时，代码优先覆盖处于阻塞状态的在途消息，否则移除队首待发布消息。旧消息的 future 返回 `Overwritten`；新消息的 future 仍需单独等待。这个策略目前没有运行时配置接口。

`ticket.future.wait_for(...)` 超时只表示本次等待结束，不取消消息。可以保留 ticket 稍后查询；如果像示例一样直接退出作用域，发布者析构将停止 worker，并结束尚未完成消息的凭据。

## 异步订阅

以下完整程序可保存为 `build/async_sub.cpp`。它在后台回调中复制第一条消息，通过 `std::promise` 交给主线程，收到后自动退出。

```cpp
#include "subscriber.hpp"
#include <array>
#include <chrono>
#include <future>
#include <iostream>
#include <string>
#include <variant>

int main() {
    std::promise<std::string> first_message;
    auto received = first_message.get_future();
    constexpr std::array<TopicId, 1> topics = {TopicId::Camera};

    auto created = Subscriber::create(topics, Subscriber::ASYNCHRONOUS,
        [&first_message, delivered = false](char *data, size_t size) mutable {
            if (!delivered) {
                first_message.set_value(std::string(data, size));
                delivered = true;
            }
        });
    if (const auto *error = std::get_if<Subscriber::CreateError>(&created.value)) {
        std::cerr << "subscriber create error=" << static_cast<int>(*error) << '\n';
        return 1;
    }
    auto &subscriber = std::get<Subscriber>(created.value);

    std::cout << "waiting for one Camera message" << std::endl;
    if (received.wait_for(std::chrono::seconds{10}) != std::future_status::ready) {
        auto stopped = subscriber.worker_exit_future();
        if (stopped.valid() &&
            stopped.wait_for(std::chrono::seconds{0}) == std::future_status::ready) {
            std::cerr << "subscriber worker exited=" << static_cast<int>(stopped.get()) << '\n';
        } else {
            std::cerr << "no message received within ten seconds\n";
        }
        return 2;
    }

    std::cout << "received: " << received.get() << std::endl;
    return 0;
}
```

### 回调的数据与执行线程

回调在该订阅者的后台线程中顺序执行。示例中的 `delivered` 只由该线程访问；`std::promise` 将复制后的字符串安全地交给主线程。

- `data` 指向当前共享内存消息的正文，只能在本次回调期间使用。保存指针到队列后直接返回，后续访问可能遇到已被复用的 chunk。
- 后续业务需要数据时，像示例一样复制为 `std::string` 或 `std::vector<std::byte>`。尽管签名使用 `char *`，仍应将消息正文按只读数据处理。
- 回调应尽快返回；引用释放发生在回调返回之后，长时间阻塞会拖慢消费并影响发布端背压。
- 回调抛出的异常会使 worker 以 `WorkerException` 退出；可通过 `worker_exit_future()` 观察。
- 回调捕获的引用必须比订阅者活得更久。示例先声明 `first_message`，再创建订阅者，退出时订阅者先析构并等待回调线程结束。
- 保持订阅对象存活才能持续接收；不要在该对象自身的回调中移动或销毁它。

## 编译与运行完整示例

在项目根目录执行 `make -j2`，然后将上面两个完整程序分别保存为 `build/async_pub.cpp` 和 `build/async_sub.cpp`。临时示例源码及编译产物均位于已被 Git 忽略的 `build/`。

使用与当前 Makefile 一致的布局选项编译：

```bash
g++ -std=c++20 -Wall -Wextra -Wpedantic -pthread -DENABLE_DEBUG_CHECKS -Iinclude \
    build/async_pub.cpp src/publisher.cpp src/fd_helpers.cpp src/logger.cpp \
    -o build/async_pub

g++ -std=c++20 -Wall -Wextra -Wpedantic -pthread -DENABLE_DEBUG_CHECKS -Iinclude \
    build/async_sub.cpp src/subscriber.cpp src/fd_helpers.cpp src/logger.cpp \
    -o build/async_sub
```

这两个文件是从本文复制的独立程序，不是现有 Makefile 的构建目标。`make clean` 只清理主项目的四个可执行程序。

三个终端均从项目根目录运行，依次执行：

```bash
# 终端一：启动控制面
./build/broker.out
```

```bash
# 终端二：等待消息；该示例最多等待十秒
./build/async_sub
```

```bash
# 终端三：在订阅端等待超时前启动
./build/async_pub
```

订阅端应输出 `received: Hello from async publisher!` 后退出；发布端输出 `published; press Enter to exit`，按 Enter 退出。最后在 broker 终端按 Ctrl+C。

也可以交叉组合：`./build/sub_node` 可接收上述异步发布程序的消息；上述异步订阅程序也可接收 `./build/pub_node` 的消息。各程序使用相同的 topic、共享内存布局和控制协议即可通信，执行模式由各端分别选择。

## 多 topic 与多发布者

### 多 topic 订阅

同步和异步创建都接收相同的 topic 列表，例如：

```cpp
constexpr std::array<TopicId, 2> topics = {
    TopicId::Camera,
    TopicId::Lidar,
};
auto created = Subscriber::create(topics, Subscriber::SYNCHRONOUS);
```

当前有效 topic 定义见 [broker_protocol.hpp](../include/broker_protocol.hpp)：`Camera`、`Lidar`、`VehicleState`。单次注册最多放入 5 个 ID，不代表已经定义了 5 种有效 topic。

当前 `MessageBatch` 只包含字节数组，回调也只收到指针和长度，均不附带 `TopicId` 或发布者身份。需要在业务层区分来源时，可为不同 topic 创建独立订阅对象，或在 payload 中编码消息类型与来源字段。

### 同一 topic 多发布者

每个发布者拥有独立共享内存；同一 topic 的两个发布者必须使用不同名称，例如 `/camera_front` 与 `/camera_rear`。创建过程会先移除传入的共享内存名称，不应复用其他活跃实例的名称。

各发布者分别维护自己的消息顺序；当前没有 topic 全局序号或跨发布者排序接口。现有 `pub_node` 把名称固定为 `/drivebus_pub_node_camera`，运行多个实例前需修改源码中的名称或自行编写使用 API 的程序。

## 数据与生命周期

| 使用位置 | 数据的所有权与有效期 |
| --- | --- |
| `publish(payload, timeout)` 输入 | 调用期间保持 span 指向的数据有效；成功时正文已复制到共享内存 |
| `publish(payload)` 输入 | 返回前复制到本地待发布队列，返回后调用者可复用原缓冲区 |
| 同步 `MessageBatch` | 订阅者返回自有字节数组，生命周期由调用者控制 |
| 异步回调 `data` | 借用共享内存正文，按只读数据处理，仅在本次回调内使用 |

### 二进制 payload

接口按字节和长度传输数据，不依赖字符串结束符。例如，已有 `publisher` 时，可以发布带零字节的缓冲区：

```cpp
const std::array<std::byte, 4> bytes = {
    std::byte{0x01}, std::byte{0x00}, std::byte{0x7f}, std::byte{0xff},
};
const auto result = publisher.publish(
    std::span<const std::byte>{bytes}, std::chrono::seconds{1});
```

`pub_node` / `sub_node` 的文本展示方式只适合演示字符串。二进制业务应按约定的字段长度、字节序与编码格式解码。发送端和接收端需要自行约定格式，直接发送 C++ 对象的内存布局会受到填充、指针和 ABI 的影响。

单条业务 payload 上限是 `1024 - 2 * sizeof(int)` 字节；4 字节 `int` 时为 1016 字节。超出返回 `PayloadTooLarge`。内部的 magic 和 sequence 由发布者添加，接收 API 交付的是业务正文，调用者无需自行添加或剥离这两个内部字段。

`Publisher` 与 `Subscriber` 均禁止复制，支持移动。移动或析构前应结束外部的并发成员调用；移动可能抛出异常，不能假设为 `noexcept`。作用域退出时对象负责释放自身持有的资源，异步模式会先停止并等待后台线程结束。

## 后台线程与错误处理

### 常见返回值

| 返回值 | 调用者应检查什么 |
| --- | --- |
| `CreateError` | broker 是否已启动、IPC 资源操作是否成功、模式与回调是否匹配 |
| `PublishError::Timeout` | 同步发布未在给定时间完成；按业务策略决定是否重试 |
| `SubscribeError::Timeout` | 本次等待没有消息；持续接收时可继续调用 |
| `PayloadTooLarge` / `ModeMismatch` | 修正 payload 大小或发布重载；重试相同调用不能解决 |
| `WorkerExited` | 异步发布线程已退出，读取 worker 退出原因 |
| `BrokerUnavailable` / `DataPathFailed` | 查看对应进程日志，按业务需要终止或重建对象 |

变体返回值用于报告这些操作错误；内存分配等 C++ 操作仍可能抛出异常。完整枚举见 [publisher.hpp](../include/publisher.hpp) 与 [subscriber.hpp](../include/subscriber.hpp)。

### 观察 worker 退出

异步模式中，发布者与订阅者都提供 `worker_exit_future()`。下面以已创建的异步 `publisher` 为例：

```cpp
auto stopped = publisher.worker_exit_future();
if (stopped.valid() &&
    stopped.wait_for(std::chrono::seconds{0}) == std::future_status::ready) {
    const auto reason = stopped.get();
    if (reason != Publisher::RunError::None) {
        std::cerr << "publisher worker exited=" << static_cast<int>(reason) << '\n';
    }
}
```

这个 future 反映线程退出原因，逐条发布结果仍由各自的 `PublishTicket::future` 表达。

`worker_exit_event_fd()` 可用于把异步线程退出通知接入应用自己的事件循环。返回的 FD 归对象所有，应用不要关闭它；先确认 FD 有效，将其加入事件循环后，在可读时结合 `worker_exit_future()` 读取原因。外部事件循环应在对象销毁或移动前移除旧监听。

移动可能重启 worker 并重建通知状态，移动后应重新取得退出 future 与 FD。同步模式没有后台 worker，应直接检查 `publish()` / `subscribe()` 返回值。

当前同步发布对运行中新增订阅者的通知处理可能延迟，演示多订阅者时先启动所有订阅端，再启动同步发布端。其余容量与实现边界见 [README 当前配置与边界](../README.md#当前配置与边界)。
