# Broker 控制协议

## 范围

本文定义 broker、publisher 和 subscriber 之间控制消息的共同规则。

当前协议只包含两个组成部分：

```text
message = message_type + payload
```

## `message_type`

`message_type` 表示消息类型。

- `message`：消息。
- `type`：类型。

接收方必须先根据 `message_type` 判断消息语义，再按该类型解释 `payload`。

## `payload`

`payload` 表示消息携带的实际内容；原义是“有效载荷”。

同一个 `payload` 的字节只在对应的 `message_type` 下才有确定含义。例如，槽位下标、订阅者注册信息和订阅者死亡信息都由各自的消息类型决定如何解释。

## 当前边界

本协议当前只约束 `message_type` 与 `payload`。新的通信需求出现时，先在本文补充对应消息类型及其 payload 语义，再实现代码。
