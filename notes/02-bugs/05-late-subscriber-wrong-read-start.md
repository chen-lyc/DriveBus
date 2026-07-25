# Bug：晚加入订阅者从错误的 descriptor 起点开始消费

> **检索词**：第二个订阅者、late subscriber、`magic` 错、free list、动态注册、起始读下标、`local_rd`、`shared_rd`、错误读取历史 chunk。
>
> **30 秒结论**：`magic` 变成 free-list 的 next offset，只说明读端此刻读到了空闲节点元数据；它不自动说明 chunk 被错误释放。此次是晚加入的 `a2` 从注册前的历史 descriptor 开始读，读到了自己从未持有引用位、但已对真实 holder 合法归还的 chunk。

## 快照与范围

- 触发顺序：`broker → b → a1 → a2`；没有 `kill` 或订阅者断开。
- 时间：2026-07-24。
- 源码范围：从提交 `86731e289a2afdd7ff63077d601d074491455b68` 出发的未提交“动态注册”工作区。本文记录的是随后重构前观察到的故障路径；以后应按符号重新核对锚点，不能把本文当作当前实现已验证的断言。
- 本文只记录根因、证据链和可迁移的调试模型；不记录修复方案。

## 先纠正分类：相同 magic 症状，有两种不同根因

| 情况 | descriptor 发布时的 holder mask | 读到坏 payload 的订阅者 | chunk 的归还是否错误 |
| --- | --- | --- | --- |
| [03：magic 被 free list 覆盖](03-magic-overwritten-by-free-list.md) | 漏掉本应持有的活跃读端 bit | 本来就在 holder 集合中的读端 | 是，真实 holder 尚未完成时就可能被归还 |
| 本次：晚加入读端起点错误 | 历史消息没有 `a2` bit 是正确的 | `a2` 错误读取了自己加入前的历史消息 | 否，历史消息对真实 holder 而言可合法归还 |

因此，下面这句推理只有前半句成立：

```text
magic == free-list next offset
→ 这个 payload 的首个 int 被空闲链元数据写过
→ 一定是某处把它错误归还了
```

最后一箭多了未验证前提：**当前读它的订阅者本来有资格读它。**

## 这次被破坏的不变量

对一个刚加入的订阅者 `s`，三个“起点”必须指向同一个 descriptor：

```text
s.local_rd
== shared_rd[s.slot]
== 第一个发布时 ref-mask 含 s.bit 的 descriptor
```

这里：

- `local_rd`：`a` 进程实际用于扫描 descriptor 的本地 `subscriber_read_index`；
- `shared_rd`：写端用来判断 ring 是否可复用的 `descriptor_read_indices[slot]`；
- `ref-mask`：发布该消息时写入 `chunk_reference_counts` 的订阅者位图。

这不是“几个变量碰巧相等”的样式要求，而是读取资格的定义：**订阅者只能消费它在发布时已经拥有 bit 的 descriptor。**

## 时序：a2 为什么会读错 chunk

令 `W` 为 `a2` 注册时的写下标；`M0 ... M(W-1)` 是此前已发布的历史消息。此时只有 `a1` 存在。

```text
阶段 1：历史消息发布
M0 ... M(W-1) 的 mask = 0b01        // 只有 a1，正确

阶段 2：a2 动态注册
shared_rd[a2] = W                   // 写端把 a2 的起点定在 W
a2.local_rd = 0                     // a2 的本地消费游标仍从 0 开始

阶段 3：下一条消息发布
M(W) 的 mask = 0b11                 // a1、a2 都是 holder，正确

阶段 4：交错执行
a1 先消费历史 Mi：0b01 -> 0b00
Mi 对真实 holder 已可归还，随后可被 free list 链接或复用

a2 收到 eventfd：从 local_rd = 0 扫描
错误读到历史 Mi 的 descriptor 和 payload
```

`a2` 不需要在历史 `Mi` 上“错误清位”才会触发问题；它只要在 `a1` 已合法归还 `Mi` 之后继续读取，就已经越过了自己的生命周期边界。free list 后续把某个空闲 chunk 头部写成 next offset 时，`a2` 读到的首个 `int` 就不再是 `kMagic`。

这也解释了时序性：

- `a2` 若先读到历史 payload，可能暂时看见旧的 `kMagic`；
- `a1` 若先完成最后一个真实 bit 的清除，且 free list 已链接/复用该 chunk，`a2` 就可能看见 next offset；
- 若注册恰好发生在 `W == 0`，本地与共享起点会偶然一致，症状可能暂时消失。

`expected_seq = -1` 后把首次看到的序号设为期望值，也会掩盖“a2 在读历史消息”的 seq 信号；这不是根因，只说明为什么本次更容易只表现为 magic 错。

## 源码如何支持这条链

【源码事实】故障快照中，写端为新 slot 设置了当前写位置作为共享读起点；随后才给新发布的消息构造包含新 slot 的 mask。动态注册的入口、订阅者跟踪和 mask 构造位于 [b.cpp 的 `track_subscriber()`、控制消息处理与发布路径](../../b.cpp#L169-L406)。

【源码事实】读端的本地 `subscriber_read_index` 从 `0` 起步，[`consume_contiguous_messages()`](../../a.cpp#L145-L153) 以它作为扫描起点；读端收到注册后只保存 slot 和 eventfd，并不从共享读下标恢复本地起点。[`read_data()`](../../a.cpp#L68-L141) 在读取 payload 后清自己的 bit，最后真实 holder 才会把 chunk 接回 free list。

【因果推论】把两段事实放在同一条时序中，就得到 `a2.local_rd = 0`、`shared_rd[a2] = W`，而 `a2.bit` 只从 `W` 后开始出现。这个矛盾已足以解释“读到已合法归还 payload”；本次不需要借助 kill/断开路径。

## 为什么当时会卡住

当时的推理不是随便猜。它正确识别了：

```text
magic 像 next offset
→ free list 曾改写 payload 头部
→ 需要沿 mask → 清位 → 归还 路线追查
```

卡住的原因是把资源生命周期从“消息已经被发布”才开始写，而遗漏了更前面的 **订阅者激活边界**：新订阅者从哪个 descriptor 开始有资格读取？

原来的消费循环虽然没有修改，但它的旧前提变了。静态订阅者启动时，`local_rd`、`shared_rd` 与 holder mask 的起点天然同相；动态加入 `a2` 后，这三个状态由不同位置建立，不能再把“循环代码没改”当作“循环前提没变”。

这次真正遗漏的不是 free list、位图或 atomic 知识，而是把**成员集合变化当作一个协议状态转换**来审查：加入动作同时定义 membership、两个读游标和第一条可读消息。

## 本次证据链是怎样想到的

不是从“谁最可疑”开始，而是依次做四件事：

1. 把 `magic == next offset` 当作“谁写过 payload 头”的线索，不把它直接升格为“释放路径有错”。
2. 用 `a2` 注册这一刻把时间线切成“历史消息”和“新消息”两段，分别写 holder mask。
3. 并列比较 `a2` 的本地读游标、共享读下标和其 bit 第一次出现的位置。
4. 做反事实检查：若 `a2` 根本不读 `[0, W)`，`a1` 归还这些历史 chunk 是否仍正确？答案是“正确”，于是第一处错误不在归还，而在 `a2` 的读取范围。

这条反事实特别有用：它会迫使调试者区分“对象被错误释放”与“一个不该读它的角色仍在读”。

## 下次遇到同类症状的检查表

不要只问“谁归还了 chunk”，先写出下面四项：

1. 这条 descriptor 发布时，holder 是谁？mask 中有哪些 bit？
2. 当前 reader 为什么有资格读它？它的 bit 或 epoch 在哪里证明？
3. `local cursor`、`shared cursor`、membership snapshot 是否来自同一个激活起点？
4. 假设归还路径完全正确，当前 reader 是否仍可能读到这个对象？若会，先查 reader 的进入范围。

> **口诀**：看到“已归还对象被读到”，先别只问“谁错误归还了它”；还要问“读它的人，当时本来有资格读它吗？”

## 相关笔记

- [03：payload 头部 magic 被 free list 覆盖](03-magic-overwritten-by-free-list.md)：相同症状、不同根因；该事故是发布时漏掉真实 holder bit。
- [04：沿状态链倒查，不按模块猜](04-debugging-concurrent-bugs.md)：本次把状态链向前补到了“订阅者激活与起点建立”。
- [局部变量固定值与 while/if](../01-spsc-ring/06-local-variables-and-while-vs-if.md)：本地 cursor 是消费语义的一部分，不是可随意替换的临时缓存。
