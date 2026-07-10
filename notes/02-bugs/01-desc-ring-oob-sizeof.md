# Bug：desc_ring 越界 — sizeof vs 元素个数

## 现象

`a.out` 稳定崩溃，`b.out` 同步崩溃。GDB 显示 `offset = 21354532`（恰好是 `kMagic`）。

```
a.out:
read 16 byte ... offset 0   seq 0   (a)
read 16 byte ... offset 16  seq 1   (b)
...
read 16 byte ... offset 224 seq 14  (o)
read 16 byte ... offset 240 seq 15  (p)
→ SIGSEGV
```

## 错过的重要线索

**a.out 每次都恰好输出 16 条消息后才崩溃。**

`desc_ring` 的容量是 `kDescNum = 16`。输出数量精确等于缓冲区大小 → 第 17 个元素越界。这条信息在 GDB 的输出里、在终端的日志里**一直都有**，但看 bug 的时候完全没注意到它。

> **经验**：当崩溃前的处理/输出次数精确等于某个缓冲区容量时，首先怀疑该缓冲区的越界或 wrap 条件。

## 根因

`b.cpp` wrap 条件用错了比较基准：

```cpp
// Bug 版本
int candidate = wr + 1;                                    // wr = 描述符个数 (0..)
if (candidate >= sizeof(p->desc_ring))                     // sizeof = 128 字节！
    p->offset_write.store(candidate % sizeof(p->desc_ring), ...);
```

`wr` 含义是元素个数，应该跟 `kDescNum`（16）比，却跟了 `sizeof(p->desc_ring)`（128 字节）比。`wr` 从 0 递增到 16 → `16 >= 128` 永远不成立 → 不绕回 → `desc_ring[16]` 越界写入。

越界位置恰好覆盖了紧随其后的 `head` 结构体，污染了空闲链表。

## 修复

`sizof(p->desc_ring)` → `kDescNum`。

## 总结：运行次数作为定位线索

以后看 bug 时追问：

1. 崩溃前**处理了多少次**？这个数字和代码里哪个常量/数组大小一致？
2. 如果崩溃出现在第 N+1 次、而缓冲区容量是 N → 优先查越界/wrap/off-by-one
3. 这个数字可能藏在终端输出、日志、GDB 的 `n` 值里——**不要只看变量值，也要数次数**
