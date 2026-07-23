# Linux IPC 基础：共享内存、Unix Socket 与 fd 传递

> 核心心法：**Linux 底层 I/O 的本质不是"传字符串"，而是"传一段内存：起始地址 + 字节长度"。** `'\0'` 只是 C 字符串的一种结束约定，不是操作系统 I/O 的本质。

本笔记覆盖 DriveBus 项目使用的三块 IPC 基础设施，按依赖关系自底向上组织。

---

## 目录

- [一、共享内存：shm_open → ftruncate → mmap](#一共享内存shm_open--ftruncate--mmap)
- [二、Unix Domain Socket](#二unix-domain-socket)
- [三、sendmsg / recvmsg：传递 fd 的结构体体系](#三sendmsg--recvmsg传递-fd-的结构体体系)
- [四、字符串与二进制约定](#四字符串与二进制约定)
- [五、项目实战：所有概念如何组合](#五项目实战所有概念如何组合)

---

## 一、共享内存：shm_open → ftruncate → mmap

### 1.1 三步创建共享内存

```cpp
// ① 创建/打开共享内存对象
int fd = shm_open("/shm", O_CREAT | O_RDWR, 0600);
//                    │      └─ flags: 不存在则创建，可读写          └─ mode: 对象权限位 (rw-------)
//                    └─ 名字，在 /dev/shm/ 下表现为 /dev/shm/shm

// ② 设置大小（刚创建时大小为 0）
ftruncate(fd, sizeof(SharedData));

// ③ 映射到当前进程的虚拟地址空间
SharedData *p = static_cast<SharedData *>(
    mmap(nullptr, sizeof(SharedData),
         PROT_READ | PROT_WRITE,    // 映射出的内存可读写
         MAP_SHARED,                // 修改对其它进程可见
         fd, 0)
);

// ④ fd 可以关了——mmap 成功后映射独立于 fd
close(fd);
```

### 1.2 权限三层模型

这是最容易混淆的地方——三个不同的权限概念同时存在：

```cpp
shm_open("/shm", O_CREAT | O_RDWR, 0600);
//                └── flags ──┘  └─ mode ─┘
//
// mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
//                     └────── prot ────────┘
```

```
层级 1: mode (0600)
  控制：共享内存对象本身的权限位
  影响：其他进程以后能不能 shm_open 这个对象
  类似：chmod 600 /dev/shm/shm

层级 2: flags (O_RDWR / O_RDONLY)
  控制：这次拿到的 fd 有什么能力
  影响：能不能对这个 fd 做 read/write/mmap PROT_WRITE
  类似：open() 时决定的本次访问权限

层级 3: prot (PROT_READ | PROT_WRITE)
  控制：映射出来的内存指针能不能读写
  约束：不能超过 fd 的能力（fd 只读 + MAP_SHARED + PROT_WRITE → 失败）
```

> **类比**：`mode` 是门锁，`flags` 是钥匙权限，`prot` 是你进门后能做什么。拿了钥匙不等于门锁消失，但已经进门的人不受门锁变化影响。

### 1.3 不是普通文件

`shm_open` 创建的不是硬盘文件，而是 `tmpfs` 中的内存型对象（Linux 上在 `/dev/shm/` 下可见）。它主要用于 `mmap` 映射，不用于 `read`/`write`。

### 1.4 清理

```cpp
munmap(p, sizeof(SharedData));  // 解除映射
shm_unlink("/shm");             // 删除名字（引用计数归零后真正释放）
//                  注意：unlink ≠ 立即销毁，已映射的内存仍可继续使用
```

### 1.5 `mmap` 原理补充：虚拟内存、页表与物理页

`mmap` 本身不是"共享内存 API"——它是**虚拟内存映射机制**。调用 `mmap` 时，内核在当前进程的虚拟地址空间中建立一段虚拟地址与某个对象（文件、shm 对象、匿名内存）之间的映射。`mmap` 不分配物理内存，只建立映射关系。第一次访问时触发缺页异常，内核才真正分配/映射物理页。

`MAP_SHARED` 告诉内核：对这段映射的修改要作用到底层对象，其他进程可以看到。如果用了 `MAP_PRIVATE`，即使两个进程 `shm_open` 同一个对象，写入时会触发写时复制——各自得到私有页，修改互不可见。

```text
进程 A 虚拟地址 (0x7f10...) ─┐
                              ├── 同一个物理页
进程 B 虚拟地址 (0x7e50...) ─┘
```

两个进程的虚拟地址通常不同，但页表指向同一物理页。这就是共享内存不需要数据拷贝的根本原因——相比管道/Socket 的 `进程A用户态 → 内核缓冲区 → 进程B用户态` 两次拷贝，共享内存直接访问同一物理页。

对比 `read()` 的文件 I/O 路径：

```text
read():  磁盘 → page cache → 用户 buffer（一次 CPU 拷贝）
mmap():  磁盘 → page cache ← 用户虚拟地址直接映射（省掉拷贝）
```

`mmap` 省的不是磁盘 I/O，是 page cache 到用户缓冲区的 CPU 拷贝。

最后：共享内存只解决**数据放在哪里**，不解决**并发访问是否正确**。两个进程同时 `counter++` 仍会更新丢失——所以共享内存之上仍需要 `std::atomic` 和 `pthread_mutex_t(PTHREAD_PROCESS_SHARED)`。位置问题和同步问题是两层独立的问题。`shm_open` + `mmap(MAP_SHARED)` 解决第一层，atomic/mutex 解决第二层。

```cpp
munmap(p, sizeof(SharedData));  // 解除映射
shm_unlink("/shm");             // 删除名字（引用计数归零后真正释放）
//                  注意：unlink ≠ 立即销毁，已映射的内存仍可继续使用
```

---

## 二、Unix Domain Socket

### 2.1 UDS vs TCP

| | TCP Socket | Unix Domain Socket |
|---|---|---|
| 地址 | `IP:端口` | 文件路径，如 `/tmp/demo.sock` |
| 范围 | 可跨机器 | **仅本机** |
| 协议栈 | 走 TCP/IP | 不走网络协议栈 |
| 传 fd | 不支持 | 支持（`SCM_RIGHTS`） |
| 创建 | `socket(AF_INET, SOCK_STREAM, 0)` | `socket(AF_UNIX, SOCK_STREAM, 0)` |

> `SOCK_STREAM` 只表示"可靠、有序、字节流"，不等于 TCP。`AF_UNIX + SOCK_STREAM` = 本机字节流 socket。

### 2.2 独立程序间的连接（六步法）

`b.out`（客户端）连 `a.out`（服务端）：

```
服务端 (a.out)                      客户端 (b.out)
① socket(AF_UNIX, SOCK_STREAM, 0)
② bind(listen_fd, "/tmp/demo.sock")
③ listen(listen_fd, 1)
④ accept(listen_fd) → conn_fd ──── accepts ──→
                                    ⑤ socket(AF_UNIX, SOCK_STREAM, 0)
                                    ⑥ connect(sock, "/tmp/demo.sock")
                                        ↓
                              send_fd(sock, efd) ──→ recv_fd(conn_fd)
```

```cpp
// ========== 服务端 ==========
unlink("/tmp/demo.sock");                           // 删除旧 socket 文件

int listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);    // ①

struct sockaddr_un addr{};
addr.sun_family = AF_UNIX;
strcpy(addr.sun_path, "/tmp/demo.sock");

bind(listen_fd, (sockaddr*)&addr, sizeof(addr));     // ②
listen(listen_fd, 1);                                // ③
int conn_fd = accept(listen_fd, nullptr, nullptr);   // ④（阻塞直到客户端连接）

// ========== 客户端 ==========
int sock = socket(AF_UNIX, SOCK_STREAM, 0);          // ⑤

struct sockaddr_un addr{};
addr.sun_family = AF_UNIX;
strcpy(addr.sun_path, "/tmp/demo.sock");

connect(sock, (sockaddr*)&addr, sizeof(addr));       // ⑥（阻塞直到服务端 accept）
```

### 2.3 `accept()` 后两个参数为什么是 `nullptr`

```cpp
int conn_fd = accept(listen_fd, nullptr, nullptr);
```

`accept()` 的后两个参数用于获取客户端地址。如果不需要记录日志/限流/黑名单，就不需要拿地址。UDS 和 TCP 都可以这样用。

### 2.4 `socketpair()`：父子进程的快捷方式

```cpp
int sv[2];
socketpair(AF_UNIX, SOCK_STREAM, 0, sv);  // sv[0] ←→ sv[1] 已连接，双向
```

适合 fork 场景（父子共享 sv[]），不适合两个独立编译的程序。

---

## 三、sendmsg / recvmsg：传递 fd 的结构体体系

这是 `b.out` 把 `eventfd` 传给 `a.out` 的机制。结构体嵌套关系如下。

### 3.1 全景图

```
msghdr          ← 一次 sendmsg/recvmsg 的"消息说明书"
│
├── msg_iov[]       ← 普通数据缓冲区数组
│   └── iovec       ← 单个缓冲区：{起始地址, 长度}
│       ├── iov_base → 数据地址
│       └── iov_len  → 数据长度
│
├── msg_iovlen      ← iovec 的数量（不是字节数！）
│
└── msg_control     ← 控制信息（传 fd 的关键）
    └── control[]   ← 原始字节数组，内部按 cmsghdr 格式组织
        └── cmsghdr ← 控制消息头
            ├── cmsg_len   = CMSG_LEN(sizeof(int))
            ├── cmsg_level = SOL_SOCKET
            ├── cmsg_type  = SCM_RIGHTS
            └── data[]     = int fd   ← CMSG_DATA(cmsg) 指向这里
```

### 3.2 `iovec`：I/O 向量

```cpp
struct iovec {
    void  *iov_base;   // 数据起始地址
    size_t iov_len;    // 数据长度（字节数）
};
```

```cpp
char dummy = 'F';
struct iovec iov;
iov.iov_base = &dummy;
iov.iov_len  = sizeof(dummy);    // 我不传内容，只传字节
```

> `iov` = **I/O Vector**，不是 `int` 或特殊类型，就是"地址+长度"的包装。

### 3.3 `msg_iovlen`：数量，不是字节数

```cpp
msg.msg_iov    = &iov;   // 指向 iovec 数组
msg.msg_iovlen = 1;      // ← 有 **1 个** iovec，不是 sizeof(iov)
```

**常见错误**：`msg.msg_iovlen = sizeof(iov)` → 如果 `sizeof(iov) == 16`，内核会以为有 16 个 iovec，越界读取。**正确做法永远是写 `1`。**

### 3.4 `msghdr`：消息说明书

```cpp
struct msghdr {
    void         *msg_name;       // 对端地址（UDS 经常不用）
    socklen_t     msg_namelen;    // 地址长度

    struct iovec *msg_iov;        // 普通数据缓冲区
    size_t        msg_iovlen;     // iovec 数量

    void         *msg_control;    // 控制信息缓冲区（传 fd 用这个）
    size_t        msg_controllen; // 控制信息缓冲区总大小

    int           msg_flags;      // 接收时的附加标志
};
```

> **记忆**：`msghdr` = 大箱子（描述整条消息），`cmsghdr` = 小箱子（控制缓冲区里的一则控制消息）。

### 3.5 `cmsghdr` 与 `CMSG_*` 宏

```cpp
struct cmsghdr {
    size_t cmsg_len;     // 本控制消息的实际长度 = CMSG_LEN(datalen)
    int    cmsg_level;   // 协议层级    = SOL_SOCKET
    int    cmsg_type;    // 消息类型    = SCM_RIGHTS
    // 后面紧跟着数据区（int fd）
};
```

```
┌─────────────────────┬──────────────┬──────────┐
│ struct cmsghdr      │ int fd       │ padding  │
│ (cmsg_len/level/type)│              │ (对齐用)  │
└─────────────────────┴──────────────┴──────────┘
↑                     ↑
cmsg                  CMSG_DATA(cmsg)
```

| 宏 | 含义 | 类比 |
|---|---|---|
| `CMSG_LEN(sizeof(int))` | 控制消息实际使用的长度（头+数据） | 手机 20cm |
| `CMSG_SPACE(sizeof(int))` | 需要的缓冲区大小（含对齐填充） | 快递盒 24cm |
| `CMSG_FIRSTHDR(&msg)` | 取出第一条控制消息头 | 拆包裹，拿出第一样东西 |
| `CMSG_DATA(cmsg)` | 返回数据区起始地址（`unsigned char*`） | 指针指到数据部分 |

```cpp
// 发送方构造控制消息
char control[CMSG_SPACE(sizeof(int))]{};        // 准备缓冲区（含 padding）
msg.msg_control    = control;
msg.msg_controllen = sizeof(control);            // 缓冲区总大小

struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
cmsg->cmsg_level = SOL_SOCKET;
cmsg->cmsg_type  = SCM_RIGHTS;
cmsg->cmsg_len   = CMSG_LEN(sizeof(int));       // 实际数据长度

*reinterpret_cast<int *>(CMSG_DATA(cmsg)) = fd;  // 把 fd 写到数据区
sendmsg(sock, &msg, 0);
```

```cpp
// 接收方读取控制消息
char control[CMSG_SPACE(sizeof(int))]{};
msg.msg_control    = control;
msg.msg_controllen = sizeof(control);

recvmsg(sock, &msg, 0);

struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
if (cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type == SCM_RIGHTS) {
    int received_fd = *reinterpret_cast<int *>(CMSG_DATA(cmsg));
}
```

### 3.6 `CMSG_DATA` 为什么返回 `unsigned char*`

因为内核不知道你要传什么数据——可能是 `int fd`、`struct ucred`、或其他二进制数据。所以只能返回原始字节地址，你自己 `reinterpret_cast` 成需要的类型。

### 3.7 传 fd 的本质

> **不是简单地把 fd 数字复制过去。** 内核在接收进程里创建一个**新的 fd 号**，指向**同一个内核 `struct file` 对象**。

```
发送进程:  efd = 3  ──→  struct file (eventfd, counter, O_NONBLOCK)
接收进程:  efd = 5  ──→  same struct file
```

数字不同，能力（`O_NONBLOCK`）相同——因为 `O_NONBLOCK` 存在 `struct file->f_flags` 里，两个 fd 共享同一个 `struct file`。

### 3.8 为什么 `control` 要 `memset` 为零

```cpp
char control[CMSG_SPACE(sizeof(int))];
memset(control, 0, sizeof(control));  // ← 防御性清零
```

- **发送方**：避免 padding 字节残留栈垃圾，`cmsg_len` 精确描述了实际使用的长度
- **接收方**：`recvmsg` 不一定填满整个缓冲区，未填充区域可能是旧数据。清零后即使误读了也不会拿到假的控制消息

---

## 四、字符串与二进制约定

### 4.1 `strcpy` vs `memcpy`

```cpp
// strcpy: 复制到 '\0' 为止（含 '\0'）
strcpy(dst, "hello");     // 复制 6 字节: h e l l o \0

// memcpy: 复制指定 n 字节，不管内容
memcpy(dst, src, 64);     // 精确复制 64 字节
```

| | `strcpy` | `memcpy` |
|---|---|---|
| 停止条件 | 遇到 `'\0'` | 复制 n 字节后停止 |
| 适合 | C 字符串 | **任意二进制数据** |
| 需要 `'\0'`？ | 源必须有 | 不需要 |
| 本项目 | ❌ 会导致越界（`memset` 无 `'\0'` 的内存用 `strcpy`） | ✅ [b.cpp:89](b.cpp#L89) |

### 4.2 `write()` vs `cout`

```cpp
// write(fd, buf, n): 精确输出 n 字节，不需要 '\0'
write(1, "hello", 5);              // 输出 5 字节，无 '\0' 也正常
write(1, p->data + rd, wr - rd);   // 本项目中的用法 [a.cpp:91](a.cpp#L91)

// cout << char*: 当作 C 字符串，需要 '\0'，遇到 '\0' 才停
// cout.write(buf, n): 精确输出 n 字节，不需要 '\0'
```

| | 需要 `'\0'`？ | 决定长度的方式 |
|---|---|---|
| `cout << char*` | ✅ 需要 | 遇 `'\0'` 停止 |
| `cout.write(p, n)` | ❌ 不需要 | 第三个参数 `n` |
| `write(1, p, n)` | ❌ 不需要 | 第三个参数 `n` |
| `std::string(p, n)` | ❌ 不需要 | 构造时传入的长度 |

### 4.3 共享内存中存数据的两种方案

**方案 A：C 字符串（依赖 `'\0'`）**

```cpp
char shm[1024];
strcpy(shm, "hello");          // 写入
std::cout << shm;              // 读取——遇到 '\0' 停
```

优点简单，缺点不能含二进制、不能含 `'\0'` 字符。

**方案 B：长度 + 数据（本项目做法）**

```cpp
struct SharedData {
    atomic<int> offset_read;   // 读者位置
    atomic<int> offset_write;  // 写者位置（隐含长度 = write - read）
    char data[N];              // 数据
};
```

写入：

```cpp
memcpy(p->data + wr, temp, sizeof(temp));
p->offset_write.store(candidate, std::memory_order_release);
```

读取：

```cpp
write(1, p->data + rd, wr - rd);   // 用偏移差作为长度
```

> 工程上更推荐长度+数据方案：不依赖 `'\0'`，能处理任意二进制数据，长度由偏移量精确控制。

---

## 五、项目实战：所有概念如何组合

### 5.1 整体架构

```
b.out（生产者）                          a.out（消费者）
                                       
① shm_open(O_CREAT) 创建 /shm           
② ftruncate + mmap                     
③ socket(AF_UNIX) + connect            
                                       ④ socket + bind + listen + accept
⑤ eventfd(EFD_NONBLOCK) 创建事件通知器   
⑥ send_fd(sock, efd) ──────────────→  ⑦ recv_fd(conn_fd) 收到 efd
                                       ⑧ shm_open(O_RDWR) + mmap 映射 /shm
                                       ⑨ epoll_ctl(ADD, efd, EPOLLET)
⑩ 循环：memcpy shm → offset_write++     
     → write(efd, 1) ───────────────→  ⑪ read(efd) + 预读 shm
                                       ⑫ epoll_wait 等后续通知
                                           → write(1, shm data)
```

### 5.2 关键代码对应

**send_fd / recv_fd** — [b.cpp:16-39](b.cpp#L16-L39) / [a.cpp:16-41](a.cpp#L16-L41)：

```cpp
// b.out — 构造并发送
struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);    // 定位控制消息头
cmsg->cmsg_len   = CMSG_LEN(sizeof(int));       // 实际数据长度
cmsg->cmsg_level = SOL_SOCKET;                  // socket 层级
cmsg->cmsg_type  = SCM_RIGHTS;                  // "传文件描述符"
*reinterpret_cast<int *>(CMSG_DATA(cmsg)) = fd; // 把 eventfd 写入数据区
sendmsg(sock, &msg, 0);                         // 发送

// a.out — 接收并提取
recvmsg(sock, &msg, 0);
struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);     // 取出第一条控制消息
int fd = *reinterpret_cast<int *>(CMSG_DATA(cmsg)); // 拿到 eventfd
```

**共享内存指针** — 两边通过 `mmap` 映射同一块 `/shm` 后，`p` 就是"共享变量"：

```cpp
// b.cpp:53 — 写者
SharedData *p = static_cast<SharedData *>(
    mmap(nullptr, sizeof(data), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));

// a.cpp:83 — 读者（映射同一个 /shm，看到同一块内存）
SharedData *p = static_cast<SharedData *>(
    mmap(nullptr, sizeof(SharedData), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));
```

### 5.3 数据流完整路径

```
b.out                               a.out
─────                               ─────
memcpy(p->data + wr, temp, 64)      // 写到共享内存
p->offset_write.store(new_wr, ...)  // 公布写位置
write(efd, &val, 8)                 // 通知对方 ──→  read(efd, &val, 8)
                                                        wr = p->offset_write   // 读写位置
                                                        rd = p->offset_read    // 读读位置
                                                        write(1, p->data+rd, wr-rd)  // 输出到屏幕
                                                        p->offset_read.store(wr, ...) // 公布读位置
```

---

## 关键记忆卡片

```
┌──────────────────────────────────────────────────────────┐
│ 共享内存 = shm_open(拿 fd) → ftruncate(设大小) → mmap(映射)   │
│ 权限模型 = mode(门锁) + flags(钥匙) + prot(进门后能干嘛)        │
│                                                          │
│ UDS = socket(AF_UNIX) + bind(listen) + accept/connect     │
│ UDS ≠ TCP，但接口像，还能传 fd                              │
│                                                          │
│ sendmsg/recvmsg = msghdr(大箱) → cmsghdr(小箱) → fd(内容)  │
│ CMSG_SPACE = 缓冲区大小，CMSG_LEN = 实际数据长度              │
│ msg_iovlen = iovec 数量，不是字节数                         │
│                                                          │
│ memcpy ≠ strcpy：一个按 n 字节，一个遇到 \0 才停               │
│ write ≠ cout：一个按 n 字节，一个遇到 \0 才停                 │
│                                                          │
│ 核心：Linux I/O = 地址 + 长度，\0 只是约定不是本质              │
└──────────────────────────────────────────────────────────┘
```
