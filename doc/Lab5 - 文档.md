# Lab5 - 文档

> 曹烨 2021012167
>
> 源地址：https://apache-iotdb-project.feishu.cn/docx/VGdNdsyhioDBPfxWRzxcXjZ9n5d

[toc]

> 背景介绍：
>
>  **数据库的故障恢复**
>
> 数据库在运行期间，不可避免地会发生各种异常和故障。当数据库所在的机器发生了系统故障或者数据库自身出现问题，导致数据库进程意外退出后，原本存储在数据库进程的内存数据就会丢失。如果此时数据库的事务尚未提交，或者事务修改的数据没有写回磁盘，就会导致数据不一致的问题，从而影响数据库的正确性。
>
> 具体来说，当数据库因为系统故障而异常退出后，往往会影响事务的**原子性**和**持久性**。
>
> 对于原子性来说，如果事务在正常运行期间把自己的数据修改写回了磁盘，但是还没来得及提交事务（即事务未完成）就发生了系统故障，那么磁盘的这部分数据就是错误的，需要在重启时进行**撤销（undo）**，从而返回到这个事务执行前的状态；但如果事务在正常运行期间不把脏数据写回磁盘，然后没来得及提交就崩溃了，那么重启时不需要做任何事，因为磁盘本身就是原始的数据，自动保证了原子性。
>
> 对于持久性来说，如果事务在完成后进行提交的瞬间就把自己修改的脏数据都写回磁盘，那么即使后面发生了系统故障，重启时也不需要对该事务做任何事，因为它的改动已经持久化到磁盘了，自动保证了持久性；但是实际实现中，如果每次事务提交都进行脏页刷盘，这个磁盘IO的开销很大，因此往往采用将缓存的方式，等待提交的事务数量达到某个阈值再进行批量化刷盘，即延迟写回，但这就导致系统故障时，某些提交的事务所做的更改仅保留在内存中，没有持久化到磁盘，因此在重启恢复时需要进行**重做（redo）**，从而恢复到事务完成后的状态。
>
> 言简意赅，**它的核心逻辑在于，当故障发生后我们进行恢复时，对于未完成的事务如果刷了盘，需要回滚回去；对于完成的事务但还没有刷盘，那就需要根据日志重做一遍。**
>
> 因此刷盘逻辑和刷盘时刻的选择，对原子性和持久性的影响很大，不同的刷盘时刻需要我们设计与之对应的故障恢复机制，以整个数据库系统的数据正确。

## 一、Lab目标

根据 **[Lab5 PPT](https://kdocs.cn/l/capxeDXY4itM)** 的介绍，数据库日志从功能上分为 undo 日志和 redo 日志，而本次 lab 希望同学们实现基于 Redo 日志的故障恢复算法，**只实现Redo 日志，不使用Undo 日志。**

它的特点是“No-Streal、No-Force”，不允许事务在执行过程中刷盘，虽然宕机后内存数据会直接丢失但不影响磁盘数据的正确性，从而直接保证了原子性；事务提交后的数据由于也允许不立即刷盘，因此存在丢失的风险，需要使用 redo 日志来保证持久性，在恢复时进行重做。

具体来说，基于 Redo 日志的故障恢复算法，就是在数据库系统正常运行期间，将已提交（或者已中止）事务的日志先写入日志文件，作为安全保障和备份；在故障恢复时，则需要遍历日志文件，需要找到所有已经提交的事务，重做这些事务的各个数据操作。

Redo日志的优点在于避免了每次事务提交时都要即刻把内存的脏数据刷盘，减少了磁盘IO的频繁调用，提升了响应速度。

## 二、Lab条件

本次 lab 的故障恢复算法是建立在 mvcc 事务模型上的，因此需要同学们像 lab4 一样先开启mvcc。

### 如何开启MVCC

在启动server时，增加 `-t mvcc` 选项来开启MVCC，假设当前目录是build：

```cmd
./bin/server -f ../config/serverConfig.ini -p 6789 -t mvcc
```

### 如何运行MVCC

开多个terminal分别输入`./client -p 6789`，使得多个客户端分别连接到server服务端。

如果直接在client中输入SQL语句，那么每个SQL在server中都是一个单独的事务，会自行获取事务ID，并在返回到client之前直接提交。但这种方式让同学们对事务无感，因此需要同学们在client显式输入`begin`、`commit`和`rollback`语句。

具体来说，每个客户端执行`begin`语句后，会申请一个事务ID并开启新事务，同时TDB会从单一语句提交模式切换成多语句提交模式，即执行后续输入的多条SQL，并返回给client，但是server不提交。只有当在客户端输入`commit`和`rollback`后，才会将这一批操作一并进行提交或者回滚。

**操作示例：**

```Shell
TDB > begin
SUCCESS

TDB > insert into t_mvcc values(1, 1, 'a');
SUCCESS

TDB > select * from t_mvcc
  id |  age | name
   1 |    1 |    a
Cost time: 495333 ns

TDB > commit
SUCCESS

-----------------------------------------------------------------------

TDB > begin
SUCCESS

TDB > show tables;
Tables_in_SYS
t_mvcc

TDB > select * from t_mvcc
  id |  age | name
Cost time: 1868583 ns

TDB > commit
SUCCESS
```

**本次 lab5 会依赖 lab1 和 lab4 的正确实现。**因此同学们在调试自己的程序时，如果发现 bug 可以检查一下是不是 lab1 或者 lab4 的实现有问题。

### Redo日志文件

Redo日志文件名字叫做 redo.log，存放在tdb/db/sys目录下，和数据文件、元数据文件、索引文件在一起。每次TDB Server启动时，都会在初始化阶段读取redo.log文件的内容，如果该文件不存在或者内容为空，则跳过故障恢复阶段；如果存在内容，则会按照规定的格式一条条读取日志项，并将对应的操作重新执行一遍。

## 三、Lab知识

### 服务端运行过程

开启了mvcc模式的TDB Server在正常运行时，如果有事务数据产生，就会生成日志，每一次操作对应一条日志项(LogEntry)。日志项记录了当前操作的内容，比如插入一条数据、删除一条数据、事务开始、事务提交和事务回滚。具体生成日志项的时机和位置如下所示：

```C++
=> 在调用MvccTrx::insert_record()时，
    将插入记录的日志项写入LogBuffer
    
=> 在调用MvccTrx::delete_record()时，
    将删除记录的日志项写入LogBuffer
    
=> 在调用MvccTrx::start_if_need()时，
    将事务开始的日志项写入LogBuffer
    
=> 在调用MvccTrx::commit()时，
    将事务提交的日志项写入LogBuffer
    
=> 在调用MvccTrx::rollback()时，
    将事务回滚的日志项写入LogBuffer
```

​        运行时生成的日志项会先保存在内存的缓冲区`LogBuffer`，当事务提交时，会将当前的事务关联的日志项都刷写到磁盘的redo.log文件中，以保证数据不丢失（随着时间推移，会看到redo.log文件的大小在慢慢增加）。 刷新日志到磁盘时，并没有开启单独的线程，而是直接调用刷盘函数`LogManager::sync`。由于事务是并发运行的，会存在于多个线程中，因此`LogBuffer::flush_buffer`做了简单粗暴的加锁控制，一次只有一个线程在刷日志。

### 服务端启动过程

#### **启动时的初始化工作**

整体TDB Server的入口是main函数（参考 main@src/server/main.cpp），因此我们从它开始，介绍服务端的启动流程。

- 解析命令行参数：parse_parameter@src/server/main.cpp
- 加载配置文件：Ini::load@deps/common/conf/ini.cpp
- 初始化系统日志（注意不是redo日志！）：init_log@src/server/common_util/init.cpp
- 初始化上下文：init_global_objects@@src/server/common_util/init.cpp
	- 为database创建目录
	- 将所有table的元数据加载到内存
	- 读取redo.log文件，恢复上次异常退出的数据（如果发生系统故障的话）
- 初始化网络服务：init_server@src/server/main.cpp
- 启动网络服务：Server::serve@src/server/net/server.cpp

server启动后，就会一直监听6789端口，等待client发送SQL请求，然后解析SQL并执行，最后将结果返回给client。

#### **启动时恢复逻辑的调用栈**

建议同学们在下面这些函数中打断点进行调试，以观察恢复逻辑的执行过程。

```C++
=> main()
  => init()
    => init_global_objects()
      => DefaultHandler::init()
        => DefaultHandler::open_db()
          => Db::init()
            => Db::recover()
              => LogManager::recover()
/* 函数位置：src/server/storage_engine/schema/database.cpp */

// name="sys", dbpath="tdb/db/sys"
RC Db::init(const char *name, const char *dbpath)
{
  RC rc = RC::SUCCESS;
  log_manager_.reset(new LogManager());
  rc = log_manager_->init(dbpath);
  rc = open_all_tables();
  rc = recover();
  return rc;
}
```

### 服务端恢复过程

#### **LogManager::recover()**

TDB Server的恢复功能主要由 LogManager::recover() 函数完成，因此下面会介绍它的主要逻辑，以供同学们参考和实现本次 lab 。

LogEntryIterator 类是 Redo 日志文件的读取工具，每次读取一条日志项。同学们需要使用这个类读取redo.log，从头开始遍历日志文件，直到读到文件尾。对于读出来的每一条日志项，要根据日志项的不同类型执行不同的恢复操作，如下所示：

- 如果是事务开始的日志项，则要根据日志项中的事务id开启一个事务；【提示：调用Trx *MvccTrxManager::create_trx(int32_t trx_id) 】
- 如果是事务提交或者事务回滚的日志项，则要根据日志项中的事务id找到它所属的事务对象，然后由该事务对象进行重做操作；【提示：调用RC MvccTrx::redo(Db *db, const LogEntry &log_entry) 】
- 如果是插入数据或者删除数据的日志项，则要根据日志项中的事务id找到它所属的事务对象，然后由该事务对象进行重做操作；【提示：调用RC MvccTrx::redo(Db *db, const LogEntry &log_entry)】

注意：在TDB Server正常运行过程中，事务在提交时除了把 commit 日志项写入 LogBuffer，还要调用 `LogManager::sync()` 函数把 LogBuffer 从内存刷到磁盘上的 redo.log 文件。但在把日志刷盘时，LogBuffer中存储的不仅是当前要提交事务的日志项，很可能还缓存了其他活跃事务执行过程中的日志项。这样导致的现象就是，在故障崩溃后进行恢复时，LogEntryIterator 读取到的不仅有已提交的事务的日志，还有一些未提交的事务的日志，但后者不应该被重做，因此同学们要格外思考一下这个问题该如何解决。

#### **MvccTrx::redo(Db \*db, const LogEntry &log_entry)**

对于redo.log的每条日志项，必须要交给它所属的事务对象来重做才有效，而上面的 LogManager::recover() 函数就是负责这个“小蝌蚪找妈妈”的任务。每个日志项找到自己的事务“妈妈”后，就由该事务完成具体的重做操作，这就是 redo() 函数的作用。

由于不同的日志项类型，则需要重做的逻辑不同，因此需要先一一识别，具体如下：

- 如果是插入数据的日志项，需要根据日志项还原出数据记录和它所属的 Table 对象，然后调用`Table::recover_insert_record(Record &record)`重新插入这条数据（PS：这个接口和前几次lab中涉及的`Table::insert_record(Record &record)`不同，因为前者具有幂等性，根据 page id 和 slot id 直接到物理位置去覆盖，因此无论调用多少次都只有一条数据，而后者每被调用一次就添加一条新记录）
- 如果是删除数据的日志项，则需要根据日志项还原出数据记录和它所属的 Table 对象，然后调用`Table::visit_record(const RID &rid, bool readonly, std::function<void(Record &)> visitor)`函数找到要删除的数据记录，并按照lab4中的版本号与可见性规则，通过修改事务字段实现逻辑删除。
- 如果是事务提交的日志项，则需要获取日志项中的commit_id，并调用`MvccTrx::commit_with_trx_id(int32_t commit_xid)`完成事务的重新提交。
- 如果是事务回滚的日志项，则需要调用rollback()进行回滚。

## 四、Lab任务

### 模拟MVCC的可见性

#### 任务描述

### 任务描述

我们在Lab5的PPT中了解到TDB的故障恢复机制。目前的TDB代码已经支持了Redo日志的缓存与文件读写，也能够保障 TDB Server 在正常运行时产生重做日志。同学们可以阅读 recover 目录下面的代码文件了解这些工具的用法和 Redo日志文件格式。

而在本次lab的任务则是在此基础上，结合lab4的mvcc事务模型，实现故障恢复的重做逻辑，做到系统崩溃后重启仍能找到原本提交的数据。**（再次强调：本次 lab 会依赖 lab4 和 lab1 的正确性）**

**需要在代码中至少实现以下方法：**代码中至少实现以下方法：**

```C++
// src/server/storage_engine/recover/log_manager.cpp
RC LogManager::recover(Db *db)

// src/server/storage_engine/transaction/mvcc_trx.cpp
RC MvccTrx::redo(Db *db, const LogEntry &log_entry)

// src/server/storage_engine/transaction/mvcc_trx.cpp
RC MvccTrx::insert_record(Table *table, Record &record);
RC MvccTrx::delete_record(Table *table, Record &record);
```

**温馨提示：**

>   关于前两个函数的大致思路，在本文第三节的“**服务端恢复过程**”部分进行了详细介绍；关于后两个函数，需要在lab4基础上补充追加日志的功能。相信同学们按照这个思路可以完成本次lab，当然如果同学们有更好的实现思路也非常欢迎，文档仅是参考~

#### 任务流程

1. 创建好 t_redo 这个表，然后开启四个事务，依次执行一些数据的插入和删除操作。
2. 手动将 TDB Server 进程杀掉，模拟系统崩溃的故障。
3. 若重启后的输出均与预期一致，说明lab功能实现正确；否则需要继续修改

#### 模拟步骤

|      | **初始化阶段：创建t_redo数据表**                             |                                                              |                                   |                                   |
| ---- | ------------------------------------------------------------ | ------------------------------------------------------------ | --------------------------------- | --------------------------------- |
|      | 启动server，开启mvcc模式`./bin/server -f ../config/serverConfig.ini -p 6789 ``-t mvcc`开启一个client 执行`create table t_redo(id int, age int);`创建初始表t_redo，然后退出。 |                                                              |                                   |                                   |
|      | **第一阶段：数据库系统正常运行，开启4个事务**                |                                                              |                                   |                                   |
|      | 开启四个client连接到server，按照下面的时间顺序，依次执行对应的SQL操作。 |                                                              |                                   |                                   |
|      | transaction1                                                 | transaction2                                                 | transaction3                      | transaction4                      |
| t1   | begin;                                                       |                                                              |                                   |                                   |
| t2   | insert into t_redo values(0, 0);                             |                                                              |                                   |                                   |
| t3   | insert into t_redo values(1, 10);                            |                                                              |                                   |                                   |
| t4   | commit;                                                      |                                                              |                                   |                                   |
| t5   |                                                              | begin;                                                       |                                   |                                   |
| t6   |                                                              | insert into t_redo values(2, 20);                            |                                   |                                   |
| t7   |                                                              |                                                              | begin;                            |                                   |
| t8   |                                                              | commit;                                                      |                                   |                                   |
| t9   |                                                              |                                                              | insert into t_redo values(3, 30); |                                   |
| t10  |                                                              |                                                              |                                   | begin;                            |
| t11  |                                                              |                                                              |                                   | insert into t_redo values(4, 40); |
| t12  |                                                              |                                                              |                                   | delete from t_redo where id=0;    |
| t13  |                                                              |                                                              | insert into t_redo values(5, 50); |                                   |
|      | **第二阶段：系统发生故障，TDB Server异常退出**               |                                                              |                                   |                                   |
|      | 手动杀死server进程：1.在terminal输入`lsof -i: 6789`获取server的进程号；2.在terminal输入`kill -9 server进程号`，从而强制server进程退出。 |                                                              |                                   |                                   |
|      | **第三阶段：重新启动TDB Server**                             |                                                              |                                   |                                   |
|      | 使用client进行连接server，在client中输入`select * from t_redo;`查看系统崩溃前写入的数据。 |                                                              |                                   |                                   |
|      | 若输出结果为：TDB > select * from t_redo; id \| age  0 \|   0  1 \|  10  2 \|  20则表明lab实现正确。 | 若输出结果为：TDB > select * from t_redo; id \| age则表明lab实现错误。 |                                   |                                   |

## 五、Lab提交

### SQL验证

同学们可以启动TDB的server后，按照表格的流程输入SQL进行模拟，然后强制退出server进程，再重新启动，与预期结果进行对比，判断自己的实现是否正确。

### 代码提交

```Java
include/storage_engine/transaction/mvcc_trx.h
storage_engine/transaction/mvcc_trx.cpp

include/storage_engine/recover/log_manager.h
storage_engine/recover/log_manager.cpp

include/storage_engine/buffer/frame_manager.h
storage_engine/buffer/frame_manager.cpp

include/storage_engine/buffer/buffer_pool.h
storage_engine/buffer/buffer_pool.cpp
```

## 六、初始代码

```hpp

```

```cpp
// storage_engine/transaction/mvcc_trx.cpp

#include "include/storage_engine/transaction/mvcc_trx.h"
#include "include/storage_engine/schema/database.h"

using namespace std;

MvccTrxManager::~MvccTrxManager()
{
  vector<Trx *> tmp_trxes;
  tmp_trxes.swap(trxes_);
  for (Trx *trx : tmp_trxes) {
    delete trx;
  }
}

RC MvccTrxManager::init()
{
  fields_ = vector<FieldMeta>{
      FieldMeta("__trx_xid_begin", AttrType::INTS, 0/*attr_offset*/, 4/*attr_len*/, false/*visible*/),
      FieldMeta("__trx_xid_end",   AttrType::INTS, 4/*attr_offset*/, 4/*attr_len*/, false/*visible*/)
  };
  LOG_INFO("init mvcc trx kit done.");
  return RC::SUCCESS;
}

const vector<FieldMeta> *MvccTrxManager::trx_fields() const
{
  return &fields_;
}

Trx *MvccTrxManager::create_trx(LogManager *log_manager)
{
  Trx *trx = new MvccTrx(*this, log_manager);
  if (trx != nullptr) {
    lock_.lock();
    trxes_.push_back(trx);
    lock_.unlock();
  }
  return trx;
}

Trx *MvccTrxManager::create_trx(int32_t trx_id)
{
  Trx *trx = new MvccTrx(*this, trx_id);
  if (trx != nullptr) {
    lock_.lock();
    trxes_.push_back(trx);
    if (current_trx_id_ < trx_id) {
      current_trx_id_ = trx_id;
    }
    lock_.unlock();
  }
  return trx;
}

void MvccTrxManager::destroy_trx(Trx *trx)
{
  lock_.lock();
  for (auto iter = trxes_.begin(), itend = trxes_.end(); iter != itend; ++iter) {
    if (*iter == trx) {
      trxes_.erase(iter);
      break;
    }
  }
  lock_.unlock();
  delete trx;
}

Trx *MvccTrxManager::find_trx(int32_t trx_id)
{
  lock_.lock();
  for (Trx *trx : trxes_) {
    if (trx->id() == trx_id) {
      lock_.unlock();
      return trx;
    }
  }
  lock_.unlock();
  return nullptr;
}

void MvccTrxManager::all_trxes(std::vector<Trx *> &trxes)
{
  lock_.lock();
  trxes = trxes_;
  lock_.unlock();
}

int32_t MvccTrxManager::next_trx_id()
{
  return ++current_trx_id_;
}

int32_t MvccTrxManager::max_trx_id() const
{
  return numeric_limits<int32_t>::max();
}

void MvccTrxManager::update_trx_id(int32_t trx_id)
{
  int32_t old_trx_id = current_trx_id_;
  while (old_trx_id < trx_id && !current_trx_id_.compare_exchange_weak(old_trx_id, trx_id));
}

////////////////////////////////////////////////////////////////////////////////
MvccTrx::MvccTrx(MvccTrxManager &kit, LogManager *log_manager) : trx_kit_(kit), log_manager_(log_manager)
{}

MvccTrx::MvccTrx(MvccTrxManager &kit, int32_t trx_id) : trx_kit_(kit), trx_id_(trx_id)
{
  started_ = true;
  recovering_ = true;
}

RC MvccTrx::insert_record(Table *table, Record &record)
{
  RC rc = RC::SUCCESS;
  // TODO [Lab4] 需要同学们补充代码实现记录的插入，相关提示见文档

  pair<OperationSet::iterator, bool> ret = operations_.insert(Operation(Operation::Type::INSERT, table, record.rid()));
  if (!ret.second) {
    rc = RC::INTERNAL;
    LOG_WARN("failed to insert operation(insertion) into operation set: duplicate");
  }
  return rc;
}

RC MvccTrx::delete_record(Table *table, Record &record)
{
  RC rc = RC::SUCCESS;
  // TODO [Lab4] 需要同学们补充代码实现逻辑上的删除，相关提示见文档

  operations_.insert(Operation(Operation::Type::DELETE, table, record.rid()));
  return rc;
}

/**
   * @brief 当访问到某条数据时，使用此函数来判断是否可见，或者是否有访问冲突
   * @param table    要访问的数据属于哪张表
   * @param record   要访问哪条数据
   * @param readonly 是否只读访问
   * @return RC      - SUCCESS 成功
   *                 - RECORD_INVISIBLE 此数据对当前事务不可见，应该跳过
   *                 - LOCKED_CONCURRENCY_CONFLICT 与其它事务有冲突
 */
RC MvccTrx::visit_record(Table *table, Record &record, bool readonly)
{
  RC rc = RC::SUCCESS;
  // TODO [Lab4] 需要同学们补充代码实现记录是否可见的判断，相关提示见文档

  return rc;
}

RC MvccTrx::start_if_need()
{
  if (!started_) {
    ASSERT(operations_.empty(), "try to start a new trx while operations is not empty");
    trx_id_ = trx_kit_.next_trx_id();
    LOG_DEBUG("current thread change to new trx with %d", trx_id_);
    RC rc = log_manager_->append_begin_trx_log(trx_id_);
    ASSERT(rc == RC::SUCCESS, "failed to append log to clog. rc=%s", strrc(rc));
    started_ = true;
  }
  return RC::SUCCESS;
}

RC MvccTrx::commit()
{
  int32_t commit_id = trx_kit_.next_trx_id();
  return commit_with_trx_id(commit_id);
}

RC MvccTrx::commit_with_trx_id(int32_t commit_xid)
{
  RC rc = RC::SUCCESS;
  started_ = false;

  if (recovering_) {
    // 在事务恢复时，更新当前事务 id 避免被后续事务重用
    trx_kit_.update_trx_id(commit_xid);
  }

  for (const Operation &operation : operations_) {
    switch (operation.type()) {
      case Operation::Type::INSERT: {
        RID rid(operation.page_num(), operation.slot_num());
        Table *table = operation.table();
        Field begin_xid_field, end_xid_field;
        trx_fields(table, begin_xid_field, end_xid_field);
        auto record_updater = [ this, &begin_xid_field, commit_xid](Record &record) {
          LOG_DEBUG("before commit insert record. trx id=%d, begin xid=%d, commit xid=%d, lbt=%s", trx_id_, begin_xid_field.get_int(record), commit_xid, lbt());
          ASSERT(begin_xid_field.get_int(record) == -this->trx_id_, "got an invalid record while committing. begin xid=%d, this trx id=%d", begin_xid_field.get_int(record), trx_id_);
          begin_xid_field.set_int(record, commit_xid);
        };
        rc = operation.table()->visit_record(rid, false/*readonly*/, record_updater);
        ASSERT(rc == RC::SUCCESS, "failed to get record while committing. rid=%s, rc=%s", rid.to_string().c_str(), strrc(rc));
      } break;

      case Operation::Type::DELETE: {
        Table *table = operation.table();
        RID rid(operation.page_num(), operation.slot_num());
        Field begin_xid_field, end_xid_field;
        trx_fields(table, begin_xid_field, end_xid_field);
        auto record_updater = [this, &end_xid_field, commit_xid](Record &record) {
          (void)this;
          ASSERT(end_xid_field.get_int(record) == -trx_id_, "got an invalid record while committing. end xid=%d, this trx id=%d", end_xid_field.get_int(record), trx_id_);
          end_xid_field.set_int(record, commit_xid);
        };
        rc = operation.table()->visit_record(rid, false/*readonly*/, record_updater);
        ASSERT(rc == RC::SUCCESS, "failed to get record while committing. rid=%s, rc=%s", rid.to_string().c_str(), strrc(rc));
      } break;

      default: {
        ASSERT(false, "unsupported operation. type=%d", static_cast<int>(operation.type()));
      }
    }
  }

  operations_.clear();

  if (!recovering_) {
    rc = log_manager_->append_commit_trx_log(trx_id_, commit_xid);
  }
  LOG_TRACE("append trx commit log. trx id=%d, commit_xid=%d, rc=%s", trx_id_, commit_xid, strrc(rc));

  return rc;
}

RC MvccTrx::rollback()
{
  RC rc = RC::SUCCESS;
  started_ = false;

  for (const Operation &operation : operations_) {
    switch (operation.type()) {
      case Operation::Type::INSERT: {
        RID rid(operation.page_num(), operation.slot_num());
        Record record;
        Table *table = operation.table();
        rc = table->get_record(rid, record);
        ASSERT(rc == RC::SUCCESS, "failed to get record while rollback. rid=%s, rc=%s", rid.to_string().c_str(), strrc(rc));
        rc = table->delete_record(record);
        ASSERT(rc == RC::SUCCESS, "failed to delete record while rollback. rid=%s, rc=%s", rid.to_string().c_str(), strrc(rc));
      } break;

      case Operation::Type::DELETE: {
        Table *table = operation.table();
        RID rid(operation.page_num(), operation.slot_num());
        ASSERT(rc == RC::SUCCESS, "failed to get record while rollback. rid=%s, rc=%s", rid.to_string().c_str(), strrc(rc));
        Field begin_xid_field, end_xid_field;
        trx_fields(table, begin_xid_field, end_xid_field);
        auto record_updater = [this, &end_xid_field](Record &record) {
          ASSERT(end_xid_field.get_int(record) == -trx_id_, "got an invalid record while rollback. end xid=%d, this trx id=%d", end_xid_field.get_int(record), trx_id_);
          end_xid_field.set_int(record, trx_kit_.max_trx_id());
        };
        rc = table->visit_record(rid, false/*readonly*/, record_updater);
        ASSERT(rc == RC::SUCCESS, "failed to get record while committing. rid=%s, rc=%s", rid.to_string().c_str(), strrc(rc));
      } break;

      default: {
        ASSERT(false, "unsupported operation. type=%d", static_cast<int>(operation.type()));
      }
    }
  }

  operations_.clear();

  if (!recovering_) {
    rc = log_manager_->append_rollback_trx_log(trx_id_);
  }
  LOG_TRACE("append trx rollback log. trx id=%d, rc=%s", trx_id_, strrc(rc));
  return rc;
}

/**
 * @brief 获取指定表上的与版本号相关的字段
 * @param table 指定的表
 * @param begin_xid_field 返回处理begin_xid的字段
 * @param end_xid_field   返回处理end_xid的字段
 */
void MvccTrx::trx_fields(Table *table, Field &begin_xid_field, Field &end_xid_field) const
{
  const TableMeta &table_meta = table->table_meta();
  const std::pair<const FieldMeta *, int> trx_fields = table_meta.trx_fields();
  ASSERT(trx_fields.second >= 2, "invalid trx fields number. %d", trx_fields.second);

  begin_xid_field.set_table(table);
  begin_xid_field.set_field(&trx_fields.first[0]);
  end_xid_field.set_table(table);
  end_xid_field.set_field(&trx_fields.first[1]);
}

// TODO [Lab5] 需要同学们补充代码，相关提示见文档
RC MvccTrx::redo(Db *db, const LogEntry &log_entry)
{

  switch (log_entry.log_type()) {
    case LogEntryType::INSERT: {
      Table *table = nullptr;
      const RecordEntry &record_entry = log_entry.record_entry();

      // TODO [Lab5] 需要同学们补充代码，相关提示见文档

      operations_.insert(Operation(Operation::Type::INSERT, table, record_entry.rid_));
    } break;

    case LogEntryType::DELETE: {
      Table *table = nullptr;
      const RecordEntry &record_entry = log_entry.record_entry();

      // TODO [Lab5] 需要同学们补充代码，相关提示见文档

      operations_.insert(Operation(Operation::Type::DELETE, table, record_entry.rid_));
    } break;

    case LogEntryType::MTR_COMMIT: {

      // TODO [Lab5] 需要同学们补充代码，相关提示见文档

    } break;

    case LogEntryType::MTR_ROLLBACK: {

      // TODO [Lab5] 需要同学们补充代码，相关提示见文档

    } break;

    default: {
      ASSERT(false, "unsupported redo log. log entry=%s", log_entry.to_string().c_str());
      return RC::INTERNAL;
    } break;
  }

  return RC::SUCCESS;
}
```

