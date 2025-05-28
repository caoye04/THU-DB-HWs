帮我分析一下下面这次作业要求。

并带入我的视角，教我完成这次作业，并且成功验证其正确性。

我希望你能给出比较完整的代码！

我给出了要修改的代码，和可以参考的代码

# Lab5 - 文档

> 曹烨 2021012167
>

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

根据 Lab5 PPT 的介绍，数据库日志从功能上分为 undo 日志和 redo 日志，而本次 lab 希望同学们实现基于 Redo 日志的故障恢复算法，**只实现Redo 日志，不使用Undo 日志。**

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

#### 启动时的初始化工作

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

#### 启动时恢复逻辑的调用栈

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

#### LogManager::recover()

TDB Server的恢复功能主要由 LogManager::recover() 函数完成，因此下面会介绍它的主要逻辑，以供同学们参考和实现本次 lab 。

LogEntryIterator 类是 Redo 日志文件的读取工具，每次读取一条日志项。同学们需要使用这个类读取redo.log，从头开始遍历日志文件，直到读到文件尾。对于读出来的每一条日志项，要根据日志项的不同类型执行不同的恢复操作，如下所示：

- 如果是事务开始的日志项，则要根据日志项中的事务id开启一个事务；【提示：调用Trx *MvccTrxManager::create_trx(int32_t trx_id) 】
- 如果是事务提交或者事务回滚的日志项，则要根据日志项中的事务id找到它所属的事务对象，然后由该事务对象进行重做操作；【提示：调用RC MvccTrx::redo(Db *db, const LogEntry &log_entry) 】
- 如果是插入数据或者删除数据的日志项，则要根据日志项中的事务id找到它所属的事务对象，然后由该事务对象进行重做操作；【提示：调用RC MvccTrx::redo(Db *db, const LogEntry &log_entry)】

注意：在TDB Server正常运行过程中，事务在提交时除了把 commit 日志项写入 LogBuffer，还要调用 `LogManager::sync()` 函数把 LogBuffer 从内存刷到磁盘上的 redo.log 文件。但在把日志刷盘时，LogBuffer中存储的不仅是当前要提交事务的日志项，很可能还缓存了其他活跃事务执行过程中的日志项。这样导致的现象就是，在故障崩溃后进行恢复时，LogEntryIterator 读取到的不仅有已提交的事务的日志，还有一些未提交的事务的日志，但后者不应该被重做，因此同学们要格外思考一下这个问题该如何解决。

#### MvccTrx::redo(Db \*db, const LogEntry &log_entry)

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

### mvcc_trx.cpp

```cpp
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

  rc = start_if_need();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to start transaction");
    return rc;
  }

  // 获取记录的版本字段，初始化信息
  Field begin_xid_field, end_xid_field;
  trx_fields(table, begin_xid_field, end_xid_field);
  
  begin_xid_field.set_int(record, -trx_id_);
  end_xid_field.set_int(record, MAX_TRX_ID);
  
  // 插入记录到表中
  rc = table->insert_record(record);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to insert record into table. rc=%s", strrc(rc));
    return rc;
  }

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

  rc = start_if_need();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to start transaction");
    return rc;
  }
  
  // 获取字段
  Field begin_xid_field, end_xid_field;
  trx_fields(table, begin_xid_field, end_xid_field);
  
  // 判断
  int32_t begin_xid = begin_xid_field.get_int(record);
  int32_t end_xid = end_xid_field.get_int(record);
  
  // 检查记录
  if (end_xid < 0 && -end_xid != trx_id_) {
    return RC::LOCKED_CONCURRENCY_CONFLICT;
  }
  
  if (begin_xid < 0 && -begin_xid != trx_id_) {
    return RC::LOCKED_CONCURRENCY_CONFLICT;
  }
  
  if (end_xid != MAX_TRX_ID && end_xid <= trx_id_) {
    return RC::RECORD_INVISIBLE;
  }
  
  if (begin_xid > trx_id_) {
    return RC::RECORD_INVISIBLE;
  }
  
  if (begin_xid < 0 && -begin_xid == trx_id_) {
    rc = table->delete_record(record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to delete record from table. rc=%s", strrc(rc));
      return rc;
    }
    
    operations_.erase(Operation(Operation::Type::INSERT, table, record.rid()));
    return rc;
  }
  
  auto record_updater = [this, &end_xid_field](Record &record) {
    end_xid_field.set_int(record, -trx_id_);
  };
  
  rc = table->visit_record(record.rid(), false/*readonly*/, record_updater);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to update record end_xid. rid=%s, rc=%s", record.rid().to_string().c_str(), strrc(rc));
    return rc;
  }

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
  Field begin_xid_field, end_xid_field;
  trx_fields(table, begin_xid_field, end_xid_field);
  
  int32_t begin_xid = begin_xid_field.get_int(record);
  int32_t end_xid = end_xid_field.get_int(record);
  
  // LOG_DEBUG("visit_record: trx_id=%d, begin_xid=%d, end_xid=%d", 
  //           trx_id_, begin_xid, end_xid);
  
  if (!readonly) {
    if ((begin_xid < 0 && -begin_xid != trx_id_) || 
        (end_xid < 0 && -end_xid != trx_id_)) {
      // LOG_DEBUG("Locked concurrency conflict detected");
      return RC::LOCKED_CONCURRENCY_CONFLICT;
    }
  }
  
  // 判断
  
  // 情况1：记录是当前事务创建的（未提交）
  if (begin_xid < 0 && -begin_xid == trx_id_) {
    // LOG_DEBUG("Record visible: created by current transaction");
    return RC::SUCCESS; 
  }
  
  // 情况2：记录是其他事务创建的（未提交)
  if (begin_xid < 0 && -begin_xid != trx_id_) {
    // LOG_DEBUG("Record invisible: created by another transaction but not committed");
    return RC::RECORD_INVISIBLE;  
  }
  
  // 情况3：记录是当前事务删除的（未提交）
  if (end_xid < 0 && -end_xid == trx_id_) {
    // LOG_DEBUG("Record invisible: deleted by current transaction");
    return RC::RECORD_INVISIBLE;
  }
  
  // 情况4：记录在当前事务开始之后创建
  if (begin_xid > 0 && begin_xid > trx_id_) {
    // LOG_DEBUG("Record invisible: created after transaction started");
    return RC::RECORD_INVISIBLE;  
  }
  
  // 情况5：记录在当前事务开始之前删除
  if (end_xid > 0 && end_xid <= trx_id_) {
    // LOG_DEBUG("Record invisible: deleted before transaction started");
    return RC::RECORD_INVISIBLE; 
  }
  
  // 其他情况：记录可见
  LOG_DEBUG("Record visible: normal case");
  return RC::SUCCESS;
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

### log_manager.cpp

```cpp
#include "include/storage_engine/recover/log_manager.h"
#include "include/storage_engine/transaction/trx.h"

RC LogEntryIterator::init(LogFile &log_file)
{
  log_file_ = &log_file;
  return RC::SUCCESS;
}

RC LogEntryIterator::next()
{
  LogEntryHeader header;
  RC rc = log_file_->read(reinterpret_cast<char *>(&header), sizeof(header));
  if (rc != RC::SUCCESS) {
    if (log_file_->eof()) {
      return RC::RECORD_EOF;
    }
    LOG_WARN("failed to read log header. rc=%s", strrc(rc));
    return rc;
  }

  char *data = nullptr;
  int32_t entry_len = header.log_entry_len_;
  if (entry_len > 0) {
    data = new char[entry_len];
    rc = log_file_->read(data, entry_len);
    if (RC_FAIL(rc)) {
      LOG_WARN("failed to read log data. data size=%d, rc=%s", entry_len, strrc(rc));
      delete[] data;
      data = nullptr;
      return rc;
    }
  }

  if (log_entry_ != nullptr) {
    delete log_entry_;
    log_entry_ = nullptr;
  }
  log_entry_ = LogEntry::build(header, data);
  delete[] data;
  return rc;
}

bool LogEntryIterator::valid() const
{
  return log_entry_ != nullptr;
}

const LogEntry &LogEntryIterator::log_entry()
{
  return *log_entry_;
}

////////////////////////////////////////////////////////////////////////////////

LogManager::~LogManager()
{
  if (log_buffer_ != nullptr) {
    delete log_buffer_;
    log_buffer_ = nullptr;
  }

  if (log_file_ != nullptr) {
    delete log_file_;
    log_file_ = nullptr;
  }
}

RC LogManager::init(const char *path)
{
  log_buffer_ = new LogBuffer();
  log_file_   = new LogFile();
  return log_file_->init(path);
}

RC LogManager::append_begin_trx_log(int32_t trx_id)
{
  return append_log(LogEntry::build_mtr_entry(LogEntryType::MTR_BEGIN, trx_id));
}

RC LogManager::append_rollback_trx_log(int32_t trx_id)
{
  return append_log(LogEntry::build_mtr_entry(LogEntryType::MTR_ROLLBACK, trx_id));
}

RC LogManager::append_commit_trx_log(int32_t trx_id, int32_t commit_xid)
{
  RC rc = append_log(LogEntry::build_commit_entry(trx_id, commit_xid));
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to append trx commit log. trx id=%d, rc=%s", trx_id, strrc(rc));
    return rc;
  }
  rc = sync(); // 事务提交时需要把当前事务关联的日志项都写入到磁盘中，这样做是保证不丢数据
  return rc;
}

RC LogManager::append_record_log(LogEntryType type, int32_t trx_id, int32_t table_id, const RID &rid, int32_t data_len, int32_t data_offset, const char *data)
{
  LogEntry *log_entry = LogEntry::build_record_entry(type, trx_id, table_id, rid, data_len, data_offset, data);
  if (nullptr == log_entry) {
    LOG_WARN("failed to create log entry");
    return RC::NOMEM;
  }
  return append_log(log_entry);
}

RC LogManager::append_log(LogEntry *log_entry)
{
  if (nullptr == log_entry) {
    return RC::INVALID_ARGUMENT;
  }
  return log_buffer_->append_log_entry(log_entry);
}

RC LogManager::sync()
{
  return log_buffer_->flush_buffer(*log_file_);
}

// TODO [Lab5] 需要同学们补充代码，相关提示见文档
RC LogManager::recover(Db *db)
{
  TrxManager *trx_manager = GCTX.trx_manager_;
  ASSERT(trx_manager != nullptr, "cannot do recover that trx_manager is null");

  // TODO [Lab5] 需要同学们补充代码，相关提示见文档

  return RC::SUCCESS;
}


```

### include/storage_engine/recover/log_entry.h

```hpp
#pragma once

#include <cstdint>
#include <string>

#include "include/storage_engine/recorder/record.h"

enum class LogEntryType
{
  ERROR,
  MTR_BEGIN,
  MTR_COMMIT,
  MTR_ROLLBACK,
  INSERT,
  DELETE
};

const char* logentry_type_name(LogEntryType type);  // log entry type 转换成字符串
int32_t logentry_type_to_integer(LogEntryType type);  // log entry type 转换成数字
LogEntryType logentry_type_from_integer(int32_t value);  // 数字转换成 log entry type

/**
 * @brief LogEntry的头部信息，每条日志项都带有它。
 */
struct LogEntryHeader
{
  int32_t trx_id_ = -1;  // 该日志项所属事务的事务id
  int32_t type_ = logentry_type_to_integer(LogEntryType::ERROR);  // 日志项类型
  int32_t log_entry_len_ = 0;  // 日志项的长度，但不包含header长度

  bool operator==(const LogEntryHeader &other) const
  {
    return trx_id_ == other.trx_id_ && type_ == other.type_ && log_entry_len_ == other.log_entry_len_;
  }

  std::string to_string() const;
};

/**
 * @brief 提交语句对应的日志项
 */
struct CommitEntry
{
  int32_t commit_xid_ = -1;  // 事务提交时的事务号

  bool operator == (const CommitEntry &other) const
  {
    return this->commit_xid_ == other.commit_xid_;
  }

  std::string to_string() const;
};

/**
 * @brief 修改数据的日志项（比如插入、删除一条数据）
 */
struct RecordEntry
{
  int32_t          table_id_ = -1;    // 操作的表
  RID              rid_;              // 操作的哪条记录
  int32_t          data_len_ = 0;     // 记录的数据长度(因为header中也包含长度信息，这个长度可以不要)
  int32_t          data_offset_ = 0;  // 操作的数据在完整记录中的偏移量
  char *           data_ = nullptr;   // 具体的数据，可能没有任何数据

  ~RecordEntry();

  bool operator==(const RecordEntry &other) const
  {
    return table_id_ == other.table_id_ &&
           rid_ == other.rid_ &&
           data_len_ == other.data_len_ &&
           data_offset_ == other.data_offset_ &&
           0 == memcmp(data_, other.data_, data_len_);
  }

  std::string to_string() const;

  const static int32_t HEADER_SIZE;  // 指RecordEntry的头长度，即不包含data_的长度
};


/**
 * @brief 表示一条日志项
 */
class LogEntry
{
public:
  LogEntry() = default;  // 通常不需要直接调用这个函数来创建一条日志，而是调用 `build_xxx`创建对象。
  ~LogEntry() {}

  /**
   * @brief 创建一个事务相关的日志项
   * @details 除了MTR_COMMIT的日志
   * @param type 日志类型
   * @param trx_id 事务id
   */
  static LogEntry *build_mtr_entry(LogEntryType type, int32_t trx_id);

  /**
   * @brief 创建一个表示事务提交的日志项
   * @param trx_id 事务id
   * @param commit_xid 事务提交时的id
   */
  static LogEntry *build_commit_entry(int32_t trx_id, int32_t commit_xid);

  /**
   * @brief 创建一个表示修改数据的日志项
   * @param type 日志项类型
   * @param trx_id 事务id
   * @param table_id 操作的表
   * @param rid 操作的哪条记录
   * @param data_len 数据的长度
   * @param data_offset 修改数据在记录中的偏移量
   * @param data 具体的数据
   */
  static LogEntry *build_record_entry(LogEntryType type, int32_t trx_id, int32_t table_id, const RID &rid, int32_t data_len, int32_t data_offset, const char *data);

  /**
   * @brief 根据二进制数据创建日志项
   * @details 通常是从日志文件中读取数据，然后调用此函数创建日志项
   * @param header 日志头信息
   * @param data   读取的剩余数据信息，长度是header.log_entry_len_
   */
  static LogEntry *build(const LogEntryHeader &header, char *data);

  int32_t  trx_id() const { return entry_header_.trx_id_; }
  LogEntryType log_type() const  { return logentry_type_from_integer(entry_header_.type_); }
  int32_t  log_entry_len() const { return entry_header_.log_entry_len_; }

  LogEntryHeader &header() { return entry_header_; }
  CommitEntry &commit_entry() { return commit_entry_; }
  RecordEntry &record_entry() { return record_entry_; }
  const LogEntryHeader &header() const { return entry_header_; }
  const CommitEntry &commit_entry() const { return commit_entry_; }
  const RecordEntry &record_entry() const { return record_entry_; }

  std::string to_string() const;

 protected:
  LogEntryHeader  entry_header_;  // 日志头信息
  RecordEntry  record_entry_;  // 如果是修改数据的日志项，此结构体生效
  CommitEntry  commit_entry_;  // 如果是事务提交的日志项，此结构体生效
};

```

### log_entry.cpp

```cpp
#include "include/storage_engine/recover/log_entry.h"

using namespace std;

int _align8(int size)
{
  return size / 8 * 8 + ((size % 8 == 0) ? 0 : 8);
}

const char* logentry_type_name(LogEntryType type)
{
  switch (type)
  {
    case LogEntryType::ERROR:        return "ERROR";
    case LogEntryType::MTR_BEGIN:    return "MTR_BEGIN";
    case LogEntryType::MTR_COMMIT:   return "MTR_COMMIT";
    case LogEntryType::MTR_ROLLBACK: return "MTR_ROLLBACK";
    case LogEntryType::INSERT:       return "INSERT";
    case LogEntryType::DELETE:       return "DELETE";
    default:                        return "unknown redo log type";
  }
}
int32_t logentry_type_to_integer(LogEntryType type)
{
  return static_cast<int32_t>(type);
}
LogEntryType logentry_type_from_integer(int32_t value)
{
  return static_cast<LogEntryType>(value);
}

////////////////////////////////////////////////////////////////////////////////

string LogEntryHeader::to_string() const
{
  stringstream ss;
  ss << "trx_id:" << trx_id_
     << ", type:" << logentry_type_name(logentry_type_from_integer(type_)) << "(" << type_ << ")"
     << ", log_entry_len:" << log_entry_len_;
  return ss.str();
}

////////////////////////////////////////////////////////////////////////////////

string CommitEntry::to_string() const
{
  stringstream ss;
  ss << "commit_xid:" << commit_xid_;
  return ss.str();
}

////////////////////////////////////////////////////////////////////////////////

const int32_t RecordEntry::HEADER_SIZE = sizeof(RecordEntry) - sizeof(RecordEntry::data_);

RecordEntry::~RecordEntry()
{
  if (data_ == nullptr) {
    delete[] data_;
  }
}
string RecordEntry::to_string() const
{
  stringstream ss;
  ss << "table_id:" << table_id_ << ", rid:{" << rid_.to_string() << "}"
     << ", len:" << data_len_ << ", offset:" << data_offset_;
  return ss.str();
}

////////////////////////////////////////////////////////////////////////////////

LogEntry *LogEntry::build_mtr_entry(LogEntryType type, int32_t trx_id)
{
  LogEntry *log_entry = new LogEntry();

  LogEntryHeader &header = log_entry->entry_header_;
  header.trx_id_ = trx_id;
  header.type_ = logentry_type_to_integer(type);

  return log_entry;
}

LogEntry *LogEntry::build_commit_entry(int32_t trx_id, int32_t commit_xid)
{
  LogEntry *log_entry = new LogEntry();

  LogEntryHeader &header = log_entry->entry_header_;
  header.trx_id_ = trx_id;
  header.type_ = logentry_type_to_integer(LogEntryType::MTR_COMMIT);
  header.log_entry_len_ = sizeof(CommitEntry);

  CommitEntry &commit_entry = log_entry->commit_entry();
  commit_entry.commit_xid_ = commit_xid;

  return log_entry;
}

LogEntry *LogEntry::build_record_entry(LogEntryType type, int32_t trx_id, int32_t table_id, const RID &rid, int32_t data_len, int32_t data_offset, const char *data)
{
  LogEntry *log_entry = new LogEntry();

  LogEntryHeader &header = log_entry->entry_header_;
  header.trx_id_ = trx_id;
  header.type_ = logentry_type_to_integer(type);
  header.log_entry_len_ = RecordEntry::HEADER_SIZE + data_len;

  RecordEntry &record_entry = log_entry->record_entry();
  record_entry.table_id_ = table_id;
  record_entry.rid_ = rid;
  record_entry.data_len_ = data_len;
  record_entry.data_offset_ = data_offset;
  if (data_len > 0) {
    record_entry.data_ = new char[data_len];
    if (nullptr == record_entry.data_) {
      delete log_entry;
      LOG_WARN("failed to allocate memory while creating log entry. memory size=%d", data_len);
      return nullptr;
    }
    memcpy(record_entry.data_, data, data_len);
  }

  return log_entry;
}

LogEntry *LogEntry::build(const LogEntryHeader &header, char *data)
{
  LogEntry *log_entry = new LogEntry();
  log_entry->entry_header_ = header;

  if (header.log_entry_len_ <= 0) {
    return log_entry;
  }
  else if (header.type_ == logentry_type_to_integer(LogEntryType::MTR_COMMIT)) {
    ASSERT(header.log_entry_len_ == sizeof(CommitEntry), "invalid length of mtr commit. expect %d, got %d", sizeof(CommitEntry), header.log_entry_len_);
    CommitEntry &commit_entry = log_entry->commit_entry();
    memcpy(reinterpret_cast<void *>(&commit_entry), data, sizeof(CommitEntry));
    LOG_DEBUG("got a commit record %s", log_entry->to_string().c_str());
  }
  else {
    RecordEntry &record_entry = log_entry->record_entry();
    memcpy(reinterpret_cast<void *>(&record_entry), data, RecordEntry::HEADER_SIZE);
    if (header.log_entry_len_ > RecordEntry::HEADER_SIZE) {
      int data_len = header.log_entry_len_ - RecordEntry::HEADER_SIZE;
      record_entry.data_ = new char[data_len];
      memcpy(record_entry.data_, data + RecordEntry::HEADER_SIZE, data_len);
    }
  }

  return log_entry;
}

string LogEntry::to_string() const
{
  if (entry_header_.log_entry_len_ <= 0) {
    return entry_header_.to_string();
  } else if (entry_header_.type_ == logentry_type_to_integer(LogEntryType::MTR_COMMIT)) {
    return entry_header_.to_string() + ", " + commit_entry().to_string();
  } else {
    return entry_header_.to_string() + ", " + record_entry().to_string();
  }
}


```

### include/storage_engine/schema/database.h

```hpp
#pragma once

#include <fcntl.h>
#include <sys/stat.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/lang/string.h"
#include "common/log/log.h"
#include "common/os/path.h"
#include "include/common/rc.h"
#include "include/storage_engine/recorder/table.h"
#include "include/storage_engine/recover/log_manager.h"
#include "include/storage_engine/schema/schema_util.h"
#include "include/storage_engine/transaction/trx.h"

class Table;
class SelectStmt;
class RedoLogManager;

/**
 * @brief 一个DB实例负责管理一批表
 * @details 当前DB的存储模式很简单，一个DB对应一个目录，所有的表和数据都放置在这个目录下。
 * 启动时，从指定的目录下加载所有的表和元数据。
 */
class Db
{
public:
  Db() = default;
  ~Db();

  /**
   * @brief 初始化一个数据库实例
   * @details 从指定的目录下加载指定名称的数据库。这里就会加载dbpath目录下的数据。
   * @param name   数据库名称
   * @param dbpath 当前数据库放在哪个目录下
   * @note 数据库不是放在dbpath/name下，是直接使用dbpath目录
   */
  RC init(const char *name, const char *dbpath);

  RC create_table(const char *table_name, int attribute_count, const AttrInfoSqlNode *attributes);

  RC create_view(const char *view_name, const char *origin_table_name, SelectStmt *select_stmt, int attribute_count, const AttrInfoSqlNode *attributes);

  RC drop_table(const char *table_name);

  Table *find_table(const char *table_name) const;
  Table *find_table(int32_t table_id) const;

  const char *name() const;

  void all_tables(std::vector<std::string> &table_names) const;

  RC sync();

  RC recover();

  LogManager *log_manager();

private:
  RC open_all_tables();

private:
  std::string name_;
  std::string path_;
  std::unordered_map<std::string, Table *> opened_tables_;
  std::unique_ptr<LogManager> log_manager_;

  /// 给每个table都分配一个ID，用来记录日志。这里假设所有的DDL都不会并发操作，所以相关的数据都不上锁
  int32_t next_table_id_ = 0;
};

```

### storage_engine/schema/database.cpp

```cpp
#include "include/storage_engine/schema/database.h"

Db::~Db()
{
  for (auto &iter : opened_tables_) {
    delete iter.second;
  }
  LOG_INFO("Db has been closed: %s", name_.c_str());
}

RC Db::init(const char *name, const char *dbpath)
{
  if (common::is_blank(name)) {
    LOG_ERROR("Failed to init DB, name cannot be empty");
    return RC::INVALID_ARGUMENT;
  }

  if (!common::is_directory(dbpath)) {
    LOG_ERROR("Failed to init DB, path is not a directory: %s", dbpath);
    return RC::INTERNAL;
  }

  log_manager_.reset(new LogManager());
  if (log_manager_ == nullptr) {
    LOG_ERROR("Failed to init LogManager.");
    return RC::NOMEM;
  }

  RC rc = log_manager_->init(dbpath);
  if (RC_FAIL(rc)) {
    LOG_WARN("failed to init redolog manager. dbpath=%s, rc=%s", dbpath, strrc(rc));
    return rc;
  }

  name_ = name;
  path_ = dbpath;

  rc = open_all_tables();
  if (RC_FAIL(rc)) {
    LOG_WARN("failed to open all tables. dbpath=%s, rc=%s", dbpath, strrc(rc));
    return rc;
  }

  rc = recover();
  if (RC_FAIL(rc)) {
    LOG_WARN("failed to recover db. dbpath=%s, rc=%s", dbpath, strrc(rc));
    return rc;
  }
  return rc;
}

RC Db::create_table(const char *table_name, int attribute_count, const AttrInfoSqlNode *attributes)
{
  RC rc = RC::SUCCESS;
  // check table_name
  if (opened_tables_.count(table_name) != 0) {
    LOG_WARN("%s has been opened before.", table_name);
    return RC::SCHEMA_TABLE_EXIST;
  }

  // 文件路径可以移到Table模块
  std::string table_file_path = table_meta_file(path_.c_str(), table_name);
  Table *table = new Table();
  int32_t table_id = next_table_id_++;
  rc = table->create(table_id, table_file_path.c_str(), table_name, path_.c_str(), attribute_count, attributes);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to create table %s.", table_name);
    delete table;
    return rc;
  }

  opened_tables_[table_name] = table;
  LOG_INFO("Create table success. table name=%s, table_id:%d", table_name, table_id);
  return RC::SUCCESS;
}

RC Db::create_view(const char *view_name, const char *origin_table_name, SelectStmt *select_stmt, int attribute_count, const AttrInfoSqlNode *attributes) {
  RC rc = RC::SUCCESS;
  // check view_name
  if (opened_tables_.count(view_name) != 0) {
    LOG_WARN("%s has been opened before.", view_name);
    return RC::SCHEMA_TABLE_EXIST;
  }

  // 文件路径可以移到Table模块
  std::string view_file_path = table_meta_file(path_.c_str(), view_name);
  auto *view = new Table();
  int32_t table_id = next_table_id_++;
  rc = view->create(table_id, view_name, origin_table_name, select_stmt, attribute_count, attributes);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to create view %s.", view_name);
    delete view;
    return rc;
  }

  opened_tables_[view_name] = view;
  LOG_INFO("Create view success. view name=%s, table_id:%d", view_name, table_id);
  return RC::SUCCESS;
}

RC Db::drop_table(const char *table_name)
{
  RC rc = RC::SUCCESS;
  // check table_name
  if(opened_tables_.count(table_name) == 0) {
    LOG_WARN("%s hasn't been opened before.", table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  Table *table = opened_tables_[table_name];
  rc = table->drop(table->table_id(),table_name,path_.c_str());
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to drop table %s.", table_name);
    delete table;
    return rc;
  }

  opened_tables_.erase(table_name);
  delete table;
  LOG_INFO("Drop table success. table name=%s", table_name);
  return RC::SUCCESS;
}

Table *Db::find_table(const char *table_name) const
{
  std::unordered_map<std::string, Table *>::const_iterator iter = opened_tables_.find(table_name);
  if (iter != opened_tables_.end()) {
    return iter->second;
  }
  return nullptr;
}

Table *Db::find_table(int32_t table_id) const
{
  for (auto pair : opened_tables_) {
    if (pair.second->table_id() == table_id) {
      return pair.second;
    }
  }
  return nullptr;
}

RC Db::open_all_tables()
{
  std::vector<std::string> table_meta_files;
  int ret = common::list_file(path_.c_str(), TABLE_META_FILE_PATTERN, table_meta_files);
  if (ret < 0) {
    LOG_ERROR("Failed to list table meta files under %s.", path_.c_str());
    return RC::IOERR_READ;
  }

  RC rc = RC::SUCCESS;
  for (const std::string &filename : table_meta_files) {
    Table *table = new Table();
    rc = table->open(filename.c_str(), path_.c_str());
    if (rc != RC::SUCCESS) {
      delete table;
      LOG_ERROR("Failed to open table. filename=%s", filename.c_str());
      return rc;
    }

    if (opened_tables_.count(table->name()) != 0) {
      delete table;
      LOG_ERROR("Duplicate table with difference file name. table=%s, the other filename=%s",
          table->name(), filename.c_str());
      return RC::INTERNAL;
    }

    if (table->table_id() >= next_table_id_) {
      next_table_id_ = table->table_id() + 1;
    }
    opened_tables_[table->name()] = table;
    LOG_INFO("Open table: %s, file: %s", table->name(), filename.c_str());
  }

  LOG_INFO("All table have been opened. num=%d", opened_tables_.size());
  return rc;
}

const char *Db::name() const
{
  return name_.c_str();
}

void Db::all_tables(std::vector<std::string> &table_names) const
{
  for (const auto &table_item : opened_tables_) {
    table_names.emplace_back(table_item.first);
  }
}

RC Db::sync()
{
  RC rc = RC::SUCCESS;
  for (const auto &table_pair : opened_tables_) {
    Table *table = table_pair.second;
    rc = table->sync();
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to flush table. table=%s.%s, rc=%d:%s", name_.c_str(), table->name(), rc, strrc(rc));
      return rc;
    }
    LOG_INFO("Successfully sync table db:%s, table:%s.", name_.c_str(), table->name());
  }
  LOG_INFO("Successfully sync db. db=%s", name_.c_str());
  return rc;
}

RC Db::recover()
{
  return log_manager_->recover(this);
}

LogManager *Db::log_manager()
{
  return log_manager_.get();
}
```





# Lab5 测试流程完整指南

下面我将为你提供一个详细的测试流程，帮助你验证实现的故障恢复机制是否正常工作。我会包含每个步骤的具体命令和预期结果，以便你能够准确地进行测试。

## 第一步：构建和启动服务器

首先，确保你已经完成了代码实现并正确构建项目。

bash

复制

```
# 进入项目目录
cd path/to/your/project

# 清理之前的构建（如果需要）
rm -rf build

# 创建新的构建目录
mkdir build
cd build

# 配置和构建项目
cmake ..
make
```

## 第二步：创建初始数据表

1. **启动服务器（开启MVCC模式）**

bash

复制

```
./bin/server -f ../config/serverConfig.ini -p 6789 -t mvcc
```

1. **在新的终端中，连接客户端并创建表**

bash

复制

```
# 启动客户端
./client -p 6789

# 在客户端中执行
TDB > create table t_redo(id int, age int);
```

预期输出：

subunit

复制

```
SUCCESS
```

1. **退出客户端**

bash

复制

```
TDB > exit
```

## 第三步：运行测试事务

我们需要启动4个终端并连接到服务器，分别代表4个事务。按照以下顺序操作：

### Transaction 1

1. **启动第一个客户端**

bash

复制

```
./client -p 6789
```

1. **执行事务1的操作**

bash

复制

```
TDB > begin;
```

预期输出：`SUCCESS`

bash

复制

```
TDB > insert into t_redo values(0, 0);
```

预期输出：`SUCCESS`

bash

复制

```
TDB > insert into t_redo values(1, 10);
```

预期输出：`SUCCESS`

bash

复制

```
TDB > commit;
```

预期输出：`SUCCESS`

### Transaction 2

1. **启动第二个客户端**

bash

复制

```
./client -p 6789
```

1. **执行事务2的操作**

bash

复制

```
TDB > begin;
```

预期输出：`SUCCESS`

bash

复制

```
TDB > insert into t_redo values(2, 20);
```

预期输出：`SUCCESS`

bash

复制

```
TDB > commit;
```

预期输出：`SUCCESS`

### Transaction 3

1. **启动第三个客户端**

bash

复制

```
./client -p 6789
```

1. **执行事务3的操作**

bash

复制

```
TDB > begin;
```

预期输出：`SUCCESS`

bash

复制

```
TDB > insert into t_redo values(3, 30);
```

预期输出：`SUCCESS`

bash

复制

```
TDB > insert into t_redo values(5, 50);
```

预期输出：`SUCCESS`

### Transaction 4

1. **启动第四个客户端**

bash

复制

```
./client -p 6789
```

1. **执行事务4的操作**

bash

复制

```
TDB > begin;
```

预期输出：`SUCCESS`

bash

复制

```
TDB > insert into t_redo values(4, 40);
```

预期输出：`SUCCESS`

bash

复制

```
TDB > delete from t_redo where id=0;
```

预期输出：`SUCCESS`

## 第四步：模拟系统崩溃

在所有事务操作执行后，但在事务3和事务4提交前，我们模拟系统崩溃：

1. **在新的终端查找服务器进程**

bash

复制

```
lsof -i :6789
```

这会显示服务器进程的PID，如：

复制

```
COMMAND   PID     USER   FD   TYPE             DEVICE SIZE/OFF NODE NAME
server  12345 username    4u  IPv4 0x12345678      0t0  TCP *:6789 (LISTEN)
```

1. **终止服务器进程**

bash

复制

```
kill -9 12345  # 替换为实际的PID
```

此时，所有客户端连接应该会断开，显示连接错误。

## 第五步：重启服务器并验证恢复

1. **重新启动服务器**

bash

复制

```
cd build
./bin/server -f ../config/serverConfig.ini -p 6789 -t mvcc
```

在启动日志中，你应该能看到服务器正在从日志文件恢复数据的信息。

1. **启动客户端连接到服务器**

bash

复制

```
./client -p 6789
```

1. **验证数据恢复情况**

bash

复制

```
TDB > select * from t_redo;
```

预期输出：

复制

```
  id |  age
   0 |    0
   1 |   10
   2 |   20
```

这表明：

- 事务1和事务2已经提交的数据被成功恢复
- 事务3和事务4由于未提交而被丢弃（符合redo日志的恢复机制）

1. **进一步验证MVCC隔离级别**

你可以再进行一些事务操作，验证MVCC是否依然正常工作：

bash

复制

```
TDB > begin;
SUCCESS

TDB > insert into t_redo values(6, 60);
SUCCESS

TDB > select * from t_redo;
  id |  age
   0 |    0
   1 |   10
   2 |   20
   6 |   60

TDB > commit;
SUCCESS
```

## 第六步：验证日志文件增长

1. **检查redo.log文件大小**

bash

复制

```
ls -la tdb/db/sys/redo.log
```

1. **再执行一些事务并再次检查文件大小**

执行一些插入和删除操作，提交事务，然后再检查文件大小，验证日志文件正在增长：

bash

复制

```
ls -la tdb/db/sys/redo.log
```

## 第七步：边界情况测试

为了全面测试你的实现，可以考虑以下边界情况：

1. **嵌套崩溃测试**：系统恢复后立即再次崩溃
2. **大量事务测试**：创建多个并发事务，模拟高负载环境
3. **大数据量测试**：在单个事务中插入大量数据
4. **重复恢复测试**：确保恢复操作具有幂等性

## 测试结果分析

如果你的实现正确，在第五步中应该只能看到事务1和事务2的数据（ID为0、1、2的记录），因为只有这两个事务在崩溃前完成了提交。事务3和事务4由于未提交，其操作不应该出现在崩溃恢复后的数据库中。

如果观察到其他结果，可能的问题包括：

1. **缺少数据**：可能是redo日志没有正确写入或者恢复过程有问题
2. **多余数据**：未提交事务的操作被错误地恢复，说明恢复逻辑可能混入了未提交事务的操作
3. **数据不一致**：可能是MVCC版本控制没有正确实现，或者日志记录的顺序有问题

通过这个完整的测试流程，你应该能够全面验证你的故障恢复机制实现是否正确工作。如果遇到任何问题，可以根据测试结果有针对性地调试和修复代码。
