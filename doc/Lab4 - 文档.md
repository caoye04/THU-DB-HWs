# Lab4 - 文档

> 曹烨 2021012167
>
> 源地址：https://apache-iotdb-project.feishu.cn/docx/PL62dNEkroSoQ6xNbkHcaDVNnke

[toc]

## 一、Lab目标

本次Lab需要同学们实现多版本并发控制，即MVCC。

MVCC 会在修改数据时，不会直接在现有数据上修改，而是创建一个新的行记录，将旧数据复制出来，在新数据上做修改。并将新旧数据使用链表的方式串联起来。每个数据都会有自己的版本号（或者称为时间戳），而版本号通常使用单调递增的数字表示，每个事务根据自己的版本号与数据的版本号进比较，来判断当前能够访问哪个版本的数据。简单来说，MVCC通过维持多个版本的记录信息，结合版本可见性的控制，数据库系统可以无阻塞地处理读写和写读冲突。

MVCC最大的优点就是可以提高只读事务的并发度，它不与其它的写事务产生冲突，因为它访问旧版本的数据就可以了。

## 二、Lab条件

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

## 三、Lab知识

同学们需要学习《TDB事务模块解析》了解TDB事务模块的设计原理，并通过阅读源码进一步熟悉它的实现细节。

> ### 《TDB事务模块解析》
>
> #### 事务实现
>
> ##### 事务模型选择
>
> 当前TDB支持两种类型的事务模型，即Vacuous和MVCC，并且默认情况下是Vacuous，即不开启事务特性。
>
> - Vacuous：真空，顾名思义，它不会做任何事务相关的处理，这保留了原始的简单性，主要是便于其他Lab的学习和实现，简化调试。
> - MVCC：多版本并发控制，需要同学们自行完善，以解决多个事务并发执行时的冲突并保证ACID。
>
> trx.h 文件中有一个抽象类 `TrxManager`，根据运行时参数传入的名字来创建对应的 `VacuousTrxManager` 和 `MvccTrxManager`。这两个类可以创建相应的事务对象，并且按照需要，初始化行数据中事务需要的额外表字段。当前 Vacuous 什么字段都不需要，而 MVCC 会额外使用`__trx_xid_begin`和`__trx_xid_end`字段。
>
> ##### 事务接口
>
> 不同的事务模型，使用了一些统一的接口，这些接口定义在 `Trx`基类 中。
>
> **事务本身相关的操作**
>
> - start_if_need：开启一个事务。
> - commit：提交一个事务；
> - rollback：回滚一个事务。
>
> **行数据相关的操作**
>
> - insert_record：插入一行数据。事务可能需要对记录做一些修改，然后调用table的插入记录接口。提交之前插入的记录通常对其它事务不可见。
> - delete_record：删除一行数据。与插入记录类似，也会对记录做一些修改，对MVCC来说，并不是真正的将其删除，而是让他对其它事务不可见（提交后）；
> - visit_record：访问一行数据。当遍历记录，访问某条数据时，需要由事务来判断一下，这条数据是否对当前事务可见，或者事务有访问冲突。
>
> #### MVCC原理
>
> 版本号与可见性
>
> 与常见的MVCC实现方案相似，在TDB中使用单调递增的数字来作为版本号，并且在表上增加两个额外的字段来表示这条记录有效的版本范围。两个版本字段是`begin_xid`和`end_xid`，其中`begin_xid`和插入相关，`end_xid`和删除相关。每个事务在开始时，就会生成一个自己的版本号，当访问某条记录时，判断自己的版本号是否在该条记录的版本号的范围内，如果在，就是可见的，否则就不可见。
>
> **记录版本号与事务版本号**
>
> 行数据上的版本号，是事务设置的，这个版本号也是事务的版本号。一个写事务，通常会有两个版本号，在启动时，会生成一个版本号，用来在运行时做数据的可见性判断。在提交时，会再生成一个版本号，这个版本号是最终设置在记录上的。
>
> ```Bash
> trx start:
>   trx_id = next_id()
>   read record: is_visible(trx_id, record_begin_xid, record_end_xid)
> 
> trx commit:
>   commit_id = next_id()
>   foreach updated record: update record begin/end xid with commit_id
> ```
>
> **版本号与插入删除**
>
> 新插入的记录，在提交后，它的版本号是 `begin_xid` = 事务提交版本号，`end_xid` = 无穷大。表示此数据从当前事务开始生效，对此后所有的新事务都可见。 而删除相反，`begin_xid` 保持不变，而 `end_xid` 变成了当前事务提交的版本号。表示这条数据对当前事务之后的新事务，就不可见了。 
>
> 记录还有一个中间状态，就是事务刚插入或者删除，但是还没有提交时，这里的修改对其它事务应该都是不可见的。比如新插入一条数据，只有当前事务可见，而新删除的数据，只有当前事务不可见。需要使用一种特殊的方法来标记，当然也是在版本号上做动作。对插入的数据，`begin_xid` 改为 (-当前事务版本号)(负数)，删除记录将`end_xid`改为 (-当前事务版本号)。在做可见性判断时，对负版本号做特殊处理即可。
>
> 假设某个事务运行时trx id是 Ta，提交时是 Tc：
>
> ![](pic/pic4_1.png)
>
> **并发冲突处理**
>
> MVCC很好的处理了读事务与写事务的并发，读事务可以在其它事务修改了某个记录后，访问它的旧版本。但是写事务与写事务之间，依然是有冲突的。简单粗暴的解决思路就是当一个写事务想要修改某个记录时，如果看到有另一个事务也在修改，就直接回滚。如何判断其它事务在修改？判断`begin_xid`或`end_xid`是否为负数就可以。
>
> **隔离级别**
>
> 事务的隔离级别一般有四种：读未提交(Read Uncommitted)、读提交(Read Committed)、可重复读(Repeatable Read)和可串行化(Serializable)。按照上述的思路，TDB的MVCC能够做到可重复读的隔离级别。
>
> #### 遗留问题
>
> 当前的MVCC是一个简化版本，还有一些功能没有实现，并且还有一些已知BUG，同时还可以扩展更多的事务模型。
>
> 1. **事务提交时，对外原子可见**
>
> 当前事务在提交时，会逐个修改之前修改过的行数据，调整版本号。这造成的问题是，在某个时刻，有些行数据的版本号已经修改了，有些还没有。那可能会存在一个事务，能够看到已经修改完成版本号的行，但是看不到未修改的行。 比如事务A，插入了3条数据，在提交的时候，逐个修改版本号，某个情况下可能会存在下面的场景(假设A的事务ID是90，commit id是100)：
>
> ![](pic/pic4_2.png)
>
> 此时有一个新的事务，假设事务号是 110，那么它可以看到记录R1和R2，但是看不到R3，因为R3从记录状态来看，还没有提交。
>
> 2. **垃圾回收**
>
> 随着数据库进程的运行，不断有事务更新数据，不断产生新版本的数据，会占用越来越多的资源。此时需要一种机制，来回收对任何事务都不再可见的数据，这称为垃圾回收。
>
> 垃圾回收也是一个很有趣的话题，实现方式有很多种。最常见的是，开启一个或多个后台线程，定期的扫描所有的行数据，检查它们的版本。如果某个数据对当前所有活跃事务都不可见，那就认为此条数据是垃圾，可以回收掉。
>
> 3. **写写冲突**
>
> TDB对于写写冲突的解决比较简单粗暴，当一个写事务想要修改某个记录时，如果看到有另一个事务也在修改，就直接回滚。但这会导致高昂的回滚代价，效率低下。

本次Lab涉及到以下代码文件：

```Bash
include/storage_engine/transaction/mvcc_trx.h
storage_engine/transaction/mvcc_trx.cpp
```

## 四、Lab任务

### 模拟MVCC的可见性

#### 任务描述

我们在《TDB事务模块解析》中了解到了TDB的Mvcc事务模型，熟悉了版本号与可见性原理。因此在本次lab中，同学们就需要实现`MvccTrx`类的三个与行数据操作相关的接口，即`insert_record`、`delete_record`和`visit_record`，从而在多个事务同时执行时，能够模拟出数据可见与不可见的情景。

**需要在代码中至少实现以下方法：**

```C++
RC insert_record(Table *table, Record &record);
RC delete_record(Table *table, Record &record);
RC visit_record(Table *table, Record &record, bool readonly);
```

**温馨提示：**

> - **insert_record**
>   - 功能：被查询引擎的InsertPhysicalOperator算子的open函数调用，完成记录在存储引擎中的写入。
>   - 提示：参考《TDB事务模块解析》的版本号与可见性原理，对记录的新增字段做一些修改，然后调用table的插入记录接口。
> - **delete_record**
>   - 功能：被查询引擎的DeletePhysicalOperator算子的next函数调用，完成记录在存储引擎的删除。
>   - 提示：参考《TDB事务模块解析》的版本号与可见性原理，对记录的新增字段做一些修改，对MVCC来说，并不是真正的将其删除，而是让他对其它事务不可见（提交后）。
> - **visit_record**
>   - 功能：在遍历数据时被调用，判断数据是否可见。
>   - 提示：参考《TDB事务模块解析》的版本号与可见性原理，设计是否可见的逻辑。访问某条数据时，需要由事务来判断一下，这条数据是否对当前事务可见，或者事务有访问冲突。

#### 任务流程

事先创建好t_mvcc这个表，然后开启五个事务，依次执行一些数据的插入、删除和查询，根据返回结果判断其是否满足《TDB事务模块解析》中mvcc可见性的原则。若每一步操作的输出均与下述表格一致，说明lab功能实现正确；否则需要继续修改。

#### 模拟步骤

1. 启动server，开启mvcc模式

```
./bin/server -f ../config/serverConfig.ini -p 6789 ``-t mvcc
```

1. 开启一个client
   1.  执行`create table t_mvcc(id int, age int, name char(4));`创建初始表t_mvcc，然后退出。
2. 开启五个client连接到server

按照下表中的时间顺序，依次执行对应的SQL操作，并检查输出是否与表格中预期的一致。

#### 验证表格

| 时间\事务 | transaction1                                                 | transaction2                                                 | transaction3                                                 | transaction4                                                 | transaction5                                                 |
| --------- | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| t1        | TDB > begin;SUCCESS                                          |                                                              |                                                              |                                                              |                                                              |
| t2        |                                                              | TDB > begin;SUCCESS                                          |                                                              |                                                              |                                                              |
| t3        | TDB > insert into t_mvcc values(1, 1, 'a');SUCCESSTDB > insert into t_mvcc values(2, 2, 'b');SUCCESS |                                                              |                                                              |                                                              |                                                              |
| t4        | TDB > select * from t_mvcc  id \|  age \| name   1 \|    1 \|    a   2 \|    2 \|    b（可以读到写入数据说明正确，这个操作是为了验证事务可以读到自己写的数据） |                                                              |                                                              |                                                              |                                                              |
| t5        |                                                              | TDB > select * from t_mvcc;(结果为空才正确，这个操作是为了验证事务读不到其他事务还未提交的数据） |                                                              |                                                              |                                                              |
| t6        | TDB > commit;SUCCESS                                         |                                                              |                                                              |                                                              |                                                              |
| t7        |                                                              | TDB > select * from t_mvcc;(结果为空才正确，因为该事务id小于事务1提交时的时间戳，所以读不到） |                                                              |                                                              |                                                              |
| t8        |                                                              |                                                              | TDB > begin;SUCCESS                                          |                                                              |                                                              |
| t9        |                                                              |                                                              | TDB > select * from t_mvcc  id \|  age \| name   1 \|    1 \|    a   2 \|    2 \|    b（可以读到事务1写入的数据才正确，因为该事务id大于事务1提交时的时间戳） |                                                              |                                                              |
| t10       |                                                              | TDB > delete from t_mvcc where id=1;（这个删除是无效的，因此该事务看不到这条数据） |                                                              |                                                              |                                                              |
| t11       |                                                              | TDB > commit;SUCCESS                                         |                                                              |                                                              |                                                              |
| t12       |                                                              |                                                              |                                                              | TDB > begin;SUCCESS                                          |                                                              |
| t13       |                                                              |                                                              | TDB > delete from t_mvcc where id=1;（输出SUCCESS才正确，因此该事务可以看到该记录） |                                                              |                                                              |
| t14       |                                                              |                                                              |                                                              | TDB > select * from t_mvcc  id \|  age \| name   1 \|    1 \|    a   2 \|    2 \|    b（可以查到完整数据才正确，因为该事务看不到其他事务未提交的删除操作） |                                                              |
| t15       |                                                              |                                                              | TDB > commit;SUCCESS                                         |                                                              |                                                              |
| t16       |                                                              |                                                              |                                                              | TDB > select * from t_mvcc  id \|  age \| name   1 \|    1 \|    a   2 \|    2 \|    b（可以查到完整数据才正确，因为该事务id小于事务3提交时的时间戳） |                                                              |
| t17       |                                                              |                                                              |                                                              | TDB > commit;SUCCESS                                         |                                                              |
| t18       |                                                              |                                                              |                                                              |                                                              | TDB > begin;SUCCESS                                          |
| t19       |                                                              |                                                              |                                                              |                                                              | TDB > select * from t_mvcc  id \|  age \| name   2 \|    2 \|    b（查不到id为1的记录才正确，因为被事务3删除了） |
| t20       |                                                              |                                                              |                                                              |                                                              | TDB > commit;SUCCESS                                         |

## 五、Lab提交

### SQL验证

同学们可以启动TDB的server后，按照表格的流程输入SQL进行模拟，并与预期结果进行对比，判断自己的实现是否正确。

### 代码提交

至少需要提交以下两个代码文件。 

注意必须是zip格式的压缩包！

如果修改了其他代码文件，也需要一并提交！

```Java
include/storage_engine/transaction/mvcc_trx.h
storage_engine/transaction/mvcc_trx.cpp
```

## 六、初始代码

```hpp
// include/storage_engine/transaction/mvcc_trx.h

#pragma once

#include "include/storage_engine/transaction/trx.h"

/**
* @brief MVCC(多版本并发控制)事务管理器
 */
class MvccTrxManager : public TrxManager
{
public:
  MvccTrxManager() = default;
  virtual ~MvccTrxManager();

  RC init() override;
  const std::vector<FieldMeta> *trx_fields() const override;
  Trx *create_trx(LogManager *log_manager) override;
  Trx *create_trx(int32_t trx_id) override;
  // 找到对应事务号的事务，当前仅在recover场景下使用
  Trx *find_trx(int32_t trx_id) override;
  void all_trxes(std::vector<Trx *> &trxes) override;
  void destroy_trx(Trx *trx) override;

  int32_t next_trx_id();
  int32_t max_trx_id() const;

  // 在 recover 场景下使用，确保当前事务 id 不小于 trx_id
  void update_trx_id(int32_t trx_id);

private:
  std::vector<FieldMeta> fields_; // 存储事务数据需要用到的字段元数据，所有表结构都需要带
  std::atomic<int32_t> current_trx_id_{0};
  common::Mutex      lock_;
  std::vector<Trx *> trxes_;
};

class MvccTrx : public Trx
{
 public:
  MvccTrx(MvccTrxManager &trx_kit, LogManager *log_manager);
  MvccTrx(MvccTrxManager &trx_kit, int32_t trx_id); // used for recover
  virtual ~MvccTrx() = default;

  TrxType type() override { return MVCC; }

  RC insert_record(Table *table, Record &record) override;
  RC delete_record(Table *table, Record &record) override;

  /**
   * @brief 当访问到某条数据时，使用此函数来判断是否可见，或者是否有访问冲突
   * @param table    要访问的数据属于哪张表
   * @param record   要访问哪条数据
   * @param readonly 是否只读访问
   * @return RC      - SUCCESS 成功
   *                 - RECORD_INVISIBLE 此数据对当前事务不可见，应该跳过
   *                 - LOCKED_CONCURRENCY_CONFLICT 与其它事务有冲突
   */
  RC visit_record(Table *table, Record &record, bool readonly) override;

  RC start_if_need() override;
  RC commit() override;
  RC rollback() override;

  RC redo(Db *db, const LogEntry &log_entry) override;

  int32_t id() const override { return trx_id_; }

 private:
  /**
   * @brief 获取指定表上的与版本号相关的字段
   * @param table 指定的表
   * @param begin_xid_field 返回begin_xid的字段
   * @param end_xid_field   返回end_xid的字段
   */
  void trx_fields(Table *table, Field &begin_xid_field, Field &end_xid_field) const;

  /**
   * @brief 使用指定的版本号来提交事务
   * @param commit_xid
   * @return
   */
  RC commit_with_trx_id(int32_t commit_xid);

 private:
  static const int32_t MAX_TRX_ID = std::numeric_limits<int32_t>::max();

 private:
  using OperationSet = std::unordered_set<Operation, OperationHasher, OperationEqualer>;
  MvccTrxManager & trx_kit_;
  LogManager *log_manager_ = nullptr;
  int32_t      trx_id_ = -1;
  bool         started_ = false;
  bool         recovering_ = false;
  OperationSet operations_;
};

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

