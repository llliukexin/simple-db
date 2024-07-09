# lab1 simple DB

pass the unit tests `TupleTest` and `TupleDescTest`

pass the unit tests in `CatalogTest`

pass the unit tests in `HeapPageIdTest`, `RecordIDTest`, and `HeapPageReadTest`

pass the unit tests in `HeapFileReadTest`

complete the `ScanTest` system test

# lab2 操作符

pass the unit tests in `PredicateTest`, `JoinPredicateTest`, `FilterTest`, and `JoinTest`

system tests `FilterTest` and `JoinTest`

unit tests `IntegerAggregatorTest`, `StringAggregatorTest`, and `AggregateTest`

pass the `AggregateTest` system test

 pass the unit tests in `HeapPageWriteTest` and `HeapFileWriteTest`,`BufferPoolWriteTest`.

pass the unit tests in `InsertTest`

pass the `InsertTest` and `DeleteTest` system tests

pass the `EvictionTest` system test

# lab3 查询优化

pass the `IntHistogramTest` unit test 

pass the unit tests in `TableStatsTest`

pass the unit tests `estimateJoinCostTest` and `estimateJoinCardinality` in `JoinOptimizerTest.java`

unit tests in `JoinOptimizerTest`

system test `QueryTest`



# lab4 事务

unit tests in `LockingTest.java`

pass the `TransactionTest` unit test 

the `AbortEvictionTest` system test

pass the `TransactionTest` system test

 `test/simpledb/DeadlockTest.java`

## exercise1 

实现LockManager：管理每一页的锁的状态，处理事务申请锁、释放锁等；主要实现方法holdsLock（判断





# lab5 B+树索引

unit tests in `BTreeFileReadTest.java`

system tests in `BTreeScanTest.java`

unit tests in `BTreeFileInsertTest.java`

system tests in `systemtest/BTreeFileInsertTest.java`

unit tests in `BTreeFileDeleteTest.java`

system tests in `systemtest/BTreeFileDeleteTest.java`

the tests in `test/simpledb/BTreeDeadlockTest.java`

pass the `BTreeTest system test`

## exercise1 Search

BTreeFile.findLeafPage()

- BTreeRootPtrPage（根结点页面）：B+树的根节点。
- - header : 储存slot使用情况。
  - root : 根节点的value。
  - rootCategory ：根节点类型。
  - dirty : 是否是脏页。
  - oldData : 用于回滚。
- BTreeInternalPage (内部节点页面）： B+树的内部节点
- - int numSlot: 内部节点中最多能存储指针的数量。
  - byte[] header : 储存slot使用情况。
  - Field[] keys: 存储key的数组。
  - **int [] children: 存储page的序号，用于每个key指向左右children的point。也因此如果keys是m，children则是m+1。**
  - int childCategorychild:节点的类型（either leaf or internal）
- BTreeLeafPage(叶子节点页面）： B+树的叶子节点
- - int numSlot: 叶节点中最多能存储指针的数量。

  - byte[] header : 储存slot使用情况。
  - int leftSibling : 左叶子节点，为0则为空。
  - int rightSibling : 右叶子节点，为0则为空。
  - Tuple[] tuples : 存放的具体元组数据。
- BTreeHeaderPage(Header节点页面）：用于记录整个B+树中的一个页面的使用情况
- - BTreePageId pid: 记录目标页面的pageId。
  - int numSlot: 叶节点中最多能存储指针的数量。
  - byte[] header : 储存slot使用情况。
  - int nextPage : 指向下一个 headerPage，为0则为空。
  - int prevPage : 指向上一个 headerPage，为0则为空。



辅助类：

- BTreePageId：以上四个页的唯一标识符。
- - tableid：该page所在table的id。
  - pgNo：所在page的no（所在的table中的第几个页）。
  - pgcateg：用于标识BTreePage的类型。
- BTreeEntry： BTreeInternalPage所**更新的单位**，虽然BTreeInternalPage页面中存储的是keys与children，但是实际更新（查找、插入、删除等）的单位则是BTreeEntry对象。

- - Field key : entry的key。
  - BTreePageId leftChild ：左孩子的page id。
  - BTreePageId rightChild ：右孩子的page id。
  - RecordId rid ： 记录entry位于哪个page。



outline提示：

给定值1，此函数应返回第一个叶页。同样，给定值8，该函数应返回第二页。而在某种case下，如果给我们一个键值6。可能有重复的键，所以两个叶页上可能都有6个。在这种情况下，函数应该返回第一个（左）叶页。

递归的搜索页面，直到搜索到所需的叶子节点页面。如果pgcateg() = BTreePageId.LEAF 则表明这是叶子页面退出递归，否则则是内部页面，需要利用BTreeInternalPage.iterator() 遍历页面中的entrys，并与每个key值做比较，递归进入到下一层的节点。

建议不要直接调用BufferPool.getPage（） 来获取每个内部页面和叶页，而是调用我们提供的包装器函数BTreeFile.getPage（） 。它的工作方式与BufferPool.get page（） 完全相同，但需要一个额外的参数来跟踪脏页列表。在接下来的两个练习中，该函数将非常重要，在这两个练习中将实际更新数据，因此需要跟踪脏页。
findLeafPage（） 实现访问的**每个内部（非叶）页面都应该使用READ_ONLY权限**获取，但**返回的叶页面**除外，该页应该**使用作为函数参数提供的权限获取**。

## exercise2 Insert

我们认为一次处理一个条目是与内部页面交互的自然方式，但重要的是要记住，底层页面实际上并不存储条目列表，而是存储 m 个键和 m+1 个子指针的有序列表。由于 BTreeEntry 只是一个接口，而不是实际存储在页面上的对象，因此**更新 BTreeEntry 的字段不会修改底层页面**。为了更改页面上的数据，您需要调用 BTreeInternalPage.updateEntry()。此外，删除条目实际上只会删除一个键和一个子指针，因此我们提供函数 BTreeInternalPage.deleteKeyAndLeftChild() 和 BTreeInternalPage.deleteKeyAndRightChild() 来明确这一点。条目的 recordId 用于查找要删除的键和子指针。插入条目也只会插入一个键和单个子指针（除非它是第一个条目），因此 BTreeInternalPage.insertEntry() 会检查提供的条目中的一个子指针是否与页面上的现有子指针重叠，并且在该位置插入条目将保持键的排序顺序。

<img src="images\BplusTree_insert.png" alt="BplusTree_insert" style="zoom:33%;" />

注：内节点和叶节点分裂不同

​	叶节点分裂：将分裂点出数据复制（备份）至父节点中，维护底层数据的指向

​	内节点分裂：将分裂点数据直接上移至父节点，无需刻意维护底层数据

<img src="images\splitting_leaf.png" alt="splitting_leaf" style="zoom:33%;" />

<img src="images\splitting_internal.png" alt="splitting_internal" style="zoom:33%;" />

outline提示：

每当你想要创建一个新的页，或者因为分裂节点需要**创建新页**。你可以调用 getEmptyPage()去获得新页，这个函数可以复用因为合并而被删除的页。

提供**BTreeLeafPage.iterator() 与 BTreeInternalPage.iterator() 去迭代每个页中的tuples / entries。**同时可以利用**BTreeLeafPage.reverseIterator()与TreeLeafPage.reverseIterator()进行分裂的两个页面之间的重新分配。**

对于Entry ：更新Entry对象将不会更新实际的页，需要**更新实际的页可以调用BTreeInternalPage.updateEntry(),**
删除一个Entry实际上是删除一个key与一个child pointer,需要**删除操作可以调用：BTreeInternalPage.deleteKeyAndLeftChild()与BTreeInternalPage.deleteKeyAndRightChild()。**
对于插入同样是插入一个key与一个single child pointer。可以使用BTreeInternalPage.insertEntry()去插入一个Entry，并保持key在entries中的顺序。

调用splitLeafPage()与splitInternalPage()产生新的页面或者修改页面数据时需要**更新dirtypages**。每次获取页面调用BTreeFile.getPage()他会先**获取本地的`dirtypages``，如果没有则再去调用BufferPool**。

其中值得注意的是以下这个方法：
`getParentWithEmptySlots`：获取具有读写权限的父页面，如果父节点中key的数量到达了n-1，则会调用`splitInternalPage()`方法继续**递归**，最终返回一个可以插入新key的内部节点。

## **exercise3 Delete:R**edistributing pages

### 实现逻辑说明

1. **计算需要移动的条目数**：首先，计算左兄弟页和当前页的总条目数，然后决定需要从左兄弟页移动多少条目，以使两页的条目数均匀分布。
2. **获取父条目的键**：从父条目获取键，并将其作为要插入当前页的第一个条目。这有助于维持树的结构。
3. **从左兄弟页移动条目**：从左兄弟页逆序迭代，逐一删除条目并插入到当前页。移动条目后，更新父节点中的父条目键。
4. **更新父指针**：调用 `updateParentPointers` 方法更新所有受影响条目的父指针。
5. **更新脏页**：最后，确保所有更改的页被标记为脏页，以便稍后在事务提交时刷新到磁盘。

## exercise4 Delete:Merging pages

流程：

<img src="images\BplusTree_delete.png" alt="BplusTree_delete" style="zoom:33%;" />

delete example：

<img src="images\redist_internal.png" alt="redist_internal" style="zoom:33%;" />

<img src="images\redist_leaf.png" alt="redist_leaf" style="zoom:33%;" />

- 叶节点之间需要维护新的指针指向，而内部节点不需要。
- 叶节点与父亲节点之间的关系是**复制关系**，而内部节点则**必须唯一**，也就是需要**挤上去**。
- 还有一个比较难看出的是 ：内部节点被挤下来的**父节点**的孩子节点应该指谁，叶子节点本来就是最后一层则不用考虑这个问题。代码中注解提到的则是：
- Keys can be thought of as rotating through the parent entry, so the original key in the parent is “pulled down” to the left-hand page, and the last key in the right-hand page is “pushed up” to the parent. Update parent pointers as needed

<img src="images\delete_pointer.png" alt="delete_pointer" style="zoom:33%;" />

<img src="images\merging_internal.png" alt="merging_internal" style="zoom:33%;" />

<img src="images\merging_leaf.png" alt="merging_internal" style="zoom:33%;" />

**`mergeLeafPages` 实现**

步骤

1. **移动右页面的所有元组**: 将右页面的所有元组移动到左页面。
2. **更新兄弟指针**: 更新左页面的右兄弟指针，确保链接正确。
3. **将右页面标记为空页面**: 右页面变为空页面，以便重用。
4. **删除父节点中的相应条目**: 从父节点中删除指向这两个页面的条目，并递归处理父节点。

**`mergeInternalPages` 实现**

步骤

1. **将右页面的所有条目移动到左页面**: 并拉下父节点的相应键作为分隔键。
2. **更新父指针**: 更新所有被移动条目的子节点的父指针。
3. **将右页面标记为空页面**: 右页面变为空页面，以便重用。
4. **从父节点中删除相应的条目**: 删除父节点中的条目，并递归处理父节点。



# lab6 回滚和故障恢复（基于日志

pass the TestAbort and TestAbortCommitInterleaved sub-tests of the LogTest system test

pass all of the LogTest system test

## exercise1 RollBack

**实现 `rollback()`**

1. **定位事务的起始日志记录**：
   - 使用 `tidToFirstLogRecord` 来找到给定事务 ID (`tid`) 的日志记录的起始位置。
2. **从日志文件读取日志记录**：
   - 遍历日志文件，读取和解析日志记录。根据记录类型，找到属于特定事务的 `UPDATE` 日志记录。
3. **提取前镜像**：
   - 从 `UPDATE` 日志记录中提取前镜像（`before-image`）。
4. **将前镜像写回到表文件中**：
   - 将前镜像写回到表文件中以恢复其状态。
5. **从缓冲池中丢弃页面**：
   - 丢弃写回的页面，以确保在事务终止后缓冲池不会持有任何旧数据。

## exercise2 Recovery

**实现步骤**

**1. 读取检查点**

从日志文件中读取最后一个检查点的位置，以确定恢复操作的起点。如果没有检查点，则从日志文件的开头开始。

**2. 前向扫描**

扫描日志文件，处理每个日志记录以重新应用已提交事务的更新，并记录那些未完成的事务。

**3. 撤销未完成事务的更新**

对于未完成的事务，读取它们的日志记录，将页面恢复到原来的状态（使用 `before-image`），并确保这些页面被正确地写回磁盘。

**深入理解下recovery的条件，什么时候进行recovery**

日志中的checkpoint会导致数据强制刷盘。而检查点的触发条件，在正常情况下，仅仅只是周期性的定时检查，不涉及事务。因此在checkpoint的阶段可能会有未提交的事务也有已经提交的事务但是未刷盘，而此时对于前者则需要回滚（undo），对于已经提交的事务此时需要redo。
还有一个点就是什么时候开始读取第一个恢复点。第一个恢复点应该是crash时记录到的checkpoint中记录的最早的活跃的事务的offset。并且获取正在live的事务的必须只能通过checkpoint，而不能通过tidToFirstLogRecord，因为在crash情况下tidToFirstLogRecord内存的数据访问不到。当然为了快速通过实验也可以直接从0开始读取全量日志进行恢复工作，但是这种做法应该是不提倡的。