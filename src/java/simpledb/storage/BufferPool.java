package simpledb.storage;

import lombok.Getter;
import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private int numPages;
//    private Page[] buffer;
    /* 缓存页 */
    private ConcurrentHashMap<PageId, Page> buffer;

    /* 维护页面顺序，实现page eviction
    *  最近访问位于链表尾部 */
    private LinkedList<PageId> pageList;
    private LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.lockManager = new LockManager();
        buffer = new ConcurrentHashMap<>(numPages);
        pageList = new LinkedList<>();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        LockType lockType;
        if(perm == Permissions.READ_ONLY){
            lockType = LockType.SHARE_LOCK;
        }else {
            lockType = LockType.EXCLUSIVE_LOCK;
        }
        try {
            // 如果获取lock失败（重试3次）则直接放弃事务
            if (!lockManager.acquireLock(pid,tid,lockType,0)){
                // 获取锁失败，回滚事务
                throw new TransactionAbortedException();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.out.println("Method 「 getPage 」获取锁发生异常！！！");
        }

        // some code goes here
        if (buffer.containsKey(pid)){
            /* 页在缓冲区，访问时更新访问顺序 */
            pageList.remove(pid);
            pageList.addLast(pid);
            return buffer.get(pid);
        }

        /* 页面不在缓冲区中，从catalog读入 */
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = dbFile.readPage(pid);

        if (buffer.size() > numPages){
            evictPage();
        }
        pageList.addLast((pid));
        buffer.put(pid, page);
        return page;
//        int idx = -1;
//        for(int i = 0;i < buffer.length;i++){
//            if(buffer[i] == null){
//                idx = i;
//            } else if (pid.equals(buffer[i].getId())) {
//                return buffer[i];
//            }
//        }
//        return buffer[idx] = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releasePage(tid, pid);
    }
    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);

    }



    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     *
     * transactionComplete（）需要对事务的完成做一个处理首先得分事务成功完成，还是失败（需要回滚）。
     *
     * 在事务成功完成时，需要将磁盘中的脏页全部刷新到磁盘，
     *               若事务失败时则需要回滚：将磁盘中的反向刷新到BufferPool。
     * 释放掉事务所拥有的所有锁。完成S2PL的释放阶段。
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit){
            try{
                flushPages(tid);
                System.out.println("bufferPool_transactionComplete.");
            } catch (IOException e) {
                throw new RuntimeException("flush error!");
            }
        }else {
            rollBack(tid);
        }
        lockManager.releasePagesByTid(tid);
    }

    private void rollBack(TransactionId tid) {
        List<PageId> rbPage = new LinkedList<>();
        for (PageId pid: buffer.keySet()){
            Page page = buffer.get(pid);
            if (page.isDirty() != null && page.isDirty().equals(tid)){
                try{
                    Page originalPage = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                    buffer.put(pid, originalPage);
                    rbPage.add(pid);
                } catch (NoSuchElementException e) {
                    throw new RuntimeException("Roll Back fail.");
                }
            }
        }
        /* 更新访问顺序 */
        for (PageId pid : rbPage){
            pageList.remove(pid);
            pageList.addLast(pid);
        }
    }

    public void updateBufferPool(List<Page> pages, TransactionId tid){
        for (Page page : pages){
            page.markDirty(true, tid);
            pageList.remove(page.getId());
            pageList.addLast(page.getId());
            buffer.put(page.getId(), page);
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> modifiedPages = dbFile.insertTuple(tid, t);
        updateBufferPool(modifiedPages,tid);
        // not necessary for lab1
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> modifiedPages = dbFile.deleteTuple(tid, t);
        updateBufferPool(modifiedPages, tid);
        // not necessary for lab1
    }



    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        if (pid != null){
            System.out.println("discard sus.");
            buffer.remove(pid);
            pageList.remove(pid);
        }else {
            System.out.println("current pid is null.");
        }

    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pid: buffer.keySet()){
            if (buffer.get(pid).isDirty() != null)
                flushPage(pid);
        }
    }
    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = buffer.get(pid);
        TransactionId tid = page.isDirty();
        if (page != null && tid != null){

            Page before = page.getBeforeImage();
            Database.getLogFile().logWrite(tid, before, page);
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (Map.Entry<PageId, Page> entry: buffer.entrySet()){
            Page flushPage = entry.getValue();
//            TransactionId flushPageDirty = flushPage.isDirty();
            Page before = flushPage.getBeforeImage();
            // !!!!!涉及到事务提交就应该setBeforeImage(设置oldData，更新数据，方便后续的事务终止能回退此版本
            flushPage.setBeforeImage();
            if (flushPage.isDirty() != null && flushPage.isDirty().equals(tid))
                Database.getLogFile().logWrite(tid, before, flushPage);
                Database.getCatalog().getDatabaseFile(flushPage.getId().getTableId()).writePage(flushPage);
//                flushPage(entry.getKey());
        }

    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        if (pageList.isEmpty())
            throw new DbException("bufferPool is empty.");
//        PageId pageId = pageList.getFirst();
        for (int i = 0;i < numPages;i++){
            PageId pid = pageList.removeFirst();
            Page page = buffer.get(pid);
            if (page.isDirty() != null){
                pageList.addLast(pid);
            }else {
                try{
                    flushPage(pid);
                } catch (IOException e) {
                    throw new DbException("flush page fail, page " + pid + ".");
                }
                buffer.remove(pid);
                return;
            }
        }

        throw new DbException("All Page Are Dirty Page");
//        try{
//            flushPage(pid);
//        } catch (IOException e) {
//            throw new DbException("flush page fail, page " + pid + ".");
//        }
//        buffer.remove(pid);
    }

    private class PageLock{
        private TransactionId tid;
        private LockType type;
        private PageId pid;

        public PageLock(TransactionId tid, PageId pageId, LockType requestLock) {
            this.tid = tid;
            this.pid = pageId;
            this.type = requestLock;
        }

        public LockType getType() {
            return type;
        }
    }

    public enum LockType{
        SHARE_LOCK (0,"共享锁"),
        EXCLUSIVE_LOCK(1,"排它锁");
        @Getter
        private Integer code;
        @Getter
        private String value;

        LockType(int code,String value) {
            this.code = code;
            this.value = value;
        }
    }

    private class LockManager {
        @Getter
        public ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, PageLock>> lockMap;

        public LockManager() {
            lockMap = new ConcurrentHashMap<>();
        }

        /**
         * Return true if the specified transaction has a lock on the specified page
         */
        public boolean holdsLock(TransactionId tid, PageId p) {
            // some code goes here
            // not necessary for lab1|lab2
            if(lockMap.get(p) == null){
                return false;
            }
            return lockMap.get(p).get(tid) != null;
        }

        public synchronized boolean acquireLock(PageId pageId, TransactionId tid, LockType requestLock, int reTry) throws TransactionAbortedException, InterruptedException {
            // 重传达到3次
            int reTryMax = 3;
            if (reTry == reTryMax) return false;
//            // 用于打印log
//            final String thread = Thread.currentThread().getName();
            // 页面上不存在锁
            if (lockMap.get(pageId) == null) {
                return putLock(tid,pageId,requestLock);
            }

            // 页面上存在锁
            ConcurrentHashMap<TransactionId, PageLock> tidLocksMap = lockMap.get(pageId);

            if (tidLocksMap.get(tid) == null) {
                // 页面上的锁不是自己的
                // 请求的为X锁
                if (requestLock == LockType.EXCLUSIVE_LOCK) {
                    wait(100);
                    return acquireLock(pageId, tid, requestLock, reTry + 1);
                } else if (requestLock == LockType.SHARE_LOCK) {
                    // 页面上是否都是读锁 -> 页面上的锁大于1个，就都是读锁
                    // 互斥锁只能被一个事务占有
                    if (tidLocksMap.size() > 1) {
                        // 都是读锁直接获取
                        return putLock(tid,pageId,requestLock);
                    } else {
                        Collection<PageLock> values = tidLocksMap.values();
                        for (PageLock value : values) {
                            // 存在的唯一的一个锁为X锁
                            if (value.getType() == LockType.EXCLUSIVE_LOCK) {
                                wait(100);
                                return acquireLock(pageId, tid, requestLock, reTry + 1);
                            } else {
                                return putLock(tid,pageId,requestLock);
                            }
                        }
                    }
                }
            }else {
                if (requestLock == LockType.SHARE_LOCK) {
                    tidLocksMap.remove(tid);
                    return putLock(tid,pageId,requestLock);
                }else {
                    // 判断自己的锁是否为排它锁，如果是直接获取
                    if(tidLocksMap.get(tid).getType() == LockType.EXCLUSIVE_LOCK){
                        return true;
                    }else {
                        // 拥有的是读锁，判断是否还存在别的读锁
                        if(tidLocksMap.size() > 1){
                            wait(100);
                            return acquireLock(pageId, tid, requestLock, reTry + 1);
                        }else{
                            // 只有自己拥有一个读锁，进行锁升级
                            tidLocksMap.remove(tid);
                            return putLock(tid,pageId,requestLock);
                        }
                    }
                }
            }
            return false;
        }

        public boolean putLock(TransactionId tid, PageId pageId, LockType requestLock){
            ConcurrentHashMap<TransactionId, PageLock> tidLocksMap = lockMap.get(pageId);
            // 页面上一个锁都没
            if(tidLocksMap == null){
                tidLocksMap = new ConcurrentHashMap<>();
                lockMap.put(pageId,tidLocksMap);
            }
            PageLock pageLock = new PageLock(tid, pageId, requestLock);
            tidLocksMap.put(tid, pageLock);
            lockMap.put(pageId, tidLocksMap);
            return true;
        }

        /**
         * 释放某个页上tid的锁
         */
        public synchronized void releasePage(TransactionId tid, PageId pid) {
            if (holdsLock(tid,pid)){
                ConcurrentHashMap<TransactionId,PageLock> tidLocks = lockMap.get(pid);
                tidLocks.remove(tid);
                if (tidLocks.size() == 0){
                    lockMap.remove(pid);
                }
                // 释放锁时就唤醒正在等待的线程
                // 因为wait与notifyAll都需要在同步代码块里，所以需要加synchronized
                this.notifyAll();
            }
        }
        public synchronized void releasePagesByTid(TransactionId tid){
            Set<PageId> pageIds = lockMap.keySet();
            for (PageId pageId : pageIds){
                releasePage(tid,pageId);
            }
        }
    }



}
