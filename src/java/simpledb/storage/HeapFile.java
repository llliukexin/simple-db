package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

import static java.lang.Math.floor;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File heapFile;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.heapFile = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.heapFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        //fileId同tableId
        return heapFile.getAbsoluteFile().hashCode();
//        throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
//        throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int tableId = pid.getTableId();
        int pgNo = pid.getPageNumber();
        try(RandomAccessFile raf = new RandomAccessFile(heapFile,"r")) {
            byte[] data = new byte[BufferPool.getPageSize()];
            int pageSize = BufferPool.getPageSize();
            int offset = pgNo * pageSize;
            raf.seek(offset);
            int num = raf.read(data,0, pageSize);
            if (num != pageSize)
                throw new IllegalArgumentException(String.format("table %d page %d does not exist in this file!",tableId,pgNo));
            //创建该页
            HeapPageId pageId = new HeapPageId(tableId, pgNo);
            HeapPage page = new HeapPage(pageId,data);
            return page;
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // see DbFile.java for javadocs
    // TODO
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pgNo = page.getId().getPageNumber();
        try(RandomAccessFile raf = new RandomAccessFile(heapFile,"rw")){
            int pageSize = BufferPool.getPageSize();
            int offset = pgNo * pageSize;
            raf.seek(offset);
            byte[] data = page.getPageData();
            raf.write(data);
        }catch (IOException e){
            throw new IOException("write fail.");
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) floor(heapFile.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        /* 找所有已存在的页，存在空闲插槽则插入，并标记页易发生修改
        *  否则创建新页，插入元组，写新页加入文件结尾 */
        List<Page> modifiedPages = new ArrayList<>();
        for (int i = 0; i < numPages();i++){
            /* 获取某页 */
            HeapPageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);

            if (page.getNumEmptySlots() == 0){
                Database.getBufferPool().unsafeReleasePage(tid, pid);
                continue;
            }
            page.insertTuple(t);
            modifiedPages.add(page);
            return modifiedPages;
        }

        /* 新建页 */
//        System.out.println("numPages_before:"+numPages());
        byte[] emptyData = HeapPage.createEmptyPageData();
        BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(heapFile,true));
        bw.write(emptyData);
        bw.close();
        // 加载进BufferPool
        HeapPage newPage = (HeapPage) Database.getBufferPool().getPage(tid,
                new HeapPageId(getId(),numPages()-1),Permissions.READ_WRITE);
        newPage.insertTuple(t);
        newPage.markDirty(true, tid);
//        System.out.println("numPages_before:"+numPages());
//        HeapPageId newPid = new HeapPageId(getId(), numPages());
//        HeapPage newPage = new HeapPage(newPid, emptyData);
//        newPage.insertTuple(t);
//        try(RandomAccessFile raf = new RandomAccessFile(heapFile, "rw")){
//            raf.seek(raf.length());
//            raf.write(newPage.getPageData());
//        }
//        System.out.println("numPages_after:"+numPages());
        modifiedPages.add(newPage);
        return modifiedPages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> modifiedPages = new ArrayList<>();

        RecordId rid = t.getRecordId();
        if (rid == null)
            throw new DbException("Tuple is not store in any pages.");
        HeapPageId pid = (HeapPageId) rid.getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        if (!page.isSlotUsed(rid.getTupleNumber()))
            throw new DbException("Tuple slot is already empty.");
        page.deleteTuple(t);
        modifiedPages.add(page);
        return modifiedPages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileItertor(this, tid);
    }
    public static final class HeapFileItertor implements DbFileIterator {
        private HeapFile heapFile;
        private TransactionId tid;
        private Iterator<Tuple> tupleIterator;
        private int index;
        public HeapFileItertor(HeapFile heapFile,TransactionId tid){
            this.heapFile = heapFile;
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            index = 0;
            tupleIterator = getTupleIterator(index);
        }

        private Iterator<Tuple> getTupleIterator(int pgNo) throws DbException, TransactionAbortedException {
            if (pgNo >= 0 && pgNo < heapFile.numPages()){
                HeapPageId pid = new HeapPageId(heapFile.getId(),pgNo);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                return page.iterator();
            }else throw new DbException(String.format("heapFile %d  does not exist in page[%d]!", pgNo,heapFile.getId()));
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(tupleIterator == null)
                return false;
            if(tupleIterator.hasNext())
                return true;
            while (!tupleIterator.hasNext()){
                index++;
                if (index < heapFile.numPages())
                    tupleIterator = getTupleIterator(index);
                else return false;
            }
            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(tupleIterator == null || !tupleIterator.hasNext())
                throw new NoSuchElementException();
            return tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            tupleIterator = null;
        }
    }

}

