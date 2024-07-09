package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    /*For plans that implement insert and delete queries,
      the top-most operator is a special Insert or
      Delete operator that modifies the pages on disk.
      These operators return the number of affected tuples.
      This is implemented by returning a single tuple with one integer field,
      containing the count.*/

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private OpIterator child;
    private int tableId;
    private TupleDesc returnTD;
//    private Tuple returnTuple;
    private boolean called;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException, TransactionAbortedException {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        this.returnTD = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"insertCount"});

    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return returnTD;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
        this.called = false;
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (called)
            return null;
        int count = 0; // 记录插入元组的数量
        BufferPool bufferPool = Database.getBufferPool();
        while (child.hasNext()){
            Tuple insertTuple = child.next();
            try{
                bufferPool.insertTuple(tid, tableId, insertTuple);
                count++;
            } catch (IOException e) {
                throw new DbException("insert fail.");
            }
        }

        called = true;
        Tuple returnTuple = new Tuple(returnTD);
        returnTuple.setField(0, new IntField(count));
        return returnTuple;

    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (children.length > 0)
            child = children[0];
    }
}
