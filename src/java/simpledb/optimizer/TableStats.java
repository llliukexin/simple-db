package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    private int tableId;
    private int ioCostPerPage;
    private ConcurrentHashMap<Integer, IntHistogram> intHistogramMap;
    private ConcurrentHashMap<Integer, StringHistogram> strHistogramMap;
    private DbFile dbFile;
    private TupleDesc tupleDesc;
    private int numTuples;
    private int numPages;

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }



    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.

        // some code goes here
        this.tableId = tableid;
        this.ioCostPerPage = ioCostPerPage;
        this.intHistogramMap = new ConcurrentHashMap<>();
        this.strHistogramMap = new ConcurrentHashMap<>();
        this.dbFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        this.tupleDesc = dbFile.getTupleDesc();
        this.numTuples = 0;

        // 初始化直方图,为每一个属性都创建一个直方图（求min、max...）
        for (int i = 0; i < tupleDesc.numFields(); i++){
            // int_type
            if (tupleDesc.getFieldType(i) == Type.INT_TYPE){
                int min = Integer.MAX_VALUE;
                int max = Integer.MIN_VALUE;

                DbFileIterator iter = dbFile.iterator(new TransactionId());
                try{
                    iter.open();
                    while (iter.hasNext()){
                        Tuple tuple = iter.next();
                        int value = ((IntField) tuple.getField(i)).getValue();
                        if (value < min) min = value;
                        else if (value > max) max = value;
                    }
                } catch (TransactionAbortedException e) {
                    throw new RuntimeException(e);
                } catch (DbException e) {
                    throw new RuntimeException(e);
                } finally {
                    iter.close();
                }
                intHistogramMap.put(i, new IntHistogram(NUM_HIST_BINS, min, max));
            }
            // string_type
            else if (tupleDesc.getFieldType(i) == Type.STRING_TYPE) {
                strHistogramMap.put(i, new StringHistogram(NUM_HIST_BINS));
            }
        }

        // 向直方图中添加tuple，并统计tuple数量
        DbFileIterator iter = dbFile.iterator(new TransactionId());
        try{
            iter.open();
            while (iter.hasNext()){
                Tuple tuple = iter.next();
                this.numTuples++;
                // 将tuple的各个field添加至其相应的Histogram
                for (int i = 0;i < tupleDesc.numFields();i++){
                    if (tupleDesc.getFieldType(i) == Type.INT_TYPE){
                        intHistogramMap.get(i).addValue(((IntField)tuple.getField(i)).getValue());
                    } else if (tupleDesc.getFieldType(i) == Type.STRING_TYPE) {
                        strHistogramMap.get(i).addValue(((StringField)tuple.getField(i)).getValue());
                    }
                }
            }
        } catch (TransactionAbortedException e) {
            throw new RuntimeException(e);
        } catch (DbException e) {
            throw new RuntimeException(e);
        }

        this.numPages = ((HeapFile)dbFile).numPages();
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return numPages * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (numTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        double avgSelec = 1.0;
        if (tupleDesc.getFieldType(field) == Type.INT_TYPE){
            avgSelec = intHistogramMap.get(field).avgSelectivity();
        } else if (tupleDesc.getFieldType(field) == Type.STRING_TYPE) {
            avgSelec = strHistogramMap.get(field).avgSelectivity();
        }
        return avgSelec;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        double selectivity = 1.0;
        if (tupleDesc.getFieldType(field) == Type.INT_TYPE){
            selectivity = intHistogramMap.get(field).estimateSelectivity(op, ((IntField)constant).getValue());
        } else if (tupleDesc.getFieldType(field) == Type.STRING_TYPE) {
            selectivity = strHistogramMap.get(field).estimateSelectivity(op, ((StringField)constant).getValue());
        }
        return selectivity;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return numTuples;
    }

}
