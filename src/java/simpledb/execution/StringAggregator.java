package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private static final Field NO_GROUP_FIELD = new StringField("NO_GROUP_FIELD",20);
    private boolean NO_GROUPING = false;
    private int gbfieldIndex;
    private Type gbfieldType;
    private int afieldIndex;
    private Op countOp;
    private TupleDesc tdAfterAgg;
    private HashMap<Field, Integer> countResMap;
    private HashMap<Field, Tuple>  resTuplesMap;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfieldIndex = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afieldIndex = afield;
        this.countOp = what;
        countResMap = new HashMap<>();
        resTuplesMap = new HashMap<>();
        if (countOp != Op.COUNT)
            throw new IllegalArgumentException("Op is not COUNT");
        if (gbfieldType == null) {
            NO_GROUPING = true;
            this.tdAfterAgg = new TupleDesc(new Type[]{Type.STRING_TYPE},
                    new String[]{"aggregateValue"});
        }else{
            this.tdAfterAgg = new TupleDesc(new Type[]{gbfieldType, Type.INT_TYPE},
                    new String[]{"groupValue", "aggregateValue"});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupByField = NO_GROUPING ? NO_GROUP_FIELD: tup.getField(gbfieldIndex);

        /* 边界条件判断 */
        /* tup的分组field与给定分组类型不匹配 */
        if(!NO_GROUPING && groupByField.getType() != gbfieldType){
            throw new IllegalArgumentException("Except groupType is: 「"+ gbfieldType + " 」,But given "+ groupByField.getType());
        }
        /* tup的聚合类型非Int */
        if(!(tup.getField(afieldIndex) instanceof StringField)){
            throw new IllegalArgumentException("Except aggType is: 「 StringField 」" + ",But given "+ tup.getField(afieldIndex).getType());
        }

        Tuple curCountTuple = new Tuple(tdAfterAgg);
        if(!NO_GROUPING){
            curCountTuple.setField(0, new IntField(0));
            resTuplesMap.put(groupByField, curCountTuple);
        }
        countResMap.put(groupByField, countResMap.getOrDefault(groupByField, 0) + 1);
        curCountTuple.setField(0, groupByField);
        curCountTuple.setField(1, new IntField(countResMap.get(groupByField)));
//        resTuplesMap.put(groupByField, curCountTuple);

    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        /*throw new UnsupportedOperationException("please implement me for lab2");*/
        return new StringAggTupIterateor(resTuplesMap);
    }

    public final class StringAggTupIterateor implements OpIterator{
        private Iterator<Map.Entry<Field, Tuple>> iter;
        private HashMap<Field, Tuple> StringAggTupIterator;
        public StringAggTupIterateor(HashMap<Field, Tuple> StringAggTupIterator){
            this.StringAggTupIterator = StringAggTupIterator;
        }
        @Override
        public void open() throws DbException, TransactionAbortedException {
            iter = StringAggTupIterator.entrySet().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return iter.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return iter.next().getValue();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return tdAfterAgg;
        }

        @Override
        public void close() {
            iter = null;
        }
    }

}
