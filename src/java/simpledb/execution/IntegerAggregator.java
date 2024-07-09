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
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private static final Field NO_GROUP_FIELD = new StringField("NO_GROUP_FIELD",20);
    private int gbfieldIndex;
    private Type gbfieldType;
    private int afieldIndex;
    private Op aggOp;
    private TupleDesc tdAfterAgg;

    private boolean NO_GROUPING = false;

    /*groupby and aggregator result*/
    private HashMap<Field, GroupAggRes> groupAggResMap;

    /*Each tuple in the result is a pair of the form (groupValue, aggregateValue)
    * unless the value of the group by field was Aggregator.NO_GROUPING,
    * in which case the result is a single tuple of the form (aggregateValue)*/
    private HashMap<Field, Tuple> resTuplesMap;
//    private TupleDesc resTupleDesc;

    class GroupAggRes {
        /*聚合结果*/
        public int aggRes;
        /*field出现频次*/
        public int count;
        public GroupAggRes(int aggRes,int count){
            this.aggRes = aggRes;
            this.count = count;
        }
    }

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfieldIndex = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afieldIndex = afield;
        this.aggOp = what;
        groupAggResMap = new HashMap<>();
        resTuplesMap = new HashMap<>();
        if (gbfieldType == null) {
            this.NO_GROUPING = true;
            this.tdAfterAgg = new TupleDesc(new Type[]{Type.INT_TYPE},
                    new String[]{"aggregateValue"});
        }else{
            this.tdAfterAgg = new TupleDesc(new Type[]{gbfieldType, Type.INT_TYPE},
                    new String[]{"groupValue", "aggregateValue"});
//            this.groupAggResMap.put(, new groupAggRes(0, 0));
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupByField = NO_GROUPING ? NO_GROUP_FIELD : tup.getField(gbfieldIndex);
//        System.out.println(groupByField);
//        System.out.println(tup);

        /* 边界条件判断 */
        /* tup的分组field与给定分组类型不匹配 */
        if(!NO_GROUPING && groupByField.getType() != gbfieldType){
            throw new IllegalArgumentException("Except groupType is: 「"+ gbfieldType + " 」,But given "+ groupByField.getType());
        }
        /* tup的聚合类型非Int */
        if(!(tup.getField(afieldIndex) instanceof IntField)){
            throw new IllegalArgumentException("Except aggType is: 「 IntField 」" + ",But given "+ tup.getField(afieldIndex).getType());
        }

        Tuple curAggTuple = new Tuple(tdAfterAgg);
        int curAggRes = 0;
        IntField aggField = (IntField) tup.getField(afieldIndex);
        int aggValue = aggField.getValue();

        /* 不需要进行分组操作 */
        /*if (NO_GROUPING){
            curAggTuple.setField(0, aggField);
            resTuplesMap.put(groupByField, curAggTuple);
            return;
        }*/

        GroupAggRes DEFAULT_MAX = new GroupAggRes(Integer.MIN_VALUE, 0);
        GroupAggRes DEFAULT_MIN = new GroupAggRes(Integer.MAX_VALUE, 0);
        GroupAggRes DEFAULT_SUM = new GroupAggRes(0, 0);
//        System.out.println("DEFAULT_SUM:"+DEFAULT_SUM.aggRes+", "+DEFAULT_SUM.count);


        /* 进行分组聚合 */
        switch (aggOp){
            case MIN:
                groupAggResMap.put(groupByField, new GroupAggRes(
                        Math.min(groupAggResMap.getOrDefault(groupByField, DEFAULT_MIN).aggRes, aggValue),
                        groupAggResMap.getOrDefault(groupByField, DEFAULT_MIN).count + 1
                ));
                curAggRes = groupAggResMap.get(groupByField).aggRes;
                break;
            case MAX:
                groupAggResMap.put(groupByField, new GroupAggRes(
                        Math.max(groupAggResMap.getOrDefault(groupByField, DEFAULT_MAX).aggRes, aggValue),
                        groupAggResMap.getOrDefault(groupByField, DEFAULT_MAX).count + 1
                ));
                curAggRes = groupAggResMap.get(groupByField).aggRes;
                break;
            case SUM:
                groupAggResMap.put(groupByField, new GroupAggRes(
                        groupAggResMap.getOrDefault(groupByField, DEFAULT_SUM).aggRes + aggValue,
                        groupAggResMap.getOrDefault(groupByField, DEFAULT_SUM).count + 1
                ));
                curAggRes = groupAggResMap.get(groupByField).aggRes;
                break;
            case AVG:
//                System.out.println("getOrDefault return:"+groupAggResMap.getOrDefault(groupByField, DEFAULT_SUM).aggRes);
                groupAggResMap.put(groupByField, new GroupAggRes(
                        groupAggResMap.getOrDefault(groupByField, DEFAULT_SUM).aggRes + aggValue,
                        groupAggResMap.getOrDefault(groupByField, DEFAULT_SUM).count + 1
                ));
                curAggRes = groupAggResMap.get(groupByField).aggRes / groupAggResMap.get(groupByField).count;
                break;
            case COUNT:
                groupAggResMap.put(groupByField, new GroupAggRes(
                        groupAggResMap.getOrDefault(groupByField, DEFAULT_SUM).aggRes + aggValue,
                        groupAggResMap.getOrDefault(groupByField, DEFAULT_SUM).count + 1
                ));
                curAggRes = groupAggResMap.get(groupByField).count;
                break;
            /* TODO lab7*/
            case SUM_COUNT:
                break;
            case SC_AVG:

        }

//        System.out.println(groupAggResMap.get(groupByField).aggRes+", "+groupAggResMap.get(groupByField).count);
        if (NO_GROUPING){
            curAggTuple.setField(0, new IntField(curAggRes));
            resTuplesMap.put(groupByField, curAggTuple);
        }else {
            curAggTuple.setField(0,groupByField);
            curAggTuple.setField(1,new IntField(curAggRes));
            resTuplesMap.put(groupByField, curAggTuple);
        }


    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        /*throw new
        UnsupportedOperationException("please implement me for lab2");*/
        return new IntAppTupIterator(resTuplesMap);
    }

    public class IntAppTupIterator implements OpIterator{
        private Iterator<HashMap.Entry<Field, Tuple>> iter;
        private HashMap<Field, Tuple> IntAppTupIterator;
        public IntAppTupIterator(HashMap<Field, Tuple> IntAppTupIterator){
            this.IntAppTupIterator = IntAppTupIterator;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            iter = IntAppTupIterator.entrySet().iterator();
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
