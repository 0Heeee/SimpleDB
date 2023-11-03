package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
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
    private final int gbField;
    private final int afield;
    private final Op aop;
    private final TupleDesc td;
    private final Map<Field, Tuple> tupleList = new HashMap<>();

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbField = gbfield;
        this.afield = afield;
        this.aop = what;
        Type[] types;
        if(gbfield == Aggregator.NO_GROUPING) types = new Type[]{Type.INT_TYPE};
        else types = new Type[]{gbfieldtype, Type.INT_TYPE};
        this.td = new TupleDesc(types);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field field = null;
        int apos = 0;
        if(gbField != Aggregator.NO_GROUPING) {
            field = tup.getField(gbField);
            apos = 1;
        }

        if(aop == Op.COUNT) {
            if(!tupleList.containsKey(field)) {
                Tuple t = new Tuple(td);
                if(gbField == Aggregator.NO_GROUPING) t.setField(0, new IntField(1));
                else {
                    t.setField(0, tup.getField(gbField));
                    t.setField(1, new IntField(1));
                }
                tupleList.put(field, t);
            } else {
                Tuple t = tupleList.get(field);
                int count = ((IntField)t.getField(apos)).getValue()+1;
                t.setField(apos, new IntField(count));
                tupleList.put(field, t);
            }
        } else {
            throw new UnsupportedOperationException(" StringAggregator only supports COUNT");
        }
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
        return new OpIterator() {
            private Iterator<Tuple> it;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                it = tupleList.values().iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return it != null && it.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                return it.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return td;
            }

            @Override
            public void close() {
                it = null;
            }
        };
    }

}
