package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.Type;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private final int afield;
    private final String afieldName;
    private final int gfield;
    private final String gfieldName;
    private final Aggregator.Op aop;
    private final TupleDesc td;
    private Aggregator aggregator = null;


    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        TupleDesc origin_td = child.getTupleDesc();
        this.afieldName = origin_td.getFieldName(afield);
        if(gfield != Aggregator.NO_GROUPING) {
            this.gfieldName = origin_td.getFieldName(gfield);
            Type[] types = new Type[]{ origin_td.getFieldType(gfield), origin_td.getFieldType(afield) };
            String [] fields = new String[]{ afieldName, gfieldName };
            this.td = new TupleDesc(types, fields);
        } else {
            this.gfieldName = null;
            Type[] types = new Type[]{ origin_td.getFieldType(afield) };
            String [] fields = new String[]{ afieldName };
            this.td = new TupleDesc(types, fields);
        }
        Type gType = null;
        if(gfield != Aggregator.NO_GROUPING) gType = origin_td.getFieldType(gfield);
        if(origin_td.getFieldType(afield) == Type.INT_TYPE) {
            this.aggregator = new IntegerAggregator(gfield, gType, afield, aop);
        } else if(origin_td.getFieldType(afield) == Type.STRING_TYPE) {
            this.aggregator = new StringAggregator(gfield, gType, afield, aop);
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        return this.gfieldName;
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        return this.afieldName;
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        child.open();
        super.open();
        while(child.hasNext()) {
            Tuple t = child.next();
            aggregator.mergeTupleIntoGroup(t);
        }
        child = aggregator.iterator();
        child.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(child.hasNext()) return child.next();
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    public void close() {
        super.close();
        child.close();
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{ this.child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if(this.child != children[0]) {
            this.child = children[0];
        }
    }

}
