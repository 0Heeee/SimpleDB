package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.common.Type;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private final JoinPredicate predicate;
    private OpIterator child1;
    private OpIterator child2;
    private final TupleDesc td1;
    private final TupleDesc td2;
    private final TupleDesc td;
    private final String fieldName1;
    private final String fieldName2;
    private Tuple t1 = null;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        this.predicate = p;
        this.child1 = child1;
        this.child2 = child2;
        this.td1 = child1.getTupleDesc();
        this.td2 = child2.getTupleDesc();
        int num = td1.numFields() + td2.numFields();
        Type[] types = new Type[num];
        String[] fields = new String[num];
        for (int i = 0; i < td1.numFields(); i++) {
            types[i] = td1.getFieldType(i);
            fields[i] = td1.getFieldName(i);
        }
        for (int i = 0; i < td2.numFields(); i++) {
            types[td1.numFields() + i] = td2.getFieldType(i);
            fields[td1.numFields() + i] = td2.getFieldName(i);
        }
        this.td = new TupleDesc(types, fields);
        this.fieldName1 = td1.getFieldName(p.getField1());
        this.fieldName2 = td2.getFieldName(p.getField2());
    }

    public JoinPredicate getJoinPredicate() {
        return this.predicate;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        return this.fieldName1;
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        return this.fieldName2;
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child1.open();
        child2.open();
        super.open();
    }

    public void close() {
        super.close();
        child2.close();
        child1.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        t1 = null;
        child1.rewind();
        child2.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(t1 == null) {
            if(child1.hasNext()) t1 = child1.next();
            else throw new DbException("child1 error!");
        }
        while(child2.hasNext()) {
            Tuple t2 = child2.next();
            if(predicate.filter(t1, t2)) {
                Tuple t = new Tuple(this.td);
                for (int i = 0; i < td1.numFields(); i++) {
                    t.setField(i, t1.getField(i));
                }
                for (int i = 0; i < td2.numFields(); i++) {
                    t.setField(td1.numFields()+i, t2.getField(i));
                }
                return t;
            }
        }
        child2.rewind();
        if(child1.hasNext()) t1 = child1.next();
        else return null;
        return fetchNext();
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{ this.child1, this.child2 };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if(children.length != 2) throw new NoSuchElementException("input children error!");
        this.child1 = children[0];
        this.child2 = children[1];
    }

}
