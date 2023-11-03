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

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private final TupleDesc td;
    private final int tableId;
    private boolean haveInsert = false;

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
            throws DbException {
        if(!Database.getCatalog().getTupleDesc(tableId).equals(child.getTupleDesc())) {
            throw new DbException("TupleDesc of child differs from table into which we are to insert.");
        }
        this.child = child;
        this.tableId = tableId;
        Type[] types = new Type[]{ Type.INT_TYPE };
        this.td = new TupleDesc(types);
    }

    public TupleDesc getTupleDesc() {
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        super.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        haveInsert = false;
        child.rewind();
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
        if(haveInsert) return null;
        int count = 0;
        BufferPool bufferPool = Database.getBufferPool();
        while(child.hasNext()) {
            Tuple t = child.next();
            try {
                bufferPool.insertTuple(new TransactionId(), tableId, t);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            ++count;
        }
        haveInsert = true;
        Tuple ret = new Tuple(td);
        ret.setField(0, new IntField(count));
        return ret;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{ this.child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if(child != children[0]) {
            child = children[0];
        }
    }
}
