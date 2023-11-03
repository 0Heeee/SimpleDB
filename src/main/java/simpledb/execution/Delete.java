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
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private final TupleDesc td;
    private boolean haveDelete = false;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.child = child;
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
        haveDelete = false;
        child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(haveDelete) return null;
        int count = 0;
        BufferPool bufferPool = Database.getBufferPool();
        while (child.hasNext()) {
            Tuple t = child.next();
            try {
                bufferPool.deleteTuple(new TransactionId(), t);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            ++count;
        }
        haveDelete = true;
        Tuple ret = new Tuple(td);
        ret.setField(0, new IntField(count));
        return ret;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{ child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if(child != children[0]) {
            child = children[0];
        }
    }

}
