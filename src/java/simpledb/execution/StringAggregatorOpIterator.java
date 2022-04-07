package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class StringAggregatorOpIterator implements OpIterator {


    private final TupleDesc tupleDesc;
    private Iterator<Tuple> iterator;
    private final List<Tuple> list;
    private boolean open = false;

    public StringAggregatorOpIterator(TupleDesc tupleDesc, List<Tuple> list) {
        this.tupleDesc = tupleDesc;
        this.iterator = list.iterator();
        this.list = list;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.open = true;
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return this.iterator.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        return this.iterator.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.iterator = list.iterator();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    @Override
    public void close() {
        this.open = false;
    }
}
