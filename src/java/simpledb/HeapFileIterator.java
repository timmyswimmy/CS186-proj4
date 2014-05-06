package simpledb;

import java.util.*;

public class HeapFileIterator implements DbFileIterator
{
    private static final long serialVersionUID = 1L;

    Iterator<Tuple> iterator;
    TupleDesc td;
    Iterable<Tuple> tups;

    public HeapFileIterator(TupleDesc td, Iterable<Tuple> tuples)
    {
	this.td = td;
	tups = tuples;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException
    {
	iterator = tups.iterator();
    }

    @Override
    public void close()
    {
	iterator = null;
    }
    
    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException
    {
	return iterator != null && iterator.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException
    {
	if (iterator == null)
	    throw new NoSuchElementException("Iterator not instantiated.");
	Tuple t = iterator.next();
	if (t == null)
	    throw new NoSuchElementException("No more tuples!");
	return t;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException
    {
	close();
	open();
    }

}

