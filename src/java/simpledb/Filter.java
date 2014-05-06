package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    DbIterator iter;
    private TupleDesc td;
    private Predicate pred;
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        // some code goes here
	pred = p;
	iter = child;
	td = child.getTupleDesc();
    }

    public Predicate getPredicate() {
        // some code goes here
        return pred;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
	super.open();
	iter.open();
    }

    public void close() {
        // some code goes here
	super.close();
	iter.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
	iter.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
	while (iter.hasNext())
	    {
		Tuple tup = iter.next();
		if (pred.filter(tup))
		    return tup;
	    }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] { iter };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
	if (iter != children[0])
	    iter = children[0];
    }

}
