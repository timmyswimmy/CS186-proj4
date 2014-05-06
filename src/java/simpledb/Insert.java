package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t;
    private DbIterator child;
    private int tableID;
    private TupleDesc td;
    private boolean getNext;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
	this.t = t;
	this.child = child;
	tableID = tableid;
	td = new TupleDesc(new Type[] { Type.INT_TYPE }, 
			   new String[] { "Records" });
	getNext = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
	super.open();
	child.open();
	getNext = true;
    }

    public void close() {
        // some code goes here
	super.close();
	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
	child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
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
        // some code goes here
        if (!getNext)
	    return null;
	int ind = 0;
	while (child.hasNext())
	    {
		Tuple tup = child.next();
		try
		    {
			Database.getBufferPool().insertTuple(t, tableID, tup);
		    }
		catch (IOException e)
		    {
		    }
		ind++;
	    }
	Tuple nextTup = new Tuple(td);
	nextTup.setField(0, new IntField(ind));
	getNext = false;
	return nextTup;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] { child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
	child = children[0];
    }
}
