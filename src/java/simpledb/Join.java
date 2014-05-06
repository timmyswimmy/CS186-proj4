package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate jp;
    private DbIterator i1, i2;
    private Tuple in, out; // for fetchNext() method

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
	jp = p;
	i1 = child1;
	i2 = child2;
	in = null;
	out = null;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return jp;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return i1.getTupleDesc().getFieldName(jp.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return i2.getTupleDesc().getFieldName(jp.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(i1.getTupleDesc(), i2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
	super.open();
	i1.open();
	i2.open();
    }

    public void close() {
        // some code goes here
	super.close();
	i1.close();
	i2.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
	i1.rewind();
	i2.rewind();
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
        // some code goes here
        if (i1.hasNext() && out == null)
	    out = i1.next();
	while (out != null)
	    {
		while (i2.hasNext())
		    {
			in = i2.next();
			if (jp.filter(out, in))
			    {
				int i = 0;
				Tuple joined = new Tuple(TupleDesc.merge(out.getTupleDesc(), in.getTupleDesc()));
				for (i = 0 ; i < out.getTupleDesc().numFields() ; i++)
				    joined.setField(i, out.getField(i));
				for (int j = 0 ; j < in.getTupleDesc().numFields() ; j++)
				    joined.setField(i + j, in.getField(j));
				//System.out.println(joined.toString());
				return joined;
			    }
		    }
		if (i1.hasNext())
		    out = i1.next();
		else
		    out = null;
		i2.rewind();
	    }
	return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] { i1, i2 };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
	i1 = children[0];
	i2 = children[1];
    }

}
