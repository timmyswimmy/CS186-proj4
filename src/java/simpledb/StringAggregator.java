package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield, afield;
    private Type gbfieldtype;
    
    private Map<Field, Field> agg;

    private String aggName, gbName;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
	if (!what.equals(Op.COUNT))
	    throw new IllegalArgumentException("Operator must be COUNT.");
	this.gbfield = gbfield;
	this.gbfieldtype = gbfieldtype;
	this.afield = afield;
	agg = new HashMap<Field, Field>();
	aggName = new String();
	gbName = new String();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
	aggName = tup.getTupleDesc().getFieldName(afield);
	Field key = new IntField(NO_GROUPING);
	if (gbfield != NO_GROUPING)
	    {
		key = tup.getField(gbfield);
		gbName = tup.getTupleDesc().getFieldName(gbfield);
	    }
	int count = 1;
	IntField aggVal = (IntField) (agg.get(key));
	if (aggVal != null)
	    count = aggVal.getValue() + 1;
	agg.put(key, new IntField(count));
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
	List<Tuple> tups = new ArrayList<Tuple>();
	TupleDesc td;
	if (gbfield == NO_GROUPING)
	    {
		td = new TupleDesc(new Type[] { Type.INT_TYPE }, 
				  new String[] { aggName });
		Tuple t = new Tuple(td);
		t.setField(0, agg.get(new IntField(NO_GROUPING)));
		tups.add(t);
	    }
	else
	    {
		td = new TupleDesc(new Type[] { gbfieldtype, Type.INT_TYPE },
				   new String[] { gbName, aggName });
		Set<Field> meep = agg.keySet();
		for (Field f : meep)
		    {
			Tuple t = new Tuple(td);
			t.setField(0, f);
			t.setField(1, agg.get(f));
			tups.add(t);
		    }
	    }
	return new TupleIterator(td, tups);
    }

}
