package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield, afield;
    private Type gbfieldtype;
    private Op op;
    
    private Map<Field, Field> agg;
    private Map<Field, Integer> sums, counts; // For AVG.

    private String aggName, gbName;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
	this.gbfield = gbfield;
	this.gbfieldtype = gbfieldtype;
	this.afield = afield;
	op = what;
	agg = new HashMap<Field, Field>();
	sums = new HashMap<Field, Integer>();
	counts = new HashMap<Field, Integer>();
	aggName = new String();
	gbName = new String();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
	aggName = tup.getTupleDesc().getFieldName(afield);
	Field key;
	if (gbfield != NO_GROUPING)
	    {
		key = tup.getField(gbfield);
		gbName = tup.getTupleDesc().getFieldName(gbfield);
	    }
	else
	    key = new IntField(NO_GROUPING);
	IntField aggField = (IntField) (tup.getField(afield)), 
	    aggVal = (IntField) (agg.get(key));
	
	if (op.equals(Op.COUNT))
	    {
		int count = 1;
		if (aggVal != null)
		    count = aggVal.getValue() + 1;
		agg.put(key, new IntField(count));
	    }

	if (op.equals(Op.SUM))
	    {
		int sum = 0;
		if (aggVal != null)
		    sum = aggVal.getValue();
		agg.put(key, new IntField(sum + aggField.getValue()));
	    }

	if (op.equals(Op.MAX))
	    {
		int max = aggField.getValue();
		if (aggVal != null)
		    max = Math.max(max, aggVal.getValue());
		agg.put(key, new IntField(max));
	    }

	if (op.equals(Op.MIN))
	    {
		int min = aggField.getValue();
		if (aggVal != null)
		    min = Math.min(min, aggVal.getValue());
		agg.put(key, new IntField(min));
	    }

	if (op.equals(Op.AVG))
	    {
		int count = 1, avg = aggField.getValue(), sum = aggField.getValue();
		if (aggVal == null)
		    {
			counts.put(key, count);
			sums.put(key, sum);
		    }
		else
		    {
			count = counts.get(key) + 1;
			sum = sums.get(key) + aggField.getValue();
			avg = sum / count;
			counts.put(key, count);
			sums.put(key, sum);
		    }
		agg.put(key, new IntField(avg));
	    }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
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
