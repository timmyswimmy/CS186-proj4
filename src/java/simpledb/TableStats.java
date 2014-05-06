package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.*;
import java.io.*;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing proj1 and proj2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    
    private ArrayList<Object> hist;
    private HeapFile hf;
    private DbFileIterator iter;
    private int io, population;
    private HashMap<String, Integer> min, max;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
	hist = new ArrayList<Object>();
	io = ioCostPerPage;
	population = 0;
	min = new HashMap<String, Integer>();
	max = new HashMap<String, Integer>();

	Transaction trans = new Transaction();
	trans.start();
	this.hf = (HeapFile) (Database.getCatalog().getDbFile(tableid));
	this.iter = hf.iterator(trans.getId());
	try
	    {
		iter.open();
		bounds();
		generateHistogram();
		iter.close();
	    }
	catch (TransactionAbortedException e1)
	    {
		e1.printStackTrace();
	    }
	catch (DbException e2)
	    {
		e2.printStackTrace();
	    }
	try
	    {
		trans.commit();
	    }
	catch (IOException e3)
	    {
		e3.printStackTrace();
	    }
    }

    private void bounds() throws TransactionAbortedException, DbException
    {
	iter.rewind();
	population = 0;
	while (iter.hasNext())
	    {
		Tuple t = iter.next();
		TupleDesc td = t.getTupleDesc();
		for (int i = 0 ; i < td.numFields() ; i++)
		    {
			Field f = t.getField(i);
			if (f.getType().equals(Type.INT_TYPE))
			    {
				// We can compare INT_TYPES.
				String fname = td.getFieldName(i);
				Integer minimum = min.get(fname), maximum = max.get(fname), fval = ((IntField) f).getValue();
				if (minimum == null || fval < minimum)
				    min.put(fname, fval);
				if (maximum == null || fval > maximum)
				    max.put(fname, fval);
			    }
		    }
		population++;
	    }
    }

    private void generateHistogram() throws TransactionAbortedException, DbException
    {
	iter.rewind();
	while (iter.hasNext())
	    {
		Tuple t = iter.next();
		TupleDesc td = t.getTupleDesc();
		for (int i = 0 ; i < td.numFields() ; i++)
		    {
			Field f = t.getField(i);
			if (f.getType().equals(Type.INT_TYPE))
			    {
				IntField intf = (IntField) f;
				if (i >= hist.size())
				    {
					String fname = td.getFieldName(i);
					hist.add(new IntHistogram(NUM_HIST_BINS, min.get(fname), max.get(fname)));
				    }
					IntHistogram ih = (IntHistogram) (hist.get(i));
					ih.addValue(intf.getValue());
					hist.set(i, ih);
			    }
			else
			    {
				StringField strf = (StringField) f;
				if (i >= hist.size())
				    {
					String fname = td.getFieldName(i);
					hist.add(new StringHistogram(NUM_HIST_BINS));
				    }
					StringHistogram sh = (StringHistogram) (hist.get(i));
					sh.addValue(strf.getValue());
					hist.set(i, sh);
			    }
		    }
	    }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return (double) (this.io * hf.numPages());
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (selectivityFactor * population);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        if (hf.getTupleDesc().getFieldType(field).equals(Type.INT_TYPE))
	    {
		IntHistogram ih = (IntHistogram) (hist.get(field));
		return ih.avgSelectivity();
	    }
	StringHistogram sh = (StringHistogram) (hist.get(field));
	return sh.avgSelectivity();
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        if (constant.getType().equals(Type.INT_TYPE))
	    {
		IntField intf = (IntField) constant;
		int fvalue = intf.getValue();
		IntHistogram ih = (IntHistogram) (hist.get(field));
		return ih.estimateSelectivity(op, fvalue);
	    }
	else
	    {
		StringField strf = (StringField) constant;
		String fvalue = strf.getValue();
		StringHistogram sh = (StringHistogram) (hist.get(field));
		return sh.estimateSelectivity(op, fvalue);
	    }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return population;
    }

}
