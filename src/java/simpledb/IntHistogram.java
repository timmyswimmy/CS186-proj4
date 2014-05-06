package simpledb;
import java.util.*;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets, min, max, buckWidth, population;
    private HashMap<Integer, Integer> hist;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	hist = new HashMap<Integer, Integer>();
	this.buckets = buckets;
	this.min = min;
	this.max = max;
	this.population = 0;
	this.buckWidth = 1 + (max - min) / buckets;
	for (int i = 1 ; i <= buckets ; i++)
	    hist.put(i, 0);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
	int hashed = 1 + (v - min) / buckWidth;
	hist.put(hashed, 1 + hist.get(hashed));
	population++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        if (population == 0)
	    return 0;
	if (v < min)
	    {
		if (op.equals(Predicate.Op.NOT_EQUALS) || op.equals(Predicate.Op.GREATER_THAN) || op.equals(Predicate.Op.GREATER_THAN_OR_EQ))
		    return 1.0;
		else
		    return 0.0;
	    }
	if (v > max)
	    {
		if (op.equals(Predicate.Op.NOT_EQUALS) || op.equals(Predicate.Op.LESS_THAN) || op.equals(Predicate.Op.LESS_THAN_OR_EQ))
		    return 1.0;
		else
		    return 0.0;
	    }
	int hashed = 1 + (v - min) / buckWidth, yes = 0;
	
	switch(op)
	    {
	    case GREATER_THAN:
	    case LESS_THAN_OR_EQ:
		    for (int i = hashed + 1 ; i <= buckets ; i++)
			yes += hist.get(i);
		    yes += (hashed * buckWidth + min - v - 1) * hist.get(hashed) / buckWidth;
		    if (op.equals(Predicate.Op.LESS_THAN_OR_EQ))
			yes = population - yes;
		    break;
	    case LESS_THAN:
	    case GREATER_THAN_OR_EQ:
		    for (int i = 1 ; i < hashed ; i++)
			yes += hist.get(i);
		    yes += (v - ((hashed - 1) * buckWidth + min)) * hist.get(hashed) / buckWidth;
		    if (op.equals(Predicate.Op.GREATER_THAN_OR_EQ))
			yes = population - yes;
		    break;
	    case EQUALS:
	    case NOT_EQUALS:
		yes = hist.get(hashed) / buckWidth;
		if(op.equals(Predicate.Op.NOT_EQUALS))
		    yes = population - yes;
	    }
	return ((double) yes) / population;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return hist.toString();
    }
}
