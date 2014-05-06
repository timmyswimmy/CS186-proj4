package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private List<TDItem> fieldItems;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldType + "(" + fieldName + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
	Iterator<TDItem> iter =  fieldItems.iterator();
        return iter;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
	fieldItems = new ArrayList<TDItem>();
	for(int i = 0 ; i < typeAr.length ; i++)
	    {
		TDItem item = new TDItem(typeAr[i], fieldAr[i]);
		fieldItems.add(item);
	    }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
	fieldItems = new ArrayList<TDItem>();
	for(int i = 0 ; i < typeAr.length ; i++)
	    {
		TDItem item = new TDItem(typeAr[i], null);
		fieldItems.add(item);
	    }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return fieldItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i > fieldItems.size() - 1)
	    throw new NoSuchElementException("Index not valid.");
	return fieldItems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i > fieldItems.size() - 1)
	    throw new NoSuchElementException("Index not valid.");
	return fieldItems.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
	for (int i = 0 ; i < fieldItems.size() ; i++)
	    {
		TDItem t = fieldItems.get(i);
		if ((t.fieldName != null && t.fieldName.equals(name)) 
		    || t.fieldName == null && name == null)
		    return i;
	    }
	throw new NoSuchElementException("No match.");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
	for (TDItem t : fieldItems)
	    {
		size += 4;
		if (t.fieldType.equals(Type.STRING_TYPE))
		    size += 124;
	    }
	return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
	int totalNumFields = td1.numFields() + td2.numFields();
	Type[] types = new Type[totalNumFields];
	String[] fields = new String[totalNumFields];
	int totalInd;

        for (totalInd = 0 ; totalInd < td1.numFields() ; totalInd++)
      	    {
       		types[totalInd] = td1.getFieldType(totalInd);
       		fields[totalInd] = td1.getFieldName(totalInd);
       	    }
       	for (int i = 0 ; i < td2.numFields() ; i++)
       	    {
       		types[totalInd] = td2.getFieldType(i);
       		fields[totalInd] = td2.getFieldName(i);
       		totalInd++;
       	    }
	return new TupleDesc(types, fields);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if(o instanceof TupleDesc)
	    {
		TupleDesc tup = (TupleDesc) o;
		int num = this.numFields();
		if (num != tup.numFields())
		    return false;
		else
		    {
			for (int i = 0 ; i < num ; i++)
			    if (!this.getFieldType(i).equals(tup.getFieldType(i)))
				return false;
		    }
		return true;
	    }
	else
	    return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String str = new String();
	int numFld = this.numFields();
	if (numFld == 0)
	    return str;
	for (int i = 0 ; i < numFld ; i++)
	    {
		//str += this.getFieldType(i) + "[" + i + "](" + this.getFieldName(i) + "[" + i + "]), ";
		str += fieldItems.get(i).toString() + ", ";
	    }
	str = str.substring(0, str.length() - 2);
	return str;
    }
}