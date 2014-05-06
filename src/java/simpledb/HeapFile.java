package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
	this.f = f;
	this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        if (getId() != pid.getTableId())
	    throw new IllegalArgumentException("Invalid Page ID.");
	int pgSize = BufferPool.PAGE_SIZE;
	byte[] info = new byte[pgSize];
	HeapPage hp = null;
	try
	    {
		RandomAccessFile raf = new RandomAccessFile(f, "r");
		raf.seek(pid.pageNumber() * pgSize);
		raf.read(info, 0, pgSize);
		raf.close();
		hp = new HeapPage((HeapPageId) pid, info);
	    }
	catch (IOException e)
	    {
		e.printStackTrace();
	    }
	return hp;
	//throw new IllegalArgumentException("Something went wrong.");
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
	// proj2
	int pageNo = page.getId().pageNumber();
	byte[] data = page.getPageData();
	RandomAccessFile raf = new RandomAccessFile(f, "rw");
	int pgSize = BufferPool.PAGE_SIZE;
	raf.seek(pageNo * pgSize);
	raf.write(data, 0, pgSize);
	raf.close();
	page.markDirty(false, null);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (Math.ceil(f.length() / BufferPool.PAGE_SIZE));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
	// proj2
	ArrayList<Page> ans = new ArrayList<Page>();
	int pageNo;
	for (pageNo = 0 ; pageNo < numPages() ; pageNo++)
	    {
		PageId pid = new HeapPageId(getId(), pageNo);
		HeapPage hp = (HeapPage) (Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE));
		if (hp.getNumEmptySlots() != 0)
		    {
			hp.insertTuple(t);
			ans.add(hp);
			return ans;
		    }
	    }
	HeapPageId newPid = new HeapPageId(getId(), pageNo);
	HeapPage newP = new HeapPage(newPid, HeapPage.createEmptyPageData());
	newP.insertTuple(t);
	int pgSize = BufferPool.PAGE_SIZE;
	RandomAccessFile raf = new RandomAccessFile(f, "rw");
	raf.seek(pageNo * pgSize);
	raf.write(newP.getPageData(), 0, pgSize);
	raf.close();
	ans.add(newP);
	return ans;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
	// proj2
	HeapPage hp = (HeapPage) (Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE));
	hp.deleteTuple(t);
	return hp;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
	// DbFileIterator ans;
        // List<HeapPage> hps = new ArrayList<HeapPage>();
	// for (int i = 0 ; i < numPages() ; i++)
	//     {
	// 	try
	// 	    {
	// 		Page page = Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_ONLY);
	// 		hps.add((HeapPage) page);
	// 	    }
	// 	catch (TransactionAbortedException e)
	// 	    {
	// 	    }
	// 	catch (DbException e2)
	// 	    {
	// 	    }
	//     }
	// List<Tuple> tups = new ArrayList<Tuple>();
	// for (HeapPage p : hps)
	//     {
	// 	Iterator<Tuple> iter = p.iterator();
	// 	while (iter.hasNext())
	// 	    tups.add(iter.next());
	//     }
	// ans = new HeapFileIterator(td, tups);
	// System.out.println(tups.size());
	// return ans;
	return new HeapFileIterator(tid);
    }
    
    private class HeapFileIterator implements DbFileIterator
    {

	private static final long serialVersionUID = 1L;
	
	private TransactionId tid;
	private Iterator<Tuple> iter;
	private int pageNo;
	private boolean open;

	public HeapFileIterator(TransactionId transId)
	{
	    this.tid = transId;
	    pageNo = 0;
	}

	@Override
	public void open() throws DbException, TransactionAbortedException
	{
	    HeapPage hp = (HeapPage) (Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pageNo), Permissions.READ_ONLY));
	    iter = hp.iterator();
	    open = true;
	}

	@Override
	public boolean hasNext() throws DbException, TransactionAbortedException
	{
	    if (open)
		{
		    if (iter == null)
			return false;
		    if (iter.hasNext())
			return true;
		    BufferPool bp = Database.getBufferPool();
		    while (pageNo < numPages() - 1)
			{
			    pageNo++;
			    HeapPageId meep = new HeapPageId(getId(),pageNo);
			    HeapPage hp = (HeapPage) (bp.getPage(tid, meep, Permissions.READ_ONLY));
			    iter = hp.iterator();
			    if (iter.hasNext())
				return true;
			}
		}
	    return false;
	}

	@Override
	public Tuple next() throws DbException, TransactionAbortedException
	{
	    if (hasNext())
		return iter.next();
	    throw new NoSuchElementException();
	}

	@Override
	public void close()
	{
	    pageNo = 0;
	    iter = null;
	    open = false;
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException
	{
	    close();
	    open();
	}
    }

}

