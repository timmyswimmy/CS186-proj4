package simpledb;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private Map<PageId, Page> pMap;
    private Map<PageId, Date> times;
    private int max;
    private LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
	pMap = new HashMap<PageId, Page>();
	times = new HashMap<PageId, Date>();
	max = numPages;
	lockManager = new LockManager();
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
	long initTime = System.currentTimeMillis();
	boolean hazLock = lockManager.setLock(pid, tid, perm);
	while (!hazLock)
	    {
		long currTime = System.currentTimeMillis();
		if (currTime - initTime > 250) // 0.25 seconds
		    throw new TransactionAbortedException();
		try
		    {
			Thread.sleep(15);
			hazLock = lockManager.setLock(pid, tid, perm);
		    }
		catch (InterruptedException e)
		    {
			e.printStackTrace();
		    }
	    }


        if (pMap.containsKey(pid))
	    {
		times.put(pid, new Date());
		return pMap.get(pid);
	    }		
	Catalog cat = Database.getCatalog();
	Page p = cat.getDbFile(pid.getTableId()).readPage(pid);
	if (pMap.size() >= max)
	    evictPage();
	pMap.put(pid, p);
	times.put(pid, new Date());
	return p;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for proj1
	lockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
	transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for proj1
        return lockManager.holdsLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for proj1
	Collection<Page> pagesss = pMap.values();
	if (commit)
	    {
		for (Page p : pagesss)
		    {
			if (p.isDirty() != null && p.isDirty().equals(tid))
			    {
				flushPage(p.getId());
				p.setBeforeImage();
			    }
			if (p.isDirty() == null)
			    p.setBeforeImage();
		    }
		lockManager.releaseAllLocks(tid);
	    }
	else
	    {
		for (Page p :pagesss)
		    if (p.isDirty() != null && p.isDirty().equals(tid))
			pMap.put(p.getId(), p.getBeforeImage());
		lockManager.releaseAllLocks(tid);
	    }
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
	// proj2
	HeapFile hf = (HeapFile) (Database.getCatalog().getDbFile(tableId));
	ArrayList<Page> pages = hf.insertTuple(tid, t);
	for (Page pagina : pages)
	    {
		pagina.markDirty(true, tid);
		pMap.put(pagina.getId(), pagina);
		times.put(pagina.getId(), new Date());
	    }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
	// proj2
	int tabID = t.getRecordId().getPageId().getTableId();
	HeapFile hf = (HeapFile) (Database.getCatalog().getDbFile(tabID));
	Page pa = hf.deleteTuple(tid, t);
	times.put(pa.getId(), new Date());
	pa.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for proj1
	// proj2
	Set<PageId> pids = pMap.keySet();
	for (PageId pid : pids)
	    flushPage(pid);
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
	// not necessary for proj1
	// proj2
	pMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for proj1
	// proj2
	Page bye = pMap.get(pid);
	TransactionId byebye = bye.isDirty();
	if (byebye != null)
	    {
		Database.getLogFile().logWrite(byebye, bye.getBeforeImage(), bye);
		Database.getLogFile().force();
		Database.getCatalog().getDbFile(pid.getTableId()).writePage(bye);
		bye.markDirty(false, null);
	    }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
	// proj2
	Collection<Page> pages = pMap.values();
	for (Page p : pages)
	    if (p.isDirty() != null && p.isDirty().equals(tid))
		flushPage(p.getId());
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for proj1
	// proj2
	boolean evicted = false;
	while (!evicted)
	    {
		PageId oldPid = null;
		Date oldest = null;
		for (Entry<PageId, Date> stamp : times.entrySet())
		    {
			boolean dirty = pMap.containsKey(stamp.getKey())
			    && pMap.get(stamp.getKey()).isDirty() != null;
			if (dirty)
			    continue;
			if (oldPid == null)
			    {
				oldPid = stamp.getKey();
				oldest = stamp.getValue();
				continue;
			    }
			if (stamp.getValue().before(oldest))
			    {
				oldPid = stamp.getKey();
				oldest = stamp.getValue();
			    }
		    }
		if (oldPid == null)
		    throw new DbException("No candidates for eviction.");
		Page checkPage = pMap.get(oldPid);
		if (checkPage.isDirty() == null)
		    {
			try
			    {
				flushPage(oldPid);
				pMap.remove(oldPid);
				times.remove(oldPid);
				evicted = true;
			    }
			catch (IOException e)
			    {
			    }
		    }
	    }
    }

    private class LockManager{
	private Map<PageId, TransactionId> exclusiveLocks;
	private Map<TransactionId, Set<PageId>> exclusiveLockPages;
	private Map<PageId, Set<TransactionId>> sharedLocks;
	private Map<TransactionId, Set<PageId>> sharedLockPages;

	public LockManager()
	{
	    exclusiveLocks = new HashMap<PageId, TransactionId>();
	    exclusiveLockPages = new HashMap<TransactionId, Set<PageId>>();
	    sharedLocks = new HashMap<PageId, Set<TransactionId>>();
	    sharedLockPages = new HashMap<TransactionId, Set<PageId>>();
	}

	public synchronized boolean setLock(PageId p, TransactionId tid, Permissions per)
	{
	    if (!per.equals(Permissions.READ_ONLY))
		return setExclusiveLock(p, tid);
	    else
		return setSharedLock(p, tid);
	}

	private synchronized boolean setExclusiveLock(PageId p, TransactionId tid)
	{
	    //some code goes here
	    TransactionId eLocks = exclusiveLocks.get(p);
	    Set<TransactionId> sLocks = sharedLocks.get(p);
	    
	    if (sLocks != null && (sLocks.size() > 1 ||
				   sLocks.size() == 1 && !sLocks.contains(tid))
		|| eLocks != null && !eLocks.equals(tid))
		return false;
	    
	    exclusiveLocks.put(p, tid);
	    Set<PageId> xlp = exclusiveLockPages.get(tid);
	    if (xlp == null)
		xlp = new HashSet<PageId>();
	    xlp.add(p);
	    exclusiveLockPages.put(tid, xlp);
	    return true;
	}

	private synchronized boolean setSharedLock(PageId p, TransactionId tid)
	{
	    //some code goes here
	    TransactionId eLock = exclusiveLocks.get(p);
	    Set<TransactionId> sLocks = sharedLocks.get(p);
	    
	    if (eLock == null || eLock.equals(tid))
		{
		    if (sLocks == null)
			sLocks = new HashSet<TransactionId>();
		    sLocks.add(tid);
		    sharedLocks.put(p, sLocks);
		    Set<PageId> slp = sharedLockPages.get(tid);
		    if (slp == null)
			slp = new HashSet<PageId>();
		    slp.add(p);
		    sharedLockPages.put(tid, slp);
		    return true;
		}
	    return false;
	}

	public void releaseLock(PageId p, TransactionId tid)
	{
	    //some code goes here
	    Set<PageId> slp = sharedLockPages.get(tid);
	    Set<TransactionId> slt = sharedLocks.get(p);
	    if (slp != null)
		{
		    slp.remove(p);
		    sharedLockPages.put(tid, slp);
		}
	    if (slt != null)
		{
		    slt.remove(tid);
		    sharedLocks.put(p, slt);
		}
	    Set<PageId> elp = exclusiveLockPages.get(tid);
	    if (elp != null)
		{
		    elp.remove(p);
		    exclusiveLockPages.put(tid, elp);
		}
	    exclusiveLocks.remove(p);
	}

	public boolean holdsLock(PageId p, TransactionId tid)
	{
	    //some code goes here
	    Set<TransactionId> tids = sharedLocks.get(p);
	    if (tids != null && tids.contains(tid))
		return true;
	    TransactionId eltid = exclusiveLocks.get(p);
	    if (eltid != null && eltid.equals(tid))
		return true;
	    return false;
	}

	public synchronized void releaseAllLocks(TransactionId tid)
	{
	    //some code goes here
	    Set<PageId> kSet = new HashSet<PageId>();
	    for (PageId meow : exclusiveLocks.keySet())
		kSet.add(meow);
	    for (PageId rem : kSet)
		{
		    TransactionId t2r = exclusiveLocks.get(rem);
		    if (t2r != null && t2r.equals(tid))
			exclusiveLocks.remove(rem);
		}
	    exclusiveLockPages.remove(tid);

	    for (PageId spid : sharedLocks.keySet())
		{
		    Set<TransactionId> tids = sharedLocks.get(spid);
		    if (tids != null)
			{
			    tids.remove(tid);
			    sharedLocks.put(spid, tids);
			}
		}
	    sharedLockPages.remove(tid);
	}
    }

}
