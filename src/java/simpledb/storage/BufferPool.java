package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    static class ClockItem {
        private final PageId pageId;
        public boolean flag;

        public ClockItem(PageId pageId, boolean flag) {
            this.pageId = pageId;
            this.flag = flag;
        }

        public PageId getPageId() {
            return pageId;
        }
    }

    private final Map<PageId, Page> pageMap;
    private final int numPages;

    private final ClockItem[] clocks;
    private int clockIndex;

    private final LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.pageMap = new ConcurrentHashMap<>();
        this.clocks = new ClockItem[this.numPages];
        clockIndex = 0;

        this.lockManager = new LockManager();
    }

    private void nextClockIndex() {
        this.clockIndex = (this.clockIndex + 1) % this.numPages;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        this.lockManager.LockPage(pid, tid, perm);
        Page res = pageMap.getOrDefault(pid, null);
        if(res == null) {
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            res = file.readPage(pid);
            if (pageMap.size() >= numPages) {
                this.evictPage();
            }
            pageMap.put(pid, res);
            this.clocks[this.clockIndex] = new ClockItem(pid, true);
            this.nextClockIndex();
        }
        return res;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting to unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        this.lockManager.ReleasePage(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting to unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        this.transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return this.lockManager.holdsLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        try {
            if (commit) {
                this.flushPages(tid);
            } else {
                this.restorePages(tid);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.lockManager.releaseAllLocks(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirties = file.insertTuple(tid, t);
        for (Page page : dirties) {
            if (!this.pageMap.containsKey(page.getId()) && pageMap.size() >= numPages) {
                this.evictPage();
                this.pageMap.put(page.getId(), page);
                this.clocks[this.clockIndex] = new ClockItem(page.getId(), true);
                this.nextClockIndex();
            }
            page.markDirty(true, tid);
            this.pageMap.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> dirties = file.deleteTuple(tid, t);
        for (Page page : dirties) {
            if (!this.pageMap.containsKey(page.getId()) && pageMap.size() >= numPages) {
                this.evictPage();
                this.pageMap.put(page.getId(), page);
                this.clocks[this.clockIndex] = new ClockItem(page.getId(), true);
                this.nextClockIndex();
            }
            page.markDirty(true, tid);
            this.pageMap.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (Map.Entry<PageId, Page> entry : pageMap.entrySet()) {
            if (entry.getValue().isDirty() != null) {
                flushPage(entry.getKey());
            }
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.

        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pageMap.remove(pid);
        for (int i = 0; i < clocks.length; i++) {
            if (clocks[i] != null && clocks[i].pageId.equals(pid)) {
                clocks[i] = null;
                this.clockIndex = i;
                break;
            }
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = pageMap.getOrDefault(pid, null);
        if (page == null) return;
        if (page.isDirty() == null) return;

        Database.getLogFile().logWrite(page.isDirty(), page.getBeforeImage(), page);
        Database.getLogFile().force();
        Database.getCatalog().getDatabaseFile(page.getId().getTableId()).writePage(page);
        page.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pageId : pageMap.keySet()) {
            Page page = this.pageMap.get(pageId);
            page.setBeforeImage();
            if (page.isDirty() == tid) {
                flushPage(pageId);
            }
        }
    }

    /** Restore all pages of the specified transaction from disk.
     */
    public synchronized void restorePages(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pageId : pageMap.keySet()) {
            Page page = this.pageMap.get(pageId);
            if (page.isDirty() == tid) {
                DbFile file = Database.getCatalog().getDatabaseFile(pageId.getTableId());
                this.pageMap.put(pageId, file.readPage(pageId));
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        Set<PageId> pageIdSet = new HashSet<>();
        while(true) {
            if (this.clocks[this.clockIndex] == null) {
                break;
            }
            if (this.pageMap.get(this.clocks[this.clockIndex].pageId).isDirty() != null) {
                this.nextClockIndex();
                pageIdSet.add(this.clocks[this.clockIndex].pageId);
                if (pageIdSet.containsAll(this.pageMap.keySet())) {
                    throw new DbException("All dirty page");
                }
            }
            if (this.clocks[this.clockIndex].flag) {
                this.clocks[this.clockIndex].flag = false;
                this.nextClockIndex();
            } else {
                try {
                    this.flushPage(this.clocks[this.clockIndex].pageId);
                    this.pageMap.remove(this.clocks[this.clockIndex].pageId);
                    this.clocks[this.clockIndex] = null;
                    break;
                } catch (IOException e) {
                    throw new DbException("Flushes the page to disk fail");
                }
            }
        }
    }

}
