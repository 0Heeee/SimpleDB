package simpledb.storage;

import simpledb.common.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    private final Map<PageId, Integer> pidToBpidMap;
    private final Page[] pages;
    private final Queue<Integer> emptyPages;
    private final LockManager lockManager;
    private final Map<TransactionId, List<Page>> tidToPagesMap;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        pages = new Page[numPages];
        pidToBpidMap = new LinkedHashMap<>();
        emptyPages = new LinkedList<>();
        for (int i = 0; i < numPages; i++) {
            emptyPages.add(i);
        }
        lockManager = new LockManager();
        tidToPagesMap = new HashMap<>();
    }
    
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
        // lockType为要获取的锁的类型
        LockManager.PageLock.LockType acquireType;
        if(perm == Permissions.READ_WRITE) {
            acquireType = LockManager.PageLock.LockType.EXCLUSIVE;
        } else {
            acquireType = LockManager.PageLock.LockType.SHARE;
        }
        // 循环获取锁，若时间超过50ms则等待超时，获取锁失败
        long start = System.currentTimeMillis();
        while (true) {
            try {
                if (lockManager.acquireLock(pid, tid, acquireType)) {
                    break;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long now = System.currentTimeMillis();
            if (now - start > 50) {
                throw new TransactionAbortedException();
            }
        }
//        Boolean success = false;
//        final ExecutorService exec = Executors.newSingleThreadExecutor();
//        Callable<Boolean> call = () -> {
//            while(true) {
//                try {
//                    if (lockManager.acquireLock(pid, tid, acquireType)) {
//                        return true;
//                    }
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        };
//        try {
//            Future<Boolean> future = exec.submit(call);
//            success = future.get(500, TimeUnit.MILLISECONDS);
//        } catch (Exception e) {
////            e.printStackTrace();
//        }
//        exec.shutdown();
//        if(!success) throw new TransactionAbortedException();
        // 成功获取锁，查找所需要的Page
        int bpid = getBufferPageId(pid);
        // 若要写Page，则将该Page添加到tid事务相关的PageList中
        if(perm == Permissions.READ_WRITE) {
            List<Page> changedPages = tidToPagesMap.get(tid);
            if(changedPages == null) {
                tidToPagesMap.put(tid, new ArrayList<Page>(){});
                changedPages = tidToPagesMap.get(tid);
            }
            changedPages.add(pages[bpid]);
        }
        return pages[bpid];
    }

    private int getBufferPageId(PageId pid) throws DbException {
        int bpid;
        if(!pidToBpidMap.containsKey(pid)) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            if(emptyPages.isEmpty()) evictPage();
            int emptyPage = emptyPages.remove();
            pages[emptyPage] = dbFile.readPage(pid);
            pidToBpidMap.put(pid, emptyPage);
            bpid = emptyPage;
        } else {
            bpid = pidToBpidMap.get(pid);
            if(bpid < 0 || bpid >= pages.length) {
                throw new DbException("BPageId is not allowed.");
            }
        }
        return bpid;
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
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        lockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        if(commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            // 回滚事务
            try {
                recoverPages(tid);
            } catch (DbException e) {
                e.printStackTrace();
            }
        }
        lockManager.completeTransaction(tid);
    }

    private void recoverPages(TransactionId tid) throws DbException {
        List<Page> changedPages = tidToPagesMap.get(tid);
        if(changedPages == null) return;
        for (Page changedPage : changedPages) {
            PageId pageId = changedPage.getId();
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pageId.getTableId());
            Page oldPage = dbFile.readPage(pageId);
            pages[getBufferPageId(pageId)] = oldPage;
            changedPage.markDirty(false, tid);
        }
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
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> changedPages = dbFile.insertTuple(tid, t);
        // update BufferPool
        tidToPagesMap.put(tid, changedPages);
        updateBufferPool(tid);
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
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> changedPages = dbFile.deleteTuple(tid, t);
        // update BufferPool
        tidToPagesMap.put(tid, changedPages);
        updateBufferPool(tid);
    }

    private void updateBufferPool(TransactionId tid) throws DbException {
        List<Page> changedPages = tidToPagesMap.get(tid);
        for (Page changedPage : changedPages) {
            changedPage.markDirty(true, tid);
            int bpid = getBufferPageId(changedPage.getId());
            pages[bpid] = changedPage;
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        pidToBpidMap.forEach((pageId, integer) -> {
            try {
                flushPage(pageId);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        pidToBpidMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        if(!pidToBpidMap.containsKey(pid)) return;
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = pages[pidToBpidMap.get(pid)];
        // append an update record to the log, with
        // a before-image and after-image.
        TransactionId dirtier = page.isDirty();
        if (dirtier != null){
            Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
            Database.getLogFile().force();
        }
        dbFile.writePage(page);
        page.markDirty(false, new TransactionId());
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        List<Page> changedPages = tidToPagesMap.get(tid);
        if(changedPages == null) return;
        for (Page changedPage : changedPages) {
            flushPage(changedPage.getId());
            // use current page contents as the before-image
            // for the next transaction that modifies this page.
            changedPage.setBeforeImage();
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        for (int bpid : pidToBpidMap.values()) {
            Page page = pages[bpid];
            if (page.isDirty() == null) {
                discardPage(page.getId());
                emptyPages.add(bpid);
                return;
            }
        }
        throw new DbException("BufferPool is full of dirty pages.");
    }

}
