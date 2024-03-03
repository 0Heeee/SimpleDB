package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static java.lang.Math.max;
import static java.lang.StrictMath.ceil;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws NoSuchElementException {
        if(pid.getTableId() != getId()) throw new NoSuchElementException("Read page: table id error.");
        InputStream is = null;
        byte[] data = new byte[BufferPool.getPageSize()];
        HeapPage hp = null;

        try {
            is = Files.newInputStream(file.toPath());
            long beforeDataLen = (long) pid.getPageNumber() * BufferPool.getPageSize();
            is.skip(beforeDataLen);
            int offset = 0;
            int count = 0;
            while (true) {
                try {
                    if (!(offset < data.length
                            && (count = is.read(data, offset, data.length - offset)) >= 0)) break;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                offset += count;
            }
            if (offset < data.length) throw new IOException("Read page: page read error.");
            is.close();

            if(!(pid instanceof HeapPageId)) throw new NoSuchElementException("Read page: page id error.");
            HeapPageId hpid = (HeapPageId) pid;
            hp = new HeapPage(hpid, data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return hp;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        if(page.getId().getTableId() != getId()) throw new NoSuchElementException("Read page: table id error.");
        byte[] data = page.getPageData();
        RandomAccessFile rf = new RandomAccessFile(file, "rw");
        if(page.getId().getPageNumber()>numPages()) {
            rf.write(data);
            rf.close();
        } else {
            rf.seek((long) page.getId().getPageNumber() * BufferPool.getPageSize());
            rf.write(data);
            rf.close();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) ceil((double) file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        BufferPool bufferPool = Database.getBufferPool();
        for (int i = 0; i < numPages(); i++) {
            PageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) bufferPool.getPage(tid, pid, Permissions.READ_WRITE);
            // insert tuple into current page
            if(page.getNumEmptySlots() != 0) {
                page.insertTuple(t);
                ArrayList<Page> pages = new ArrayList<>();
                pages.add(page);
                return pages;
            } else {
                bufferPool.unsafeReleasePage(tid, page.getId());
            }
        }
        // insert tuple into new page
        BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file, true));
        byte[] emptyPageData = HeapPage.createEmptyPageData();
        // 向文件末尾添加数据
        outputStream.write(emptyPageData);
        outputStream.close();
        HeapPage hp = (HeapPage) bufferPool.getPage(tid, new HeapPageId(getId(), numPages() - 1), Permissions.READ_WRITE);
        hp.insertTuple(t);
        ArrayList<Page> pages = new ArrayList<>();
        pages.add(hp);
        return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        RecordId recordId = t.getRecordId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, recordId.getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        ArrayList<Page> pages = new ArrayList<>();
        pages.add(page);
        return pages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new AbstractDbFileIterator() {
            private Iterator<Tuple> tupleIterator = null;
            private HeapPage currentPage = null;
            @Override
            protected Tuple readNext() throws DbException, TransactionAbortedException {
                if(tupleIterator==null || currentPage==null) return null;
                if(tupleIterator.hasNext()) return tupleIterator.next();
                else {
                    if(currentPage.pid.getPageNumber()+1<numPages()) {
                        PageId pid = new HeapPageId(currentPage.pid.getTableId(), currentPage.pid.getPageNumber()+1);
                        currentPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                        tupleIterator = currentPage.iterator();
                        return readNext();
                    } else return null;
                }
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                PageId pid = new HeapPageId(getId(), 0);
                currentPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                tupleIterator = currentPage.iterator();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();
            }

            @Override
            public void close() {
                super.close();
                tupleIterator = null;
                currentPage = null;
            }
        };
    }

}

