package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.index.BTreeLeafPage;
import simpledb.index.BTreePageId;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

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
    private final TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
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
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pageSize = BufferPool.getPageSize();
        int pageNo = pid.getPageNumber();
        int offset = pageNo * pageSize;
        if (offset >= this.file.length()) {
            throw new IllegalArgumentException("the page does not exist in this file.");
        }
        byte[] data = new byte[pageSize];
        try (RandomAccessFile r = new RandomAccessFile(this.file, "r")) {
            r.seek(offset);
            r.read(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            if (pid.getClass() == HeapPageId.class) {
                return new HeapPage((HeapPageId)pid, data);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pageSize = BufferPool.getPageSize();
        int pageNo = page.getId().getPageNumber();
        int offset = pageNo * pageSize;
        assert (offset >= 0 && offset <= this.file.length());
        try (RandomAccessFile w = new RandomAccessFile(this.file, "rws")) {
            w.seek(offset);
            w.write(page.getPageData());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (this.file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        List<Page> pages = new ArrayList<>();
        int pageNums = this.numPages();
        for (int i = 0; i < pageNums; i++) {
            Page page = Database.getBufferPool().getPage(tid, new HeapPageId(this.getId(), i), Permissions.READ_WRITE);
            assert (page instanceof HeapPage);
            HeapPage heapPage = (HeapPage) page;
            if (heapPage.getNumEmptySlots() != 0) {
                heapPage.insertTuple(t);
                heapPage.markDirty(true, tid);
                pages.add(heapPage);
                return pages;
            }
        }

        BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(this.file, true));
        byte[] emptyData = HeapPage.createEmptyPageData();
        bw.write(emptyData);
        bw.close();

        HeapPage newPage = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(this.getId(), pageNums), Permissions.READ_WRITE);
        newPage.insertTuple(t);
        newPage.markDirty(true, tid);
        pages.add(newPage);
        return pages;
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        Page page = Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        assert (page instanceof HeapPage);
        HeapPage heapPage = (HeapPage) page;
        heapPage.deleteTuple(t);
        heapPage.markDirty(true, tid);
        List<Page> pages = new ArrayList<>();
        pages.add(heapPage);
        return pages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid, this.getId(), this.numPages());
    }

}

