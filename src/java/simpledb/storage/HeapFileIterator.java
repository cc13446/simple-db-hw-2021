package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Iterator;

public class HeapFileIterator extends AbstractDbFileIterator{

    private final TransactionId transactionId;
    private final int tableId;
    private final int numPages;

    private int nextPageNo;
    private HeapPage page;
    private Iterator<Tuple> tuples;

    private boolean open;

    public HeapFileIterator(TransactionId tid, int tableId, int numPages) {
        super();
        this.transactionId = tid;
        this.tableId = tableId;
        this.numPages = numPages;
        this.nextPageNo = 0;
        this.open = false;
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        if (!this.open) throw new DbException("the iterator is not open");
        if (this.page == null) return null;
        if (this.tuples == null) this.tuples = this.page.iterator();
        if (this.tuples.hasNext()) {
            return tuples.next();
        } else {
            while(!tuples.hasNext() && this.nextPageNo < this.numPages) {
                Page page = Database.getBufferPool().getPage(this.transactionId, new HeapPageId(this.tableId, this.nextPageNo), Permissions.READ_ONLY);
                if (page.getClass() != HeapPage.class) throw new DbException("Page class worry");
                this.page = (HeapPage) page;
                this.tuples = this.page.iterator();
                this.nextPageNo ++;
            }
            if(tuples.hasNext()) {
                return tuples.next();
            }
        }
        return null;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.open = true;
        this.nextPageNo = 0;
        while(this.nextPageNo < this.numPages) {
            Page page = Database.getBufferPool().getPage(this.transactionId, new HeapPageId(this.tableId, this.nextPageNo), Permissions.READ_ONLY);
            if (page.getClass() != HeapPage.class) throw new DbException("Page class worry");
            this.page = (HeapPage) page;
            this.tuples = this.page.iterator();
            this.nextPageNo ++;
            if (tuples.hasNext()) break;
        }
    }

    @Override
    public void close() {
        super.close();
        this.open = false;
        this.page = null;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.open();
    }
}
