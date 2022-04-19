package simpledb.storage;

import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {

    static class TransactionLock{
        private final Set<TransactionId> transactionIds;
        private Permissions permissions;

        public TransactionLock() {
            this.transactionIds = ConcurrentHashMap.newKeySet();
            this.permissions = null;
        }

        synchronized boolean acquireLock(TransactionId transactionId, Permissions permissions) {
            if (this.permissions == null) {
                this.permissions = permissions;
                this.transactionIds.clear();
                this.transactionIds.add(transactionId);
            }
            // acquireReadLock
            if (permissions == Permissions.READ_ONLY) {
                if (this.permissions == Permissions.READ_ONLY) {
                    // ReadLock
                    this.transactionIds.add(transactionId);
                    return true;
                } else {
                    // WriteLock
                    if (transactionIds.isEmpty()) {
                        this.permissions = permissions;
                        this.transactionIds.add(transactionId);
                        return true;
                    } else {
                        return this.transactionIds.contains(transactionId);
                    }
                }
            } else {
                if (this.permissions == Permissions.READ_ONLY) {
                    // ReadLock
                    if (this.transactionIds.size() == 0 || this.transactionIds.size() == 1 && this.transactionIds.contains(transactionId)) {
                        this.permissions = permissions;
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    // WriteLock
                    if (transactionIds.isEmpty()) {
                        this.permissions = permissions;
                        this.transactionIds.add(transactionId);
                        return true;
                    } else {
                        return this.transactionIds.size() == 1 && this.transactionIds.contains(transactionId);
                    }
                }
            }
        }


        synchronized void releaseLock(TransactionId transactionId) {
            this.transactionIds.remove(transactionId);
        }

        synchronized boolean holdsLock(TransactionId transactionId) {
            return this.transactionIds.contains(transactionId);
        }
    }

    private final Map<PageId, TransactionLock> lockMap;


    public LockManager() {
        this.lockMap = new ConcurrentHashMap<>();
    }

    public void LockPage(PageId pageId, TransactionId transactionId, Permissions permissions) throws TransactionAbortedException {
        if (!lockMap.containsKey(pageId)) {
            this.lockMap.put(pageId, new TransactionLock());
        }
        TransactionLock transactionLock = this.lockMap.get(pageId);
        boolean acquired = false;
        long start = System.currentTimeMillis();
        long timeOut = new Random().nextInt(2000) + 1000;
        while (!acquired) {
            acquired = transactionLock.acquireLock(transactionId, permissions);
            if (!acquired) {
                if (System.currentTimeMillis() - start > timeOut) throw new TransactionAbortedException();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw new TransactionAbortedException();
                }
            }
        }
    }

    public void ReleasePage(PageId pageId, TransactionId transactionId) {
        TransactionLock transactionLock = this.lockMap.getOrDefault(pageId, null);
        if (null == transactionLock) {
            return;
        }
        transactionLock.releaseLock(transactionId);
    }

    public boolean holdsLock(PageId pageId, TransactionId transactionId) {
        TransactionLock transactionLock = this.lockMap.getOrDefault(pageId, null);
        if (null == transactionLock) {
            return false;
        }
        return transactionLock.holdsLock(transactionId);
    }
}
