package simpledb.storage;

import simpledb.common.Permissions;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.security.acl.Owner;
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
    private final Map<TransactionId, Set<PageId>> wantLockMap;


    public LockManager() {
        this.lockMap = new ConcurrentHashMap<>();
        this.wantLockMap = new ConcurrentHashMap<>();
    }

    public void LockPage(PageId pageId, TransactionId transactionId, Permissions permissions) throws TransactionAbortedException {
        if (!lockMap.containsKey(pageId)) {
            this.lockMap.put(pageId, new TransactionLock());
        }
        TransactionLock transactionLock = this.lockMap.get(pageId);
        if (!this.wantLockMap.containsKey(transactionId)) {
            this.wantLockMap.put(transactionId, ConcurrentHashMap.newKeySet());
        }
        this.wantLockMap.get(transactionId).add(pageId);
        boolean acquired = false;
        int trys = 0;
        while (!acquired) {
            acquired = transactionLock.acquireLock(transactionId, permissions);
            if (!acquired) {
                try {
                    Thread.sleep(10);
                    trys++;
                    if (trys % 10 == 0){
                        deadLock(transactionId);
                    }
                } catch (InterruptedException e) {
                    throw new TransactionAbortedException();
                }
            }
        }
        this.wantLockMap.get(transactionId).remove(pageId);
        if (this.wantLockMap.get(transactionId).isEmpty()) {
            this.wantLockMap.remove(transactionId);
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

    public void releaseAllLocks(TransactionId tid) {
        for (PageId pageId : this.lockMap.keySet()) {
            if (holdsLock(pageId, tid)){
                this.ReleasePage(pageId, tid);
            }
        }
    }

    private void deadLock(TransactionId transactionId) throws TransactionAbortedException{
        Set<PageId> my = new HashSet<>();
        for (PageId pageId : this.lockMap.keySet()) {
            if (holdsLock(pageId, transactionId)) {
                my.add(pageId);
            }
        }

        Set<PageId> want = new HashSet<>(this.wantLockMap.get(transactionId));
        while (!want.isEmpty()) {
            Set<PageId> newWant = new HashSet<>();
            Set<TransactionId> owner = new HashSet<>();
            for (PageId p : want) {
                owner.addAll(this.lockMap.getOrDefault(p, new TransactionLock()).transactionIds);
            }
            owner.remove(transactionId);
            for (TransactionId tid : owner) {
                newWant.addAll(this.wantLockMap.getOrDefault(tid, new HashSet<>()));
            }
            for (PageId p : newWant) {
                if (my.contains(p)) {
                    throw new TransactionAbortedException();
                }
            }
            want = newWant;
        }
    }
}
