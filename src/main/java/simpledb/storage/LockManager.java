package simpledb.storage;

import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

public class LockManager {

    public static class PageLock implements Serializable {
        public enum LockType { SHARE, EXCLUSIVE }
        // 锁类型
        private LockType type;
        // 事务id
        private final TransactionId transactionId;

        public PageLock(LockType type, TransactionId transactionId) {
            this.type = type;
            this.transactionId = transactionId;
        }

        public void setType(LockType type) {
            this.type = type;
        }

        public LockType getType() {
            return this.type;
        }

        public String toString() {
            return type + " " + transactionId;
        }
    }
    /**
     * 记录某个页面是否被加锁了,如果被加锁了,那么是哪些事务加的锁
     */
    private final Map<PageId, Map<TransactionId, PageLock>> pageLockMap;

    public LockManager() {
        pageLockMap = new ConcurrentHashMap<>();
    }

    /**
     * LockManager来实现对锁的管理，LockManager中主要有申请锁、释放锁、查看指定数据页的指定事务是否有锁这三个功能，其中加锁的逻辑比较麻烦，需要基于严格两阶段封锁协议去实现。
     * 事务t对指定的页面加锁时，思路如下：
     * 锁管理器中没有任何锁或者该页面没有被任何事务加锁，可以直接加读/写锁；
     * 如果t在页面有锁，分以下情况讨论：
     * 2.1 加的是读锁：直接加锁；
     * 2.2 加的是写锁：如果锁数量为1，进行锁升级；如果锁数量大于1，会死锁，抛异常中断事务；
     * 如果t在页面无锁，分以下情况讨论：
     * 3.1 页面有读锁：如果锁数量为1，这个锁是读锁则可以加，是写锁就wait；如果锁数量大于1，说明有很多读锁，直接加；
     * 3.2 页面是写锁：不管是多个读锁还是一个写锁，都不能加，wait
     */
    public synchronized boolean acquireLock(PageId pageId, TransactionId tid, PageLock.LockType acquireType) throws TransactionAbortedException, InterruptedException {
        // 获取当前页面上已经添加的锁
        Map<TransactionId, PageLock> lockMap = pageLockMap.get(pageId);
        // 当前page还没有加过锁
        if (lockMap == null || lockMap.size() == 0) {
            PageLock pageLock = new PageLock(acquireType, tid);
            lockMap = new ConcurrentHashMap<>();
            lockMap.put(tid, pageLock);
            // 记录当前页面被当前事务加了一把什么样的锁
            pageLockMap.put(pageId, lockMap);
            return true;
        }
        // 若当前事务在当前page上加过锁了
        if (isHoldLock(pageId, tid)) {
            // 若当前事务希望在当前page上加读锁，此时无论lock是读锁还是写锁都可以加锁成功
            if (acquireType == PageLock.LockType.SHARE) {
                return true;
            // 若当前事务希望在当前page上加写锁
            } else if (acquireType == PageLock.LockType.EXCLUSIVE) {
                // 若存在其他事务在当前页面上加了读锁 --> 直接抛出异常,防止等待锁升级产生死锁问题
                if (lockMap.size() > 1) {
                    return false;
//                    throw new TransactionAbortedException();
                //  若当前事务在当前页面上已经加了写锁
                } else {
                    // 获取当前事务在当前页面上加的锁
                    PageLock lock = lockMap.get(tid);
                    if(lock.getType() == PageLock.LockType.EXCLUSIVE) {
                        return true;
                    // 若当前页面只存在当前事务加的读锁 --> 进行锁升级
                    } else if(lock.getType() == PageLock.LockType.SHARE) {
                        lock.setType(PageLock.LockType.EXCLUSIVE);
                        lockMap.put(tid, lock);
                        pageLockMap.put(pageId, lockMap);
                        return true;
                    }
                }
            } else {
                throw new IllegalArgumentException("wrong acquireType");
            }
        // 若当前事务在当前page没加过锁
        } else {
            // 如果当前事务想要加共享锁
            if (acquireType == PageLock.LockType.SHARE) {
                PageLock.LockType addedLock = lockMap.values().iterator().next().getType();
                // 当前页面已经被其他事务加了读锁，此处的读锁也可以直接加
                if (addedLock == PageLock.LockType.SHARE) {
                    PageLock pageLock = new PageLock(acquireType, tid);
                    lockMap.put(tid, pageLock);
                    pageLockMap.put(pageId, lockMap);
                    return true;
                // 若当前页面只有一个锁，并且为写锁，则等待
                } else if (addedLock == PageLock.LockType.EXCLUSIVE){
                    wait(5);
                    return false;
                } else {
                    throw new IllegalArgumentException("wrong addedLock Type");
                }
            // 如果当前事务想要加写锁,那么直接等待 --> 因为当前页面上存在其他事务加的锁,无论锁类型是什么,都与写锁互斥,所以直接等待即可
            } else if (acquireType == PageLock.LockType.EXCLUSIVE) {
                wait(1);
                return false;
            } else {
                throw new IllegalArgumentException("wrong acquireType");
            }
        }
        return true;
    }

    /**
     * 释放指定页面的指定事务加的锁
     *
     * @param pageId 页id
     * @param tid    事务id
     */
    public synchronized void releaseLock(PageId pageId, TransactionId tid) {
        Map<TransactionId, PageLock> lockMap = pageLockMap.get(pageId);
        if (lockMap == null || tid == null || lockMap.get(tid) == null) {
            return;
        }
        lockMap.remove(tid);
        if (lockMap.size() == 0) {
            pageLockMap.remove(pageId);
        }
        this.notifyAll();
    }

    /**
     * 判断事务是否持有对应页的锁
     *
     * @param pageId 页id
     * @param tid    事务id
     * @return 事务是否持有对应页的锁
     */
    public synchronized boolean isHoldLock(PageId pageId, TransactionId tid) {
        Map<TransactionId, PageLock> lockMap = pageLockMap.get(pageId);
        if (lockMap == null) {
            return false;
        }
        return lockMap.get(tid) != null;
    }

    /**
     * 释放事务对所有页面的锁
     */
    public synchronized void completeTransaction(TransactionId tid) {
        pageLockMap.forEach(new BiConsumer<PageId, Map<TransactionId, PageLock>>() {
            @Override
            public void accept(PageId pageId, Map<TransactionId, PageLock> transactionIdPageLockMap) {
                releaseLock(pageId, tid);
            }
        });
    }
}
