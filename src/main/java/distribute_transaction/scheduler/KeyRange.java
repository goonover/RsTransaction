package distribute_transaction.scheduler;

import java.util.Comparator;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 请求资源封锁范围
 * Created by swqsh on 2017/9/4.
 */
class KeyRange <T extends Comparable<T>> extends Range<T> implements Comparable<KeyRange<T>>,Comparator<KeyRange<T>>{

    //所属的资源表
    TableResource resource;

    //已获取到资源的事务列表
    BlockingDeque<Transaction> holdingTransactions;

    //未获取到资源的请求队列
    BlockingQueue<KeyRange<T>> waitingRanges;

    //只有当前事务在等待时才起作用
    KeyRange<T> parent;

    KeyRange(Range<T> range){
        super(range.transaction,range.left,range.right,range.lockType);
    }

    KeyRange(Transaction transaction, T left, T right, Lock lockType, KeyRange<T> parent){
        super(transaction,left,right,lockType);
        this.parent = parent;
    }

    KeyRange(Transaction transaction, T left, T right, TableResource resource, Lock lockType) {
        super(transaction,left,right,lockType);
        this.resource = resource;
        holdingTransactions = new LinkedBlockingDeque<Transaction>();
        waitingRanges = new LinkedBlockingQueue<KeyRange<T>>();
        authorizeToTransaction(transaction);
    }

    /**
     * 把事务进行排队，与新建一个范围锁存在区别。
     * 只能由{@link TableResource}或者获取资源的锁后调用，主要是为了防止锁进队列
     * 时，holdingTransaction为空，当前的范围锁变为无效锁，事务从此变为死事务
     * @param transaction
     * @param left
     * @param right
     * @param lockType
     * @return
     */
    void enQueue(Transaction transaction, T left, T right, Lock lockType) {
        if(this.lockType == Lock.S && waitingRanges.isEmpty() &&lockType == Lock.S){
            authorizeToTransaction(transaction);
        }else{
            KeyRange<T> request = new KeyRange<T>(transaction,left,right,lockType,this);
            notifyTransactionToWait(transaction,request);
        }
    }

    /**
     * 事务获取范围锁
     * @param transaction
     */
    private void authorizeToTransaction(Transaction transaction){
        holdingTransactions.add(transaction);
        transaction.acquireKeyRange(this);
    }

    /**
     * 通知事务，资源未能立刻获取，需要等待
     * @param range
     */
    private void notifyTransactionToWait(Transaction transaction, KeyRange<T> range){
        waitingRanges.add(range);
        transaction.waitingKeyRange(range);
    }

    //判断KeyRange是否只是对一个值封锁的范围锁
    boolean isSingleRange(){
        return left.compareTo(right)==0;
    }

    public int compare(KeyRange<T> o1, KeyRange<T> o2) {
        return o1.compareTo(o2);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == this)
            return true;
        if(!(obj instanceof KeyRange))
            return false;
        KeyRange target = (KeyRange) obj;
        if(target.left.equals(left)&&target.right.equals(right)&&target.resource==resource)
            return true;
        return false;
    }

    @Override
    public int hashCode() {
        int hashCode = 39;
        hashCode = hashCode*17 + left.hashCode();
        hashCode = hashCode*17 + right.hashCode();
        hashCode = hashCode*17 + resource.hashCode();
        return hashCode;
    }

    public int compareTo(KeyRange<T> o) {
        return left.compareTo(o.left);
    }

    /**
     * 解除Transaction与KeyRange的绑定，Range的release和unBound不能同时执行
     */
    void unBound(){
        if(parent!=null){
            synchronized (parent) {
                parent.waitingRanges.remove(this);
            }
        }
    }

    /**
     * 事务释放已获取的资源。如resource正在处于分配过程中，resource.release会阻塞到分配完成再执行
     * @param transaction   执行完成的事务
     */
    void release(Transaction transaction){
        holdingTransactions.remove(transaction);
        if(holdingTransactions.isEmpty()){
            resource.release(this);
        }
    }

    @Override
    public String toString() {
        String res = "TransactionId:"+transaction.getTransactionId()+"; ";
        res += "left:   "+left+";   right:"+right;
        return res;
    }
}
