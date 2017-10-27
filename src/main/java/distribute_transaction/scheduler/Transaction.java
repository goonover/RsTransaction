package distribute_transaction.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 整个事务的抽象
 * Created by swqsh on 2017/9/4.
 */
public class Transaction<T extends Comparable<T>> {

    private long transactionId;

    //待申请的范围锁列表
    private List<Range<T>> toBeApply;
    private AtomicInteger waitCount = new AtomicInteger(0);
    // TODO:尝试移除waitingList并用计数代替
    //已在排队的资源列表
    BlockingQueue<KeyRange<T>> waitingList;
    //已经获取得到的资源列表
    private BlockingQueue<KeyRange<T>> holdingList;

    private String message = "";

    private Scheduler scheduler;

    //是否已经完成第一次分配
    private AtomicBoolean isAllocated = new AtomicBoolean(false);

    //请求原语
    String requestStr;

    public Transaction(long transactionId,List<Range<T>> keyRangeList){
        this.transactionId = transactionId;
        toBeApply = keyRangeList;
        waitingList = new LinkedBlockingQueue<KeyRange<T>>();
        holdingList = new LinkedBlockingQueue<KeyRange<T>>();
        initToBeApply();
    }

    public Transaction(long transactionId, List<Range<T>> toBeApply, String requestStr) {
        this.transactionId = transactionId;
        this.toBeApply = toBeApply;
        waitingList = new LinkedBlockingQueue<KeyRange<T>>();
        holdingList = new LinkedBlockingQueue<KeyRange<T>>();
        this.requestStr = requestStr;
        initToBeApply();
    }

    private void initToBeApply(){
        if(toBeApply == null)
            toBeApply = new ArrayList<Range<T>>();
        for(Range range:toBeApply){
            range.setTransaction(this);
        }
    }

    boolean shouldBeExecuted(){
        //return isAllocated.get()&&toBeApply.isEmpty() && waitingList.isEmpty();
        return isAllocated.get()&&(waitCount.get()==0);
    }

    synchronized void acquireKeyRange(KeyRange<T> range){
        holdingList.add(range);
    }

    void waitingKeyRange(KeyRange<T> range){
        //this.waitingList.add(range);
        waitCount.incrementAndGet();
    }

    @Override
    public int hashCode() {
        return (int) ((transactionId>>>32)^transactionId);
    }

    List<Range> getRequestRange(){
        if(toBeApply!=null) {
            return new ArrayList<Range>(toBeApply);
        }else{
            return new ArrayList<Range>();
        }
    }

    void afterApply(Range range){
        toBeApply.remove(range);
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    void setScheduler(Scheduler scheduler){
        this.scheduler = scheduler;
    }

    /**
     * 第一次分配资源完毕，立刻判断是否能运行，可以则添加到运行队列中；否则，应添加到
     * 等待队列中
     */
    synchronized void afterAllocated(){
        isAllocated.set(true);
        if(shouldBeExecuted()){
            scheduler.fireTransaction(this);
        }else{
            scheduler.waitTransaction(this);
        }
    }

    /**
     * 第二次分配，原来在等待列表中的range获取了资源，每次waitingList中的Range得到
     * 资源时，都应该验证事务是否可以运行
     * @param range 第二次分配的封锁范围
     */
    void afterReallocated(KeyRange<T> range){
        //waitingList.remove(range);
        waitCount.decrementAndGet();
        if(shouldBeExecuted()){
            scheduler.fireTransaction(this);
        }
    }

    /**
     * 事务完成，释放所有资源，可能是正常执行完成，也可能是人为干预
     */
    private void release(){
        while (!waitingList.isEmpty()){
            KeyRange<T> waitingRange = waitingList.poll();
            waitingRange.unBound();
        }
       while (!holdingList.isEmpty()){
           KeyRange<T> holdingRange = holdingList.poll();
           holdingRange.release(this);
       }
    }

    public void complete(){
        scheduler.transactionCompleted(this);
        release();
    }

    public long getTransactionId() {
        return transactionId;
    }

}
