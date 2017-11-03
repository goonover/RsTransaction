package distribute_transaction.scheduler;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * 在Scheduler包中，事务的具体实现
 * Created by swqsh on 2017/10/29.
 */
class TransactionImpl extends Transaction{

    //在等待的范围集合
    HashSet<ApplyRange> waitingSet = new HashSet<>();
    //已经获取得到的资源集合
    List<ApplyRange> acquiredList = new ArrayList<>();
    //调度器
    Scheduler scheduler;

    boolean firstAllocated = true;

    TransactionImpl(long transactionId, List<Range> applyRanges,Scheduler scheduler) {
        super(transactionId, applyRanges);
        this.scheduler = scheduler;
    }

    TransactionImpl(long transactionId, List<Range> applyRanges, String requestStr, Scheduler scheduler) {
        super(transactionId, applyRanges, requestStr);
        this.scheduler = scheduler;
    }

    /**
     * 在某资源范围内第一次申请时失败
     * @param applyRange    申请范围
     */
    void notAcquiredImmediately(ApplyRange applyRange){
        waitingSet.add(applyRange);
    }

    private void releaseAcquiredResource(){
        while (!acquiredList.isEmpty()){
            ApplyRange acquiredRange = acquiredList.remove(0);
            acquiredRange.unboundTransaction(this);
        }
    }

    /**
     * 获取得到ApplyRange范围的资源，在第一次分配时不需要从waitingSet中
     * 移除等待中的集合外，以后都需要将相应的范围从等待集合中移除
     * @param acquireRange
     */
    void acquireRange(ApplyRange acquireRange){
        if(firstAllocated){
            acquiredList.add(acquireRange);
        }else{
            waitingSet.remove(acquireRange);
            acquiredList.add(acquireRange);
            fireTransactionCheck();
        }
    }

    /**
     * 第一次申请资源完成
     */
    void firstAllocatedCompleted(){
        this.firstAllocated = false;
        fireTransactionCheck();
    }

    /**
     * 检查是否应该立刻执行事务
     * @return
     */
    private boolean fireTransactionCheck(){
        if(!firstAllocated&&waitingSet.isEmpty()){
            fireTransaction();
            return true;
        }
        return false;
    }

    /**
     * 事务执行完成，释放所有资源并更新事务状态
     */
    void complete(){
        while (acquiredList.size()>0){
            ApplyRange holdingResource = acquiredList.remove(0);
            holdingResource.unboundTransaction(this);
        }
        this.state = State.FINISH;
    }

    void fireTransaction(){
        this.state = State.RUNNING;
        scheduler.fireTransaction(this);
    }
}
