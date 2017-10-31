package distribute_transaction.scheduler;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static distribute_transaction.scheduler.Lock.S;
import static distribute_transaction.scheduler.Lock.X;

/**
 * 向{@link TableResource}申请资源时，用于帮助TableResource记录该范围上一申请事务
 * Created by swqsh on 2017/10/29.
 */
class ApplyRange<T extends Comparable<T>> extends Range<T> implements Comparable<ApplyRange<T>> {

    //如未获取所有资源，需等待waitingSet中的ApplyRange释放其资源
    private HashSet<ApplyRange<T>> waitingSet = new HashSet<>();
    //当与该范围相关事务执行完成后，通知后续的范围执行
    private List<ApplyRange<T>> notifyList = new ArrayList<>();
    //根结点的标志
    private boolean isRoot = true;
    //父结点
    private ApplyRange<T> parent = null;

    /**
     * 只有在共享模式下才会使用该数据
     */
    class SharedInfo{
        //已获取资源的上一读结点
        private HashSet<ApplyRange<T>> acquiredReadRanges = new HashSet<>();
        //未获得资源的上一读结点
        private HashSet<ApplyRange<T>> notAcquiredReadRanges = new HashSet<>();
        //在该结点上等待的读请求
        private List<ApplyRange<T>> waitingRanges = new ArrayList<>();

        void notifyAllWaitingReadRanges(ApplyRange<T> range){
            while (!waitingRanges.isEmpty()){
                ApplyRange<T> waitingReadRange = waitingRanges.remove(0);
                waitingReadRange.sharedInfo.notAcquiredReadRanges.remove(range);
                waitingReadRange.sharedInfo.acquiredReadRanges.add(range);
            }
        }
    }
    //在lockMode==S情况下会初始化
    SharedInfo sharedInfo;

    //包内使用
    ApplyRange(Range<T> range){
        super(range.transaction,range.left,range.right,range.lockModel);
        if(lockModel==S){
            this.sharedInfo = new SharedInfo();
        }
    }

    //只有内部创建子结点时才会使用，子结点只是用于在TableResource中占据资源用
    private ApplyRange(T left,T right,ApplyRange<T> parent){
        super(null,left,right,parent.lockModel);
        this.isRoot = false;
        this.parent = parent;
    }

    /**
     * 向有重叠区域的上一范围申请资源
     * @param lastApplyRange
     */
    void applyOnLastApplyRange(ApplyRange<T> lastApplyRange){
        if(lastApplyRange.lockModel==X)
            applyOnExclusiveMode(lastApplyRange);
        else
            applyOnSharedMode(lastApplyRange);
    }

    /**
     * 当前范围是互斥范围，采取互斥方式申请资源
     * @param lastApplyRange 上一个在同一块资源申请的range
     */
    private void applyOnExclusiveMode(ApplyRange<T> lastApplyRange){
        if(lastApplyRange.isRoot) {
            waitingSet.add(lastApplyRange);
            lastApplyRange.notifyList.add(this);
        }else{
            waitingSet.add(lastApplyRange.parent);
            lastApplyRange.parent.notifyList.add(this);
        }
    }

    /**
     * 当前范围是读范围，采取共享方式申请资源
     * @param lastApplyRange    上一申请范围
     */
    private void applyOnSharedMode(ApplyRange<T> lastApplyRange){
        if(lastApplyRange.lockModel==X)
            applyOnExclusiveMode(lastApplyRange);
        if(lastApplyRange.isRoot){
            applyOnSharedRange(lastApplyRange);
            lastApplyRange.wasAppliedBySharedRange(this);
        }else{
            applyOnSharedMode(lastApplyRange.parent);
        }
    }

    /**
     * 把事务与资源封锁请求解除关系
     * @param transaction   事务
     */
    void unboundTransaction(Transaction transaction){
        if(this.transaction!=transaction)
            return;
        this.transaction = null;
        if(shouldRelease()){
            notifyAllWaitingRanges();
        }
    }

    /**
     * 在执行完毕后应该判断是否应该释放资源，返回值如为true，
     * 应该调用notifyAllWaitingRanges()
     * @return
     */
    boolean shouldRelease(){
       if(lockModel==X){
           if(transaction==null)
               return true;
       }else{
           //对于共享模式，必须等待前面的所有共享集合释放后才能释放
           if(transaction==null&&sharedInfo.acquiredReadRanges.isEmpty())
               return true;
       }
       return false;
    }

    /**
     * 通知在其后申请同一资源的ApplyRange，该资源已经可以获取
     */
    private void notifyAllWaitingRanges() {
        while (!notifyList.isEmpty()){
            ApplyRange waitingRange = notifyList.remove(0);
            waitingRange.acquireRange(this);
        }
    }

    /**
     * 上一个范围被释放，当前范围可以将其从等待列表中移除
     * @param parentRange
     */
    private void acquireRange(ApplyRange<T> parentRange) {
        if(lockModel==S&&parentRange.lockModel==S){
            sharedInfo.acquiredReadRanges.remove(parentRange);
            if(shouldRelease())
                notifyAllWaitingRanges();
        }else {
            waitingSet.remove(parentRange);
            if (waitingSet.isEmpty()&&(lockModel==X||
                    (lockModel==S&&sharedInfo.notAcquiredReadRanges.isEmpty()))) {
                invokeTransaction();
            }
        }
    }

    //通知所有事务，其已经获取了该范围的资源
    private void invokeTransaction() {
        if(lockModel==S){
            sharedInfo.notifyAllWaitingReadRanges(this);
        }
        ((TransactionImpl)transaction).acquireRange(this);
    }

    @Override
    public int compareTo(ApplyRange<T> o) {
        return left.compareTo(o.left);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj==this)
            return true;
        if(!(obj instanceof ApplyRange)){
            return false;
        }
        ApplyRange<T> compareObj = (ApplyRange<T>) obj;
        if(compareObj.lockModel.equals(lockModel)&&compareObj.left.equals(left)
                &&compareObj.right.equals(right)&&isRoot==compareObj.isRoot){
            return true;
        }
        return false;
    }

    /**
     * 判断当前的applyRange是否在range里面
     * @param range
     * @return
     */
    boolean isWithin(ApplyRange<T> range){
        return range.left.compareTo(left)<=0&&range.right.compareTo(right)>=0;
    }

    //创建子结点
    ApplyRange<T> newChildRange(T left,T right){
        return new ApplyRange<T>(left,right,this);
    }

    /**
     * 当前范围的所申请资源是否已经全部得到
     */
    boolean hasAcquiredAllResource(){
        return waitingSet.isEmpty();
    }

    //主动申请
    private void applyOnSharedRange(ApplyRange<T> applyRange){
        if(applyRange.hasAcquiredAllResource())
            sharedInfo.acquiredReadRanges.add(applyRange);
        else
            sharedInfo.notAcquiredReadRanges.add(applyRange);
    }

    //被申请
    private void wasAppliedBySharedRange(ApplyRange<T> applyRange){
        notifyList.add(applyRange);
        if(!hasAcquiredAllResource())
            sharedInfo.waitingRanges.add(applyRange);
    }

}
