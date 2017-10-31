package distribute_transaction.scheduler;

import distribute_transaction.core.AlgUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * 虚拟资源的抽象，取代原来用红黑树来实现加锁策略
 * Created by swqsh on 2017/10/29.
 */
public class TableResource<T extends Comparable<T>> {

    private String tableName;

    TableResource(String tableName){
        this.tableName = tableName;
    }

    List<ApplyRange<T>> lastApplyRanges = new ArrayList<ApplyRange<T>>();

    /**
     * 向资源表申请范围资源
     * @param applyRange
     */
    void applyFor(Range<T> applyRange){
        ApplyRange<T> newApplyRange = new ApplyRange<T>(applyRange);
        //找到新范围的前继
        int indexOfPre = AlgUtils.findPreOfList(lastApplyRanges,newApplyRange);
        applyResourceForRange(newApplyRange,indexOfPre);
        TransactionImpl transaction = (TransactionImpl) newApplyRange.transaction;
        if(newApplyRange.hasAcquiredAllResource()){
            transaction.acquireRange(newApplyRange);
        }else {
            transaction.notAcquiredImmediately(newApplyRange);
        }
    }

    /**
     * 为范围锁申请资源
     * @param newApplyRange 申请范围
     * @param indexOfPre    前继结点
     */
    private void applyResourceForRange(ApplyRange<T> newApplyRange, int indexOfPre) {
        ApplyRange<T> preRange = lastApplyRanges.get(indexOfPre);
        //与前继结点存在重合
        if(isValidRange(indexOfPre)&&overlap(preRange,newApplyRange)){
            //新申请范围在前继结点里面
            if(newApplyRange.isWithin(preRange)){
                //右边界相等时
                if(newApplyRange.right.compareTo(preRange.right)==0){
                    preRange.right = newApplyRange.left;
                    newApplyRange.applyOnLastApplyRange(preRange);
                    lastApplyRanges.add(indexOfPre+1,newApplyRange);
                    return;
                }else{
                    //新申请的范围在原范围之内，需要将原范围分裂
                    ApplyRange<T> childRange = preRange.newChildRange(newApplyRange.right,preRange.right);
                    preRange.right = newApplyRange.left;
                    newApplyRange.applyOnLastApplyRange(preRange);
                    lastApplyRanges.add(++indexOfPre,newApplyRange);
                    lastApplyRanges.add(++indexOfPre,childRange);
                    return;
                }
            }else{
                preRange.right = newApplyRange.left;
                newApplyRange.applyOnLastApplyRange(preRange);
            }
        }
        int insertIndex = indexOfPre+1;
        while (insertIndex<lastApplyRanges.size()){
            ApplyRange<T> currentRange = lastApplyRanges.remove(insertIndex);
            //移除所有失活结点
            if(currentRange.shouldRelease())
                continue;
            if(!overlap(newApplyRange,currentRange))
                break;
            /**
             * 对应着申请结点与后继结点的三种重叠情况，对于current.left == new.left的情况
             * 依然使用，这种情况被下面三种情况囊括了
             */
            if(currentRange.right.compareTo(newApplyRange.right)<0){
                newApplyRange.applyOnLastApplyRange(currentRange);
            }else if(currentRange.right.compareTo(newApplyRange.right)==0){
                newApplyRange.applyOnLastApplyRange(currentRange);
                break;
            }else{
                currentRange.left = newApplyRange.right;
                newApplyRange.applyOnLastApplyRange(currentRange);
                lastApplyRanges.add(insertIndex,currentRange);
                break;
            }
        }
        lastApplyRanges.add(insertIndex,newApplyRange);
    }

    //判断两个范围是否有重合
    private boolean overlap(ApplyRange<T> first,ApplyRange<T> second){
        if(first==null||second==null)
            return false;
        if(first.left.compareTo(second.left)==0)
            return true;
        else if(first.left.compareTo(second.left)<0&&first.right.compareTo(second.left)>0)
            return true;
        else if(first.left.compareTo(second.left)>0&&first.left.compareTo(second.right)<0)
            return true;
        return false;
    }

    public String getTableName(){
        return this.tableName;
    }

    /**
     * 判断一个锁范围是不是依然有效，即其相关的事务处于活跃状态。如处于不活跃
     * 状态，即应该将其从加锁范围中移除
     * @param indexOfRange  范围的索引
     * @return
     */
    private boolean isValidRange(int indexOfRange){
        ApplyRange<T> validateRange = lastApplyRanges.get(indexOfRange);
        if(validateRange.shouldRelease()){
            lastApplyRanges.remove(indexOfRange);
            return false;
        }
        return true;
    }

}
