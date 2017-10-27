package distribute_transaction.scheduler;

import distribute_transaction.core.Common;
import distribute_transaction.core.RBTree;
import distribute_transaction.core.RBTreeNode;
import org.apache.log4j.Logger;


/**
 * 每一个表都应该有一个与之对应的TableResource，由其管理每个表相关的逻辑资源
 * Created by swqsh on 2017/9/4.
 */
class TableResource<T extends Comparable<T>> {

    long requestTime = 0;
    long insertRbTree = 0;
    long releaseTime = 0;
    long deleteRbTree = 0;
    long afterReallocate = 0;
    long firstApply = 0;
    long applyCallTimes = 0;
    long firstApplyCallTimes = 0;

    private Logger logger = Logger.getLogger(TableResource.class);

    RBTree<KeyRange<T>> resourceHolder;
    String tableName;

    TableResource(String tableName){
        this.tableName = tableName;
        this.resourceHolder = new RBTree<KeyRange<T>>();
    }

    void applyFor(Range<T> range){
        firstApplyCallTimes++;
        long startTime = System.nanoTime();
        KeyRange<T> keyRange;
        if(!(range instanceof KeyRange)) {
            keyRange = new KeyRange<T>(range);
        }else{
            keyRange = (KeyRange<T>) range;
        }
        applyFor(keyRange);
        firstApply += System.nanoTime()-startTime;
    }

    /**
     * 事务提供待封锁资源范围，申请资源
     * @param range 事务提供的封锁范围，实际的范围不会采用原来的范围。
     *                在range里面的transaction信息就是申请资源的那个事务
     */
    void applyFor(KeyRange<T> range){
        applyCallTimes++;
        long startApply = System.nanoTime();
        RBTreeNode<KeyRange<T>> prevNode = resourceHolder.findPredecessor(range);
        //存在前继节点且range.left<prev.right，即范围部分资源在前继结点中
        KeyRange<T> prev = prevNode==null?null:prevNode.getKey();
        if(overlap(prev,range)){
            enQueueExistNode(prev,range);
        }else{
            handleSucc(range);
        }
        requestTime += System.nanoTime()-startApply;
    }

    /**
     * 申请的范围是否与前一结点有重叠部分
     * @param holdingRange  持有锁范围
     * @param applyRange 申请范围
     * @return
     */
    private boolean overlap(KeyRange<T> holdingRange,KeyRange<T> applyRange){
        if(holdingRange==null||applyRange==null)
            return false;
        //case1 左边界相等
        if(Common.comparableEquals(holdingRange.left,applyRange.left)
                //case2 holdingRange的左边界小于applyRange,同时其有边界大于aR的左边界
                ||(Common.comparableSmaller(holdingRange.left,applyRange.left) &&Common.comparableBigger(holdingRange.right,applyRange.left))
                //case 3 hR的左边界大于aR的左边界，同时小于其右边界
                ||(Common.comparableBigger(holdingRange.left,applyRange.left) && (Common.comparableSmaller(holdingRange.left,applyRange.right))))
            return true;
       return false;
    }

    /**
     * 请求封锁范围之前的所有资源都已经分配完毕
     * @param request 请求封锁范围
     */
    private void handleSucc(KeyRange<T> request) {
        RBTreeNode<KeyRange<T>> successorNode = resourceHolder.findNodeOrSuccessor(request);
        if(successorNode!=null){
            KeyRange<T> successor = successorNode.getKey();
            if(request.right.compareTo(successor.left)<=0){
                allocateNewNode(request.transaction,request.left,request.right,request.lockType);
            }else{
                if(keyRangeShouldExtend(successor,request)){
                    if(successor.left.compareTo(request.left)>0) {
                        resourceHolder.delete(successor);
                        successor.left = request.left;
                        keyRangeExtend(successor, request);
                        resourceHolder.insert(successor);
                    }else{
                        keyRangeExtend(successor,request);
                    }
                }else{
                    if(request.left.compareTo(successor.left)<0) {
                        allocateNewNode(request.transaction, request.left, successor.left, request.lockType);
                        request.left = successor.left;
                    }
                }
                enQueueExistNode(successor,request);
            }
            /*
            KeyRange<T> successor = successorNode.getKey();
            if(request.right.compareTo(successor.left)<=0){
                //验证是否单点，且为后继左边界，进队列
                if((request.left.compareTo(request.right)==0)
                        &&(request.left.compareTo(successor.left)==0)){
                    enQueueExistNode(successor,request);
                }else {
                    allocateNewNode(request.transaction, request.left, request.right, request.lockType);
                }
            }else{
                if(request.left.compareTo(successor.left)<0) {
                    allocateNewNode(request.transaction, request.left, successor.left, request.lockType);
                }
                request.left = successor.left;
                enQueueExistNode(successor,request);
            }*/
        }else{
            allocateNewNode(request.transaction,request.left,request.right,request.lockType);
        }
    }

    /**
     * 分配新的封锁节点，每一个新的锁都是通过该方法向红黑树中添加节点的
     * @param transaction
     * @param left
     * @param right
     * @param lockType
     */
    private void allocateNewNode(Transaction transaction, T left, T right, Lock lockType) {
        long startTime = System.nanoTime();
        KeyRange<T> newNode = new KeyRange<T>(transaction,left,right,this,lockType);
        resourceHolder.insert(newNode);
        insertRbTree+= System.nanoTime()-startTime;
    }

    /**
     * 封锁请求在一个已存在锁上排队
     * @param exist  已存在锁
     * @param range 请求
     */
    private void enQueueExistNode(KeyRange<T> exist, KeyRange<T> range) {
        if(range.right.compareTo(exist.right)<=0){
            //range在exist里面
            exist.enQueue(range.transaction,range.left,range.right,range.lockType);
        }else{
            /**
             * 如果keyRange范围应该扩展的话，就应该扩展范围
             */
            if(keyRangeShouldExtend(exist,range)) {
                keyRangeExtend(exist, range);
                exist.enQueue(range.transaction, range.left, exist.right, range.lockType);
                //扩展后的exist不能完全容纳range
                if (exist.right.compareTo(range.right) < 0) {
                    range.left = exist.right;
                    handleSucc(range);
                }
            }else{
                exist.enQueue(range.transaction,range.left,exist.right,range.lockType);
                range.left = exist.right;
                handleSucc(range);
            }
        }
    }

    /**
     *  前一范围为单值范围，必须范围扩展。另外，只要不是持有锁类型为互斥，请求锁类型为共享，均应该
     *  采用锁扩展，以减少锁的数量(!(holdingRange.lockType==Lock.X&&applyRange.lockType==Lock.S)
     */
    private boolean keyRangeShouldExtend(KeyRange<T> holdingRange,KeyRange<T> applyRange){
        if(holdingRange.isSingleRange()||(!(holdingRange.lockType==Lock.X&&applyRange.lockType==Lock.S)))
            return true;
        else
            return false;
    }

    /**
     * 当目前处理加锁节点的右边界小于request的右边界时，锁的范围就应该扩展。
     * 锁扩展到range.right或者下一个已存在的锁的左边界
     * @param holding   拥有锁的KeyRange
     * @param request   加锁请求
     */
    private void keyRangeExtend(KeyRange<T> holding, KeyRange<T> request) {
        RBTreeNode<KeyRange<T>> succOfHolding = resourceHolder.findSuccessor(holding);
        if(succOfHolding!=null){
               KeyRange<T> succ = succOfHolding.getKey();
               //range.right扩展到达了下一个锁的范围
               if(succ.left.compareTo(request.right)<0) {
                   holding.right = succ.left;
                   return;
               }
        }
        holding.right = request.right.compareTo(holding.right)>0?request.right:holding.right;
    }

    /**
     * 有{@link KeyRange}调用，当{@link KeyRange#holdingTransactions}为空时，调用
     * 该函数，从红黑树中移除该锁，并为等待列表中的所有事务申请分配资源
     * @param range 范围实例
     */
    void release(KeyRange<T> range){
        long startTime = System.nanoTime();
        //需要二次验证
        if(range.holdingTransactions.isEmpty()) {
            long deleteTime = System.nanoTime();
            resourceHolder.delete(range);
            deleteRbTree += System.nanoTime()-deleteTime;
            while (!range.waitingRanges.isEmpty()) {
                KeyRange<T> nextRange = range.waitingRanges.poll();
                //nextRange = range.waitingRanges.poll(300, TimeUnit.MICROSECONDS);
                if (nextRange != null) {
                    applyFor(nextRange);
                    long beforeAllocate = System.nanoTime();
                    nextRange.transaction.afterReallocated(nextRange);
                    afterReallocate += System.nanoTime() - beforeAllocate;
                }
            }
        }
        releaseTime += System.nanoTime()-startTime;
    }

    public void printProcessTimes(){
        System.out.println("firstApplyTime:     "+firstApply/1000);
        System.out.println("requestTime:  "+requestTime/1000);
        System.out.println("releaseTime:    "+releaseTime/1000);
        System.out.println("insertTime:     "+insertRbTree/1000);
        System.out.println("deleteTime:     "+deleteRbTree/1000);
        System.out.println("reallocateTime:     "+afterReallocate/1000);
        System.out.println("applyCallTime:  "+applyCallTimes);
        System.out.println("firstApplyCallTime:     "+firstApplyCallTimes);
    }

}
