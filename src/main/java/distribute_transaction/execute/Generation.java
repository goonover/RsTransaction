package distribute_transaction.execute;

import org.apache.log4j.Logger;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static distribute_transaction.execute.Generation.GenerationStatus.*;
import static distribute_transaction.execute.RSFutureTask.RSFutureStatus.ROLLBACKSUCCESSFULLY;

/**
 * 记录同一批次的单元任务{@link UnitTask}以及当当前的任务都完成
 * 之后，下一Generation的id
 * Created by swqsh on 2017/11/23.
 */
class Generation {

    private Integer generationId;
    //下一个generation的id
    private Integer nextGenerationId;
    //前一代的generationId，记录前一代id主要是为了实现回滚预留
    private Integer prevGenerationId;
    /**
     * 任务的状态{@link distribute_transaction.execute.RSFutureTask.RSFutureStatus}及其对应的任务列表
     * key的可能值为NEW、RUNNING、NORMAL、ROLLINGBACK、ROLLBACKSUCCESSFULLY、ROLLBACKFAILED
     */
    private ConcurrentHashMap<RSFutureTask.RSFutureStatus,RSFutureTaskContainer> statusTaskMap;

    //当前generation的运行状态
    private volatile GenerationStatus status;
    //全局线程池
    private static ExecutorService executorService;
    //当前generation所属的bookKeeping
    private TransactionBookKeeping transactionBookKeeping;

    Logger logger = Logger.getLogger(Generation.class);

    /**
     * generation的状态
     * NEW------>RUNNING----->COMPLETING------>FINISHED
     * NEW------>RUNNING<---->SUCCEEDALL
     * NEW------>RUNNING------->ABORT
     * NEW------>RUNNING------->COMPLETING------->ABORT
     * 只有RUNNING和SUCCEEDALL时可以互相转化的
     */
    enum GenerationStatus{
        NEW,            //generation已经初始化完成，但尚未执行
        RUNNING,       //当前generation为活跃generation，任务正在执行，并且nextGeneration为空
        COMPLETING,    //与RUNNING类似，但是nextGenerationId不为空，不允许新任务提交
        SUCCEEDALL,    //当前generation为活跃generation，并且当前任务已经全部执行成功
        FINISHED,      //SUCCEEDALL+nextGenerationId!=null
        ABORT,         //出现错误，需要回滚，可能由本代引起，也可能由子代引起
        ROLLBACKSUCCESS,    //回滚成功
        ROLLBACKFAILED      //回滚失败
    }

    Generation(UnitTask unitTask,TransactionBookKeeping transactionBookKeeping){
        statusTaskMap = new ConcurrentHashMap<>();
        this.transactionBookKeeping = transactionBookKeeping;
        status = NEW;
        unitTask.setGeneration(this);
        RSFutureTask executeTask = generateFutureTask(unitTask);
        RSFutureTaskContainer newTasksContainer = getContainerOrPutNewOneIfAbsent(RSFutureTask.RSFutureStatus.NEW);
        generationId = unitTask.getPriority();
        newTasksContainer.put(executeTask);
    }

    /**
     * 把unit task加入到当代中，为了在调用enqueue时，状态由RUNNING-------->ABORT
     * 必须采用同步手段
     */
    boolean enqueue(UnitTask unitTask){
        if(nextGenerationId!=null)
            return false;
        unitTask.setGeneration(this);
        RSFutureTask executeTask = generateFutureTask(unitTask);
        synchronized (this) {
            switch (status) {
                case NEW:
                    enqueueNew(executeTask);
                    break;
                case RUNNING:
                    enqueueRunning(executeTask);
                    break;
                case SUCCEEDALL:
                    enqueueSucceedAll(executeTask);
                    break;
                default:
                    return false;
            }
        }
        return true;
    }

    private void enqueueNew(RSFutureTask task){
        RSFutureTaskContainer container = getContainerOrPutNewOneIfAbsent(RSFutureTask.RSFutureStatus.NEW);
        container.put(task);
    }

    private void enqueueRunning(RSFutureTask task){
        RSFutureTaskContainer runningStatusTasks = getContainerOrPutNewOneIfAbsent(RSFutureTask.RSFutureStatus.RUNNING);
        runningStatusTasks.put(task);
        submitTask(task);
    }

    private void enqueueSucceedAll(RSFutureTask task){
        this.status = RUNNING;
        RSFutureTaskContainer runningStatusTasks = getContainerOrPutNewOneIfAbsent(RSFutureTask.RSFutureStatus.RUNNING);
        runningStatusTasks.put(task);
        submitTask(task);
    }

    /**
     * 轮到当前generation执行，提交状态为NEW的所有的任务，
     * 这个方法同步对性能影响不大，可以直接保留
     */
    synchronized void fire(){
        status = RUNNING;
        RSFutureTaskContainer runningStatusTasks = getContainerOrPutNewOneIfAbsent(RSFutureTask.RSFutureStatus.RUNNING);
        RSFutureTaskContainer newStatusTasks = getContainerOrPutNewOneIfAbsent(RSFutureTask.RSFutureStatus.NEW);
        RSFutureTask task;
        while ((task=newStatusTasks.poll())!=null){
            runningStatusTasks.put(task);
            submitTask(task);
            newStatusTasks.downAndGet();
        }
    }

    /**
     * 把待执行任务提交到线程池执行
     * @param task  待执行任务
     */
    private void submitTask(RSFutureTask task){
        ExecutorService es = getExecutorService();
        es.submit(task);
    }

    /**
     * 生成执行UnitTask任务的future task，
     * @param task  待处理的单元任务
     * @param <T>   unit task的run的返回类型
     */
    private <T> RSFutureTask<T> generateFutureTask(UnitTask task){
        Callable<T> callable = task::run;
        RSFutureTask futureTask = RSFutureTask.createNormalTask(callable);
        futureTask.setUnitTask(task);
        return futureTask;
    }

    private RSFutureTask generateRollbackFutureTask(UnitTask task){
        Runnable runnable = task::rollback;
        RSFutureTask futureTask = RSFutureTask.createRollbackTask(runnable,null);
        futureTask.setUnitTask(task);
        return futureTask;
    }

    /**
     *  任务执行完成，需要对其记录，有可能执行rollback或者唤醒下一个generation的任务
     * @param task 执行完成的任务
     */
    void taskDone(RSFutureTask task){
        RSFutureTask.RSFutureStatus taskStatus = task.getStatus();
        //过滤正在执行状态的任务
        if(taskStatus==RSFutureTask.RSFutureStatus.RUNNING||taskStatus== RSFutureTask.RSFutureStatus.NEW
                ||taskStatus== RSFutureTask.RSFutureStatus.ROLLINGBACK)
            return;
        switch (taskStatus){
            case NORMAL:
                taskDoneNormal(task);
                break;
            case ABORT:
                taskDoneAbort(task);
                break;
            case ROLLBACKSUCCESSFULLY:
                taskDoneRollbackSuccessfully(task);
                break;
            case ROLLBACKFAILED:
                taskDoneRollbackFailed(task);
                break;
            default:
                break;
        }
    }

    /**
     * 处理任务正常执行的情况
     * @param task  成功执行的任务
     */
    private void taskDoneNormal(RSFutureTask task){
        //没有更多的意义，只是为了当其他abort已经出现时，不用取hashMap和从queue中移除元素，小优化
        if(status==ABORT)
            return;
        //如果running列表中没有task，说明task已经被abort处理了
        RSFutureTaskContainer runningTasks = getContainerOrPutNewOneIfAbsent(RSFutureTask.RSFutureStatus.RUNNING);
        if(!runningTasks.remove(task))
            return;

        RSFutureTaskContainer normalTasks = getContainerOrPutNewOneIfAbsent(RSFutureTask.RSFutureStatus.NORMAL);
        boolean lastSuccess = false;
        synchronized (this){
            if(taskDoneInNormalStatus()){
                normalTasks.put(task);
                lastSuccess = (runningTasks.downAndGet()==0);
            }else{
                //在从runningTasks移除之后，出现了状态为abort的结果，任务应该回滚
                taskRollback(task.getUnitTask());
                runningTasks.downAndGet();
                return;
            }
        }
        if(lastSuccess){
            if(status==RUNNING) {
                status = SUCCEEDALL;
                transactionBookKeeping.generationSucceedAll(this);
            } else if(status==COMPLETING) {
                status = FINISHED;
                transactionBookKeeping.generationFinished(this);
            }
        }
    }

    /**
     * 处理任务执行出现RSAbortException，表明事务需要回滚
     * @param task  出现RSAbortException的任务
     */
    private void taskDoneAbort(RSFutureTask task){
        if(status==ABORT)
            return;

        RSFutureTaskContainer runningTasks = getContainerOrPutNewOneIfAbsent(RSFutureTask.RSFutureStatus.RUNNING);
        if(!runningTasks.remove(task))
            return;

        //更新status必须采用同步机制，否则enqueue和normalDone的时候可能会导致竞态条件
        synchronized (this) {
            //double check是需要的，为了避免两个abort请求同时出现
            if(status==ABORT){
                taskRollback(task.getUnitTask());
                runningTasks.downAndGet();
                return;
            }
            status = ABORT;
        }
        UnitTask unitTask = task.getUnitTask();
        taskRollback(unitTask);
        runningTasks.downAndGet();
        rollback(true);
    }

    /**
     * 处理执行回滚操作成功的情况
     * @param task  执行回滚操作成功的任务
     */
    private void taskDoneRollbackSuccessfully(RSFutureTask task){
        if(taskDoneInNormalStatus())
            throw new RuntimeException("generation's status is not abort,rollback task received!");
        RSFutureTaskContainer rollingBackTasks = getContainerOrPutNewOneIfAbsent(RSFutureTask.RSFutureStatus.ROLLINGBACK);
        if(!rollingBackTasks.remove(task)){
            return;
        }

        RSFutureTaskContainer rollingBackSuccessfully = 
                getContainerOrPutNewOneIfAbsent(ROLLBACKSUCCESSFULLY);
        rollingBackSuccessfully.put(task);

        if(rollingBackTasks.downAndGet()==0){
            if(this.status!=ROLLBACKFAILED) {
                this.status = ROLLBACKSUCCESS;
                transactionBookKeeping.generationRollbackCompleted(this,true);
            }else{
                transactionBookKeeping.generationRollbackCompleted(this,false);
            }
        }
    }

    /**
     * 处理回滚任务执行失败的情况
     * @param task  回滚任务执行失败
     */
    private void taskDoneRollbackFailed(RSFutureTask task){
        if(taskDoneInNormalStatus())
            throw new RuntimeException("generation's status is not abort,rollback task received!");

        RSFutureTaskContainer rollingBackTasks =
                getContainerOrPutNewOneIfAbsent(RSFutureTask.RSFutureStatus.ROLLINGBACK);
        if(!rollingBackTasks.remove(task))
            return;

        task.rollbackFailed();
        if(task.shouldRollback()){
            UnitTask unitTask = task.getUnitTask();
            task = generateRollbackFutureTask(unitTask);
            submitTask(task);
        }else{
            //只要出现一个回滚失败，generation就标志为回滚失败
            status = ROLLBACKFAILED;
            RSFutureTaskContainer rollbackFailedTasks = 
                    getContainerOrPutNewOneIfAbsent(RSFutureTask.RSFutureStatus.ROLLBACKFAILED);
            rollbackFailedTasks.put(task);
            //回滚完成
            if(rollingBackTasks.downAndGet()==0){
                transactionBookKeeping.generationRollbackCompleted(this,false);
            }
        }
    }

    /**
     * 从statusMap中获取相应状态的container，如果没有，则新建一个并添加到statusMap中。
     * 方法线程安全，无需加锁
     * @param rsFutureStatus    statusMap的key
     */
    private RSFutureTaskContainer getContainerOrPutNewOneIfAbsent(RSFutureTask.RSFutureStatus rsFutureStatus){
        RSFutureTaskContainer ret = statusTaskMap.get(rsFutureStatus);
        if(ret!=null)
            return ret;
        RSFutureTaskContainer containerToPut = new RSFutureTaskContainer(rsFutureStatus);
        ret = statusTaskMap.putIfAbsent(rsFutureStatus,containerToPut);
        if(ret!=null)
            return ret;
        else{
            return containerToPut;
        }
    }

    private void taskRollback(UnitTask unitTask){
        /**
         * 由于taskDoneRollbackFailed中过滤掉所有想要rollback，
         * 并且失败次数到达上限的情况，这里的判断得到不应该
         * 进行rollback处理的，均视为rollback成功处理
         */
        if(!unitTask.shouldRollback()){
            Runnable runnable = unitTask::rollback;
            RSFutureTask needNotRollback = RSFutureTask.createSpecTask(runnable,null,ROLLBACKSUCCESSFULLY);
            taskDoneRollbackSuccessfully(needNotRollback);
            return;
        }

        RSFutureTask rollbackFuture = generateRollbackFutureTask(unitTask);
        RSFutureTaskContainer rollingBackTasks = getContainerOrPutNewOneIfAbsent(RSFutureTask.RSFutureStatus.ROLLINGBACK);
        rollingBackTasks.put(rollbackFuture);
        submitTask(rollbackFuture);
    }

    /**
     * 有UnitTask在运行过程中捕捉到RSAbortException，发动回滚操作;
     * 或者其他nextGeneration运行过程中出现回滚导致当前的generation回滚
     */
    void rollback(boolean innerInvoke){
        if(innerInvoke){
            innerInvokeRollback();
        }else{
            outerInvokeRollback();
        }
    }

    private void innerInvokeRollback(){
        RSFutureTaskContainer runningTasks = getContainerOrPutNewOneIfAbsent(RSFutureTask.RSFutureStatus.RUNNING);
        RSFutureTask task;
        while ((task = runningTasks.poll())!=null){
            task.cancel(true);
            taskRollback(task.getUnitTask());
            runningTasks.downAndGet();
        }

        if(!statusTaskMap.containsKey(RSFutureTask.RSFutureStatus.NORMAL))
            return;
        RSFutureTaskContainer normalFinishedTasks = getContainerOrPutNewOneIfAbsent(RSFutureTask.RSFutureStatus.NORMAL);
        while ((task = normalFinishedTasks.poll())!=null){
            taskRollback(task.getUnitTask());
            normalFinishedTasks.downAndGet();
        }
    }

    private void outerInvokeRollback(){
        this.status = ABORT;
        RSFutureTaskContainer normalFinishedTasks = statusTaskMap.remove(RSFutureTask.RSFutureStatus.NORMAL);
        RSFutureTask task;
        while ((task = normalFinishedTasks.poll())!=null){
            taskRollback(task.getUnitTask());
            normalFinishedTasks.downAndGet();
        }
    }

    /**
     *  线程池采取延迟初始化的方式，
     *  TODO:目前只是简单的fixThreadPool，实际有待商榷
     */
    private ExecutorService getExecutorService(){
        if(executorService == null){
            executorService = Executors.newFixedThreadPool(3);
        }
        return executorService;
    }

    Integer getGenerationId() {
        return generationId;
    }

    Integer getNextGenerationId() {
        return nextGenerationId;
    }

    synchronized void setNextGenerationId(Integer nextGenerationId) {
        this.nextGenerationId = nextGenerationId;
        if(status==RUNNING)
            status = COMPLETING;
        else if(status==SUCCEEDALL){
            status = FINISHED;
            transactionBookKeeping.generationFinished(this);
        }
    }

    Integer getPrevGenerationId() {
        return prevGenerationId;
    }

    void setPrevGenerationId(Integer prevGenerationId) {
        this.prevGenerationId = prevGenerationId;
    }

    /**
     *  当一个任务完成时，只有generation的状态为running或者completing的时候才可能为正常，
     *  否则任务整个generation进入了rollback mode
     */
    private boolean taskDoneInNormalStatus(){
        if(status==RUNNING||status==COMPLETING)
            return true;
        else
            return false;
    }


    private class RSFutureTaskContainer{

        private final RSFutureTask.RSFutureStatus taskStatus;
        private BlockingQueue<RSFutureTask> futureTasks;
        /**
         * activeTasks表示整个Container中活跃的tasks数目，由于存在
         * activeTasks的数目可能与futureTasks.size()不一致的情况，
         * 所以必须分别记录
         *
         * 假设taskStatus为RUNNING，其正常完成，应该从running container
         * 取出task然后加到normal container之中，在取出之后和加到normal container
         * 之间有一段空档期，避免多线程问题出现，需要加锁或者添加之后再更新
         * 相应的任务数目
         */
        private AtomicInteger activeTasks;

        RSFutureTaskContainer(RSFutureTask.RSFutureStatus status){
            taskStatus = status;
            futureTasks = new LinkedBlockingDeque<>();
            activeTasks = new AtomicInteger(0);
        }

        boolean put(RSFutureTask futureTask){
            try {
                futureTasks.put(futureTask);
            } catch (InterruptedException e) {
                put(futureTask);
            }
            activeTasks.incrementAndGet();
            return true;
        }

        boolean remove(RSFutureTask futureTask){
            return futureTasks.remove(futureTask);
        }

        RSFutureTask poll(){
            return futureTasks.poll();
        }

        boolean isEmpty(){
            return activeTasks.get()==0;
        }

        int downAndGet(){
            return activeTasks.decrementAndGet();
        }

    }

}
