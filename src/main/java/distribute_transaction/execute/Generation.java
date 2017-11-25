package distribute_transaction.execute;

import java.util.HashMap;
import java.util.concurrent.*;

import static distribute_transaction.execute.Generation.GenerationStatus.*;

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
    private HashMap<RSFutureTask.RSFutureStatus,RSFutureTaskContainer> statusTaskMap;

    //当前generation的运行状态
    private volatile GenerationStatus status;
    //全局线程池
    private static ExecutorService executorService;
    //当前generation所属的bookKeeping
    private TransactionBookKeeping transactionBookKeeping;

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
        ABORT          //出现错误，需要回滚，可能由本代引起，也可能由子代引起
    }

    Generation(UnitTask unitTask,TransactionBookKeeping transactionBookKeeping){
        statusTaskMap = new HashMap<>();
        this.transactionBookKeeping = transactionBookKeeping;
        status = NEW;
        unitTask.setGeneration(this);
        RSFutureTask executeTask = generateFutureTask(unitTask);
        RSFutureTaskContainer newTasksContainer = new RSFutureTaskContainer(RSFutureTask.RSFutureStatus.NEW);
        newTasksContainer.put(executeTask);
        generationId = unitTask.getPriority();
        statusTaskMap.put(RSFutureTask.RSFutureStatus.NEW,newTasksContainer);
    }

    /**
     * 把unit task加入到当代中，为了在调用enqueue时，状态由RUNNING-------->ABORT
     * 必须采用同步手段
     */
    synchronized boolean enqueue(UnitTask unitTask){
        if(nextGenerationId!=null)
            return false;
        unitTask.setGeneration(this);
        RSFutureTask executeTask = generateFutureTask(unitTask);
        switch (status){
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
        return true;
    }

    private void enqueueNew(RSFutureTask task){
        RSFutureTaskContainer container = statusTaskMap.get(RSFutureTask.RSFutureStatus.NEW);
        container.put(task);
    }

    private void enqueueRunning(RSFutureTask task){
        RSFutureTaskContainer runningStatusTasks = statusTaskMap.get(RSFutureTask.RSFutureStatus.RUNNING);
        runningStatusTasks.put(task);
        submitTask(task);
    }

    private void enqueueSucceedAll(RSFutureTask task){
        this.status = RUNNING;
        RSFutureTaskContainer runningStatusTasks = statusTaskMap.get(RSFutureTask.RSFutureStatus.RUNNING);
        runningStatusTasks.put(task);
        submitTask(task);
    }

    /**
     * 轮到当前generation执行，提交状态为NEW的所有的任务
     */
    synchronized void fire(){
        status = RUNNING;
        RSFutureTaskContainer runningStatusTasks = new RSFutureTaskContainer(RSFutureTask.RSFutureStatus.RUNNING);
        statusTaskMap.put(RSFutureTask.RSFutureStatus.RUNNING,runningStatusTasks);

        RSFutureTaskContainer newStatusTasks = statusTaskMap.get(RSFutureTask.RSFutureStatus.NEW);
        RSFutureTask task;
        while ((task=newStatusTasks.poll())!=null){
            runningStatusTasks.put(task);
            submitTask(task);
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
        RSFutureTask<T> futureTask = new RSFutureTask<T>(callable);
        futureTask.setUnitTask(task);
        return futureTask;
    }

    private RSFutureTask generateRollbackFutureTask(UnitTask task){
        Runnable runnable = task::rollback;
        RSFutureTask futureTask = new RSFutureTask(runnable,null);
        futureTask.setUnitTask(task);
        return futureTask;
    }

    /**
     * TODO:可以修改statusMaps的value为TaskContainer，
     * class TasksContainer{
     * //remains!=remainTasks.size();
     *     AtomicInteger remains;
     *     RSFutureTaskContainer remainTasks
     * }
     *  任务执行完成，需要对其记录，有可能执行rollback或者唤醒下一个generation的任务
     * @param task 执行完成的任务
     */
    synchronized void taskDone(RSFutureTask task){
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
        //如果之前已有任务发起回滚，则无视其完成
        if(status==ABORT)
            return;

        RSFutureTaskContainer runningTasks = statusTaskMap.get(RSFutureTask.RSFutureStatus.RUNNING);
        if(!runningTasks.remove(task))
            return;

        if(!statusTaskMap.containsKey(RSFutureTask.RSFutureStatus.NORMAL)){
            statusTaskMap.put(RSFutureTask.RSFutureStatus.NORMAL,new RSFutureTaskContainer(RSFutureTask.RSFutureStatus.NORMAL));
        }

        RSFutureTaskContainer normalTasks = statusTaskMap.get(RSFutureTask.RSFutureStatus.NORMAL);
        normalTasks.put(task);
        if(runningTasks.isEmpty()){
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

        RSFutureTaskContainer runningTasks = statusTaskMap.get(RSFutureTask.RSFutureStatus.RUNNING);
        if(!runningTasks.remove(task))
            return;

        status = ABORT;
        UnitTask unitTask = task.getUnitTask();
        taskRollback(unitTask);
        rollback(true);
    }

    /**
     * 处理执行回滚操作成功的情况
     * @param task  执行回滚操作成功的任务
     */
    private void taskDoneRollbackSuccessfully(RSFutureTask task){
        if(status!=ABORT)
            throw new RuntimeException("generation's status is not abort,rollback task received!");
        RSFutureTaskContainer rollingBackTasks = statusTaskMap.get(RSFutureTask.RSFutureStatus.ROLLINGBACK);
        if(!rollingBackTasks.remove(task)){
            return;
        }

        if(!statusTaskMap.containsKey(RSFutureTask.RSFutureStatus.ROLLBACKSUCCESSFULLY)){
            statusTaskMap.put(RSFutureTask.RSFutureStatus.ROLLBACKSUCCESSFULLY,
                    new RSFutureTaskContainer(RSFutureTask.RSFutureStatus.ROLLBACKSUCCESSFULLY));
        }
        RSFutureTaskContainer rollingBackSuccessfully = statusTaskMap.get(RSFutureTask.RSFutureStatus.ROLLBACKSUCCESSFULLY);
        rollingBackSuccessfully.put(task);

        if(rollingBackTasks.isEmpty()){
            transactionBookKeeping.generationRollbackSuccessfully(this);
        }
    }

    /**
     * 处理回滚任务执行失败的情况
     * @param task  回滚任务执行失败
     */
    private void taskDoneRollbackFailed(RSFutureTask task){
        if(status!=ABORT)
            throw new RuntimeException("generation's status is not abort,rollback task received!");

        if(!(statusTaskMap.get(RSFutureTask.RSFutureStatus.ROLLINGBACK).remove(task)))
            return;

        task.rollbackFailed();
        if(task.shouldRollback()){
            task.rollbackReset();
            submitTask(task);
        }else{
            if(!statusTaskMap.containsKey(RSFutureTask.RSFutureStatus.ROLLBACKFAILED)) {
                statusTaskMap.put(RSFutureTask.RSFutureStatus.ROLLBACKFAILED,
                        new RSFutureTaskContainer(RSFutureTask.RSFutureStatus.ROLLBACKFAILED));
            }

            RSFutureTaskContainer rollbackFailedTasks = statusTaskMap.get(RSFutureTask.RSFutureStatus.ROLLBACKFAILED);
            rollbackFailedTasks.put(task);
            //回滚完成
            RSFutureTaskContainer rollingBackTasks = statusTaskMap.get(RSFutureTask.RSFutureStatus.ROLLINGBACK);
            if(rollingBackTasks.isEmpty()){
                transactionBookKeeping.generationRollbackFailed(this);
            }
        }
    }

    private void taskRollback(UnitTask unitTask){
        if(!statusTaskMap.containsKey(RSFutureTask.RSFutureStatus.ROLLINGBACK)){
            statusTaskMap.put(RSFutureTask.RSFutureStatus.ROLLINGBACK,
                    new RSFutureTaskContainer(RSFutureTask.RSFutureStatus.ROLLINGBACK));
        }

        RSFutureTask rollbackFuture = generateRollbackFutureTask(unitTask);
        RSFutureTaskContainer rollingBackTasks = statusTaskMap.get(RSFutureTask.RSFutureStatus.ROLLINGBACK);
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
        RSFutureTaskContainer runningTasks = statusTaskMap.remove(RSFutureTask.RSFutureStatus.RUNNING);
        RSFutureTask task;
        while ((task = runningTasks.poll())!=null){
            task.cancel(true);
            taskRollback(task.getUnitTask());
        }

        if(!statusTaskMap.containsKey(RSFutureTask.RSFutureStatus.NORMAL))
            return;
        RSFutureTaskContainer normalFinishedTasks = statusTaskMap.remove(RSFutureTask.RSFutureStatus.NORMAL);
        while ((task = normalFinishedTasks.poll())!=null){
            taskRollback(task.getUnitTask());
        }
    }

    private void outerInvokeRollback(){
        this.status = ABORT;
        if(!statusTaskMap.containsKey(RSFutureTask.RSFutureStatus.ABORT))
            return;
        RSFutureTaskContainer normalFinishedTasks = statusTaskMap.remove(RSFutureTask.RSFutureStatus.NORMAL);
        RSFutureTask task;
        while ((task = normalFinishedTasks.poll())!=null){
            taskRollback(task.getUnitTask());
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

    void setNextGenerationId(Integer nextGenerationId) {
        this.nextGenerationId = nextGenerationId;
        if(status==RUNNING)
            status = COMPLETING;
    }

    Integer getPrevGenerationId() {
        return prevGenerationId;
    }

    void setPrevGenerationId(Integer prevGenerationId) {
        this.prevGenerationId = prevGenerationId;
    }


    class RSFutureTaskContainer{

        private final RSFutureTask.RSFutureStatus taskStatus;
        private BlockingQueue<RSFutureTask> futureTasks;

        RSFutureTaskContainer(RSFutureTask.RSFutureStatus status){
            taskStatus = status;
            futureTasks = new LinkedBlockingDeque<>();
        }

        boolean put(RSFutureTask futureTask){
            if(futureTask.getStatus()!=taskStatus)
                return false;
            try {
                futureTasks.put(futureTask);
            } catch (InterruptedException e) {
                put(futureTask);
            }
            return true;
        }

        boolean remove(RSFutureTask futureTask){
            return futureTasks.remove(futureTask);
        }

        RSFutureTask poll(){
            return futureTasks.poll();
        }

        boolean isEmpty(){
            return futureTasks.isEmpty();
        }

    }

}
