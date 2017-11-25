package distribute_transaction.execute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
    private HashMap<RSFutureTask.RSFutureStatus,List<RSFutureTask>> statusTaskMap;

    //当前generation的运行状态
    private volatile GenerationStatus status;
    //全局线程池
    private static ExecutorService executorService;

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

    Generation(UnitTask unitTask){
        statusTaskMap = new HashMap<>();
        status = NEW;
        RSFutureTask executeTask = generateFutureTask(unitTask);
        List<RSFutureTask> newStatusTasks = new ArrayList<>();
        newStatusTasks.add(executeTask);
        generationId = unitTask.getPriority();
        statusTaskMap.put(RSFutureTask.RSFutureStatus.NEW,newStatusTasks);
    }

    /**
     * 把unit task加入到当代中
     * 虽然waitingTasks采用的不是线程安全容器，但是事实上调用enqueue(task)和
     * 调用execute()的线程应该为同一个线程，不存在线程问题，无须加锁
     */
    synchronized boolean enqueue(UnitTask unitTask){
        if(nextGenerationId!=null)
            return false;
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
        List<RSFutureTask> newStatusTasks = statusTaskMap.get(RSFutureTask.RSFutureStatus.NEW);
        newStatusTasks.add(task);
    }

    private void enqueueRunning(RSFutureTask task){
        List<RSFutureTask> runningStatusTasks = statusTaskMap.get(RSFutureTask.RSFutureStatus.RUNNING);
        runningStatusTasks.add(task);
        submitTask(task);
    }

    private void enqueueSucceedAll(RSFutureTask task){
        this.status = RUNNING;
        List<RSFutureTask> runningStatusTasks = statusTaskMap.get(RSFutureTask.RSFutureStatus.RUNNING);
        runningStatusTasks.add(task);
        submitTask(task);
    }

    /**
     * 轮到当前generation执行，提交状态为NEW的所有的任务
     * TODO:改善加锁方式，使锁的颗粒度更小
     */
    synchronized void fire(){
        status = RUNNING;
        List<RSFutureTask> runningStatusTasks = new ArrayList<>();
        statusTaskMap.put(RSFutureTask.RSFutureStatus.RUNNING,runningStatusTasks);

        List<RSFutureTask> newStatusTasks = statusTaskMap.get(RSFutureTask.RSFutureStatus.NEW);
        List<RSFutureTask> runningStatusTask = statusTaskMap.get(RSFutureTask.RSFutureStatus.RUNNING);
        while (newStatusTasks.size()>0){
            RSFutureTask task = newStatusTasks.remove(0);
            runningStatusTask.add(task);
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
     *     List<RSFutureTask> remainTasks
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

        List<RSFutureTask> runningTasks = statusTaskMap.get(RSFutureTask.RSFutureStatus.RUNNING);
        if(!runningTasks.remove(task))
            return;

        if(!statusTaskMap.containsKey(RSFutureTask.RSFutureStatus.NORMAL)){
            statusTaskMap.put(RSFutureTask.RSFutureStatus.NORMAL,new ArrayList<>());
        }

        List<RSFutureTask> normalTasks = statusTaskMap.get(RSFutureTask.RSFutureStatus.NORMAL);
        normalTasks.add(task);
        //TODO:状态更新问题现在解决方案并不好，有待完善
        if(runningTasks.size()==0){
            if(status==RUNNING)
                status = SUCCEEDALL;
            else if(status==COMPLETING)
                status = FINISHED;
        }
    }

    /**
     * 处理任务执行出现RSAbortException，表明事务需要回滚
     * @param task  出现RSAbortException的任务
     */
    private void taskDoneAbort(RSFutureTask task){
        if(status==ABORT)
            return;

        List<RSFutureTask> runningTasks = statusTaskMap.get(RSFutureTask.RSFutureStatus.RUNNING);
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
        List<RSFutureTask> rollingBackTasks = statusTaskMap.get(RSFutureTask.RSFutureStatus.ROLLINGBACK);
        if(!rollingBackTasks.remove(task)){
            return;
        }

        if(!statusTaskMap.containsKey(RSFutureTask.RSFutureStatus.ROLLBACKSUCCESSFULLY)){
            statusTaskMap.put(RSFutureTask.RSFutureStatus.ROLLBACKSUCCESSFULLY,new ArrayList<>());
        }
        List<RSFutureTask> rollingBackSuccessfully = statusTaskMap.get(RSFutureTask.RSFutureStatus.ROLLBACKSUCCESSFULLY);
        rollingBackSuccessfully.add(task);
        //TODO:处理全部任务已经完成rollback，通知上一generation进行rollback
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
            if(!statusTaskMap.containsKey(RSFutureTask.RSFutureStatus.ROLLBACKFAILED))
                statusTaskMap.put(RSFutureTask.RSFutureStatus.ROLLBACKFAILED,new ArrayList<>());
            List<RSFutureTask> rollbackFailedTasks = statusTaskMap.get(RSFutureTask.RSFutureStatus.ROLLBACKFAILED);
            rollbackFailedTasks.add(task);
            //TODO:判断当前generation的rollback是否执行完成，进行下一阶段的rollback
        }
    }

    private void taskRollback(UnitTask unitTask){
        if(!statusTaskMap.containsKey(RSFutureTask.RSFutureStatus.ROLLINGBACK)){
            statusTaskMap.put(RSFutureTask.RSFutureStatus.ROLLINGBACK,new ArrayList<>());
        }

        RSFutureTask rollbackFuture = generateRollbackFutureTask(unitTask);
        List<RSFutureTask> rollingBackTasks = statusTaskMap.get(RSFutureTask.RSFutureStatus.ROLLINGBACK);
        rollingBackTasks.add(rollbackFuture);
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
        List<RSFutureTask> runningTasks = statusTaskMap.remove(RSFutureTask.RSFutureStatus.RUNNING);
        while (runningTasks.size()>0){
            RSFutureTask task = runningTasks.remove(0);
            task.cancel(true);
            taskRollback(task.getUnitTask());
        }

        List<RSFutureTask> normalFinishedTasks = statusTaskMap.remove(RSFutureTask.RSFutureStatus.NORMAL);
        while (normalFinishedTasks!=null&&normalFinishedTasks.size()>0){
            RSFutureTask task = normalFinishedTasks.remove(0);
            taskRollback(task.getUnitTask());
        }
    }

    private void outerInvokeRollback(){
        this.status = ABORT;
        List<RSFutureTask> normalFinishedTasks = statusTaskMap.remove(RSFutureTask.RSFutureStatus.NORMAL);
        while (normalFinishedTasks!=null&&normalFinishedTasks.size()>0){
            RSFutureTask task = normalFinishedTasks.remove(0);
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

}
