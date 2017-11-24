package distribute_transaction.execute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

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
     * key的可能值为NEW、RUNNING、NORMAL、ROLLINGBACK、ROLLBACKSUCCESS、ROLLBACKFAILED
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
     */
    void fire(){
        synchronized (this){
            status = RUNNING;
            List<RSFutureTask> runningStatusTasks = new ArrayList<>();
            statusTaskMap.put(RSFutureTask.RSFutureStatus.RUNNING,runningStatusTasks);
        }
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
        RSFutureTask.RSFutureStatus status = task.getStatus();
        //过滤正在执行状态的任务
        if(status==RSFutureTask.RSFutureStatus.RUNNING||status== RSFutureTask.RSFutureStatus.NEW
                ||status== RSFutureTask.RSFutureStatus.ROLLINGBACK)
            return;
        List<RSFutureTask> runningTasks = statusTaskMap.get(RSFutureTask.RSFutureStatus.RUNNING);
        //正常执行的情况
        if(runningTasks.remove(task)){
            switch (task.getStatus()){
                case NORMAL:
                    taskDoneNormal(task);
                    break;
                case ABORT:
                    taskDoneAbort(task);
                    break;
            }
        }
    }

    //任务正常执行
    private void taskDoneNormal(RSFutureTask task){
        if(this.status!=ABORT) {
            //正常结束
            if (!statusTaskMap.containsKey(RSFutureTask.RSFutureStatus.NORMAL)) {
                List<RSFutureTask> normalTasks = new ArrayList<>();
                statusTaskMap.put(RSFutureTask.RSFutureStatus.NORMAL, normalTasks);
            }
            statusTaskMap.get(RSFutureTask.RSFutureStatus.NORMAL).add(task);
            List<RSFutureTask> runningTask = statusTaskMap.get(RSFutureTask.RSFutureStatus.RUNNING);
            if (runningTask.size() == 0) {
                if (status == RUNNING)
                    status = SUCCEEDALL;
                else if (status == COMPLETING)
                    status = FINISHED;
            }
        }else{
            UnitTask rollbackUnitTask = task.getUnitTask();
            task = null;
            RSFutureTask rollbackTask = generateRollbackFutureTask(rollbackUnitTask);
            List<RSFutureTask> rollbackTasks = statusTaskMap.get(RSFutureTask.RSFutureStatus.ROLLINGBACK);
            rollbackTasks.add(rollbackTask);
            submitTask(rollbackTask);
        }
    }

    //执行过程中出现了RSAbortException
    private void taskDoneAbort(RSFutureTask task){
        rollback();
    }

    private void submitRollbackTask(){

    }

    private void rollback(){
        //TODO:implement rollback
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

    void setGenerationId(Integer generationId) {
        this.generationId = generationId;
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
