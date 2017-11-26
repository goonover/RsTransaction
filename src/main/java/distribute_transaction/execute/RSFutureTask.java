package distribute_transaction.execute;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * Created by swqsh on 2017/11/10.
 */
public class RSFutureTask<T> extends FutureTask<T> {

    volatile boolean isAbort = false;
    UnitTask.RsAbortException rsAbortException;

    private volatile RSFutureStatus status;
    private UnitTask unitTask;

    enum RSFutureStatus{
        NEW,             //尚未提供到线程池执行
        RUNNING,        //正在执行
        NORMAL,         //正常执行完成，可能稍后会因为其他task的失败而回滚
        ABORT,          //捕捉到RsAbortException，通知执行器回滚
        ROLLINGBACK,   //相关任务正在回滚
        ROLLBACKSUCCESSFULLY,    //任务对应的回滚操作执行完成
        ROLLBACKFAILED      //执行回滚操作失败，并且已经达到回滚次数上限
    }

    //TODO:应该添加可以赋予状态的新构造函数
    public RSFutureTask(Callable<T> callable) {
        super(callable);
        status = RSFutureStatus.NEW;
    }

    public RSFutureTask(Runnable runnable, T result) {
        super(runnable, result);
        status = RSFutureStatus.NEW;
    }

    RSFutureTask(Runnable runnable,RSFutureStatus status){
        super(runnable,null);
        this.status = status;
    }

    @Override
    public void run() {
        status = RSFutureStatus.RUNNING;
        super.run();
    }

    @Override
    protected void done() {
        try{
            if(!isCancelled())
                get();
        }catch (ExecutionException executionException){
            Throwable cause = executionException.getCause();
            if(cause instanceof UnitTask.RsAbortException){
                isAbort = true;
                rsAbortException = (UnitTask.RsAbortException) cause;
            }
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * 在FutureTask执行过程中，捕捉到{@link distribute_transaction.execute.UnitTask.RsAbortException}
     * 表明用户主动回滚整个事务，回滚事务
     * @return
     */
    public boolean isAbort() {
        return isAbort;
    }

    public UnitTask.RsAbortException getRsAbortException() {
        return rsAbortException;
    }

     RSFutureStatus getStatus(){
        return this.status;
    }

    public UnitTask getUnitTask() {
        return unitTask;
    }

    public void setUnitTask(UnitTask unitTask) {
        this.unitTask = unitTask;
    }

    void rollbackFailed(){
         unitTask.rollbackFailed();
    }

    boolean shouldRollback(){
        return unitTask.shouldRollback();
    }

    /**
     * 在上一次的回滚操作中执行失败，
     * 重置rollback状态
     */
    public void rollbackReset(){
        //TODO：实现rollbackReset
    }
}
