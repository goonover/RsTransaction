package distribute_transaction.execute;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import static distribute_transaction.execute.RSFutureTask.RSFutureStatus.*;

/**
 * Created by swqsh on 2017/11/10.
 */
public class RSFutureTask<T> extends FutureTask<T> {

    volatile boolean isAbort = false;
    Throwable throwable;

    private volatile RSFutureStatus status;
    private UnitTask unitTask;

    enum RSFutureStatus{
        NEW,             //尚未提供到线程池执行
        RUNNING,        //正在执行
        NORMAL,         //正常执行完成，可能稍后会因为其他task的失败而回滚
        ABORT,          //捕捉到RsAbortException，通知执行器回滚
        ROLLBACKNEW,   //新建执行rollback的任务，但尚未开始执行
        ROLLINGBACK,   //相关任务正在回滚
        ROLLBACKSUCCESSFULLY,    //任务对应的回滚操作执行完成
        ROLLBACKFAILED      //执行回滚操作失败，并且已经达到回滚次数上限
    }

    private RSFutureTask(Runnable runnable,T result,RSFutureStatus status){
        super(runnable,result);
        this.status = status;
    }

    private RSFutureTask(Callable<T> callable,RSFutureStatus status){
        super(callable);
        this.status = status;
    }

    public static <T> RSFutureTask createNormalTask(Callable<T> callable){
        return new RSFutureTask<T>(callable,RSFutureStatus.NEW);
    }

    public static <T> RSFutureTask createRollbackTask(Runnable runnable,T result){
        return new RSFutureTask(runnable,result,ROLLBACKNEW);
    }

    public static<T> RSFutureTask<T> createSpecTask(Callable<T> callable,RSFutureStatus status){
        return new RSFutureTask<T>(callable,status);
    }

    public static<T> RSFutureTask<T> createSpecTask(Runnable runnable,T result,RSFutureStatus status){
        return new RSFutureTask<T>(runnable,result,status);
    }

    @Override
    public void run() {
        switch (this.status){
            case NEW:
                this.status = RUNNING;
                break;
            case ROLLBACKNEW:
                this.status = ROLLINGBACK;
                break;
            default:
                return;
        }
        super.run();
    }

    @Override
    protected void done() {
        if(isCancelled()){
            this.status = ABORT;
            return;
        }
        try{
            /**
             * 处理非cancel的任务，对于被调用cancel的任务，均是由于其他任务
             * 调用cancel引起的
             */
            get();
        }catch (ExecutionException executionException){
            this.throwable = executionException.getCause();
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }finally {
            if(this.status == RUNNING){
                handleNormalComplete();
            }else if(this.status == ROLLINGBACK){
                handleRollbackComplete();
            }
        }
    }

    private void handleNormalComplete(){
        if(throwable!=null&&(throwable instanceof UnitTask.RSAbortException)){
            this.status = ABORT;
        }else{
            this.status = NORMAL;
        }
        notifyGenerationOfUnitTask();
    }

    private void handleRollbackComplete(){
        if((throwable!=null)&&(throwable instanceof UnitTask.RSRollbackException)){
            this.status = ROLLBACKFAILED;
        }else{
            this.status = ROLLBACKSUCCESSFULLY;
        }
        notifyGenerationOfUnitTask();
    }

    private void notifyGenerationOfUnitTask(){
        if(unitTask!=null&&unitTask.getGeneration()!=null)
            unitTask.getGeneration().taskDone(this);
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

}
