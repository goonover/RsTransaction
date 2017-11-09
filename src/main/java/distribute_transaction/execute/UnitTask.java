package distribute_transaction.execute;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * 提供给用户使用的单元任务,在使用时，用户必须覆盖{@code run}方法，实际调用时也是
 * 执行run方法
 * Created by swqsh on 2017/11/9.
 */
public abstract class UnitTask {

    FutureTask futureTask;

    //同一事务多个unit task之间的执行顺序由优先级决定，多个task机能串行也可以并行
    int priority = 0;

    abstract public <T> T run(Object... args);

    /**
     *  返回run方法的执行结果
     */
    public <T> T get() throws ExecutionException, InterruptedException {
        return (T) futureTask.get();
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    /**
     * 当在运行任务期间，捕捉到此类型的任务，即表明用户想要放弃该事务，
     * 执行器应当执行回滚操作
     */
    public static class RsAbortException extends RuntimeException{

        private static final long serialVersionUID = -1;
        private String message = "no message specified";

        public RsAbortException(String message) {
            this.message = message;
        }

        public RsAbortException(Throwable cause, String message) {
            super(cause);
            if(cause.getMessage()!=null){
                this.message = cause.getMessage();
            }else if(cause.getCause()!=null){
                this.message = cause.getCause().getMessage();
            }
        }

        @Override
        public String getMessage() {
            return message;
        }
    }

}
