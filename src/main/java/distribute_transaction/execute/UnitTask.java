package distribute_transaction.execute;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * 提供给用户使用的单元任务,在使用时，用户必须提供{@code run}方法，实际调用时也是
 * 执行run方法
 * Created by swqsh on 2017/11/9.
 */
public abstract class UnitTask {

    FutureTask futureTask;
    //当前UnitTask处于的Generation
    private Generation generation;

    //同一事务多个unit task之间的执行顺序由优先级决定，多个task机能串行也可以并行
    Integer priority = 0;

    /**
     * 使用者必须实现run方法，UnitTask被执行时，调用run方法
     * run方法本身不接受任何参数，返回泛型结果，如需要参数，
     * 可以实现传进UnitTask中，如下所示：
     *
     * PlusTask extends UnitTask{
     *     int a;
     *     int b;
     *
     *     public PlusTask(int a,int b){
     *         this.a = a;
     *         this.b = b;
     *     }
     *
     *     public Integer run(){
     *         return a+b;
     *     }
     * }
     * @param <T>
     * @return
     */
    abstract public <T> T run();

    /**
     * 当run方法执行过程中出现RSAbortException时，将会通知generation
     * 回滚所有任务，回滚的实现通过调用rollback实现，使用者可以重载
     * rollback，实现自己的回滚函数
     */
    abstract void rollback();

    abstract void rollbackFailed();

    abstract boolean shouldRollback();

    /**
     *  返回run方法的执行结果
     */
    public <T> T get() throws ExecutionException, InterruptedException {
        return (T) futureTask.get();
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    void setGeneration(Generation generation) {
        this.generation = generation;
    }

    Generation getGeneration() {
        return generation;
    }

    /**
     * 当在运行任务期间，捕捉到此类型的任务，即表明用户想要放弃该事务，
     * 执行器应当执行回滚操作
     */
    public static class RSAbortException extends RuntimeException{

        private static final long serialVersionUID = -1;
        private String message = "no message specified";

        public RSAbortException(String message) {
            this.message = message;
        }

        public RSAbortException(Throwable cause, String message) {
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

    /**
     * 在任务回滚阶段期间，出现该异常表明回滚失败，如果shouldRollback为真
     * 的话，进行回滚
     */
    public static class RSRollbackException extends RuntimeException{

        private static final long serialVersionUID = -2;
        private String message = "no message specified";

        public RSRollbackException(String message) {
            this.message = message;
        }

        public RSRollbackException(Throwable cause, String message) {
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
