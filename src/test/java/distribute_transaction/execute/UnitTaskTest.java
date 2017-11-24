package distribute_transaction.execute;

import org.apache.log4j.Logger;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Objects;
import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 * Created by swqsh on 2017/11/9.
 */
public class UnitTaskTest {

    Logger logger = Logger.getLogger(UnitTaskTest.class);

    @Test
    public void get() throws Exception {
        UnitTask task = new FirstUnitTask();
        System.out.println(task.getClass());
        for(Method method :task.getClass().getDeclaredMethods()){
            System.out.println(method);
        }
        task.futureTask = new RSFutureTask(new Callable() {
            @Override
            public Object call() throws Exception{
                Method[] methods = task.getClass().getDeclaredMethods();
                Method method = null;
                for(Method temp:methods){
                    if(temp.getName().equals("run")){
                        method = temp;
                        break;
                    }
                }
                throw new UnitTask.RsAbortException("testException");
                /*if(method!=null){
                   return method.invoke(task,1,2);
                }else{
                    return null;
                }*/
            }
        });
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(task.futureTask);
        TimeUnit.MILLISECONDS.sleep(10);
        if(((RSFutureTask)task.futureTask).isAbort){
            logger.info("transaction should rollback");
        }
        try {
            task.futureTask.get();
        }catch (ExecutionException e){
            System.out.println("hello");
            if(e.getCause() instanceof UnitTask.RsAbortException){
                UnitTask.RsAbortException rsAbortException = (UnitTask.RsAbortException) e.getCause();
                System.out.println(rsAbortException.getMessage());
            }
        }
        //Integer a = task.get();
        //System.out.println(a);
    }

    class FirstUnitTask extends UnitTask{

        Integer a;
        Integer b;

        public Integer run() {
            return a+b;
        }
    }

}