package distribute_transaction.execute;

import org.junit.Test;

import java.util.concurrent.*;

/**
 * Created by swqsh on 2017/11/24.
 */
public class InterruptedTest {

    @Test
    public void testFutureTaskInterrupt() throws InterruptedException {
        FutureTask futureTask = new FutureTask<Integer>(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                while (true){
                    System.out.println("call");
                    TimeUnit.SECONDS.sleep(1);
                }
            }
        });
        ExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.submit(futureTask);
        TimeUnit.SECONDS.sleep(3);
        futureTask.cancel(true);
        try {
            futureTask.get();
        }catch (InterruptedException e){
          e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        TimeUnit.SECONDS.sleep(10);
    }

}
