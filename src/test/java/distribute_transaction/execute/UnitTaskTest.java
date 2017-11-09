package distribute_transaction.execute;

import org.junit.Test;

import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 * Created by swqsh on 2017/11/9.
 */
public class UnitTaskTest {
    @Test
    public void get() throws Exception {
        FirstUnitTask firstUnitTask = new FirstUnitTask();
        firstUnitTask.futureTask = new FutureTask(new Callable() {
            @Override
            public Object call() throws Exception {
                return firstUnitTask.run(1,2);
            }
        });
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(firstUnitTask.futureTask);
        Integer a = firstUnitTask.get();
        System.out.println(a);
    }

    class FirstUnitTask extends UnitTask{

        @Override
        public Integer run(Object... args) {
            Integer a = (Integer) args[0];
            Integer b = (Integer) args[1];
            return a+b;
        }
    }

}