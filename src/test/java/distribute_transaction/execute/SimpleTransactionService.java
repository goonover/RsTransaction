package distribute_transaction.execute;

import java.util.concurrent.TimeUnit;

/**
 * Created by swqsh on 2017/11/27.
 */
public class SimpleTransactionService extends TransactionService {
    @Override
    public void run() {
        SimpleUnitTask task1 = new SimpleUnitTask("task1");
        task1.setPriority(1);
        SimpleUnitTask task2 = new SimpleUnitTask("task2");
        SimpleUnitTask task3 = new SimpleUnitTask("task3");
        SimpleUnitTask task4 = new SimpleUnitTask("task4");
        task2.setPriority(2);
        task3.setPriority(2);
        task4.setPriority(2);
        SimpleUnitTask task5 = new SimpleUnitTask("task5");
        SimpleUnitTask task6 = new SimpleUnitTask("task6");
        task5.setPriority(3);
        task6.setPriority(3);
        submit(task1);
        submit(task2);
        submit(task3);
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        submit(task4);

        submit(task5);
        submit(task6);
     }
}

class TaskSubmitInWrongOrderTransactionService extends TransactionService{

    @Override
    public void run() {
        SimpleUnitTask task1 = new SimpleUnitTask("task1");
        SimpleUnitTask task2 = new SimpleUnitTask("task2");
        task1.setPriority(1);
        task2.setPriority(0);
        submit(task1);
        submit(task2);
    }
}

class RollbackSuccessService extends TransactionService{

    @Override
    public void run() {
        SimpleUnitTask task1 = new SimpleUnitTask("task1");
        task1.setPriority(1);
        SimpleUnitTask task2 = new SimpleUnitTask("task2");
        SimpleUnitTask task3 = new SimpleUnitTask("task3");
        SimpleUnitTask task4 = new SimpleUnitTask("task4");
        task2.setPriority(2);
        task3.setPriority(2);
        task4.setPriority(2);
        SimpleUnitTask task5 = new SimpleUnitTask("task5");
        UnitTask task6 = new RollbackSuccessUnitTask("task6");
        task5.setPriority(3);
        task6.setPriority(3);
        submit(task1);
        submit(task2);
        submit(task3);
        submit(task4);
        submit(task5);
        submit(task6);
    }
}

class RollbackFailedService extends TransactionService{

    @Override
    public void run() {
        SimpleUnitTask task1 = new SimpleUnitTask("task1");
        task1.setPriority(1);
        SimpleUnitTask task2 = new SimpleUnitTask("task2");
        SimpleUnitTask task3 = new SimpleUnitTask("task3");
        UnitTask task4 = new RollbackFailedUnitTask("task4");
        task2.setPriority(2);
        task3.setPriority(2);
        task4.setPriority(2);
        SimpleUnitTask task5 = new SimpleUnitTask("task5");
        UnitTask task6 = new RollbackSuccessUnitTask("task6");
        task5.setPriority(3);
        task6.setPriority(3);
        submit(task1);
        submit(task2);
        submit(task3);
        submit(task4);
        submit(task5);
        submit(task6);
    }
}
