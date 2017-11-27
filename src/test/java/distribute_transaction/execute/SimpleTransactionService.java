package distribute_transaction.execute;

/**
 * Created by swqsh on 2017/11/27.
 */
public class SimpleTransactionService extends TransactionService {
    @Override
    public void run() {
        System.out.println("simpleTransactionService starting......");
        SimpleUnitTask theFirstUnitTask = new SimpleUnitTask("theFirstTask");
        theFirstUnitTask.setPriority(1);

        SimpleUnitTask theSecondUnitTask = new SimpleUnitTask("theSecondTask");
        theSecondUnitTask.setPriority(2);

        SimpleUnitTask theThirdUnitTask = new SimpleUnitTask("theThirdTask");
        theThirdUnitTask.setPriority(2);
        submit(theFirstUnitTask);
        submit(theSecondUnitTask);
        submit(theThirdUnitTask);
        System.out.println("simpleTransactionService ending......");
    }
}
