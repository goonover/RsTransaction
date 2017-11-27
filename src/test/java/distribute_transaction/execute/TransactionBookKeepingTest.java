package distribute_transaction.execute;

import distribute_transaction.scheduler.Transaction;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Created by swqsh on 2017/11/27.
 */
public class TransactionBookKeepingTest {

    @Test
    public void bookKeepingTest() throws InterruptedException {
        SimpleTransactionService simpleTransactionService = new SimpleTransactionService();
        Transaction transaction = new Transaction(1,null);
        TransactionBookKeeping transactionBookKeeping = new TransactionBookKeeping(transaction,simpleTransactionService);
        ExecutorService es = Executors.newCachedThreadPool();
        es.submit(simpleTransactionService);
        TimeUnit.SECONDS.sleep(100);
    }

}
