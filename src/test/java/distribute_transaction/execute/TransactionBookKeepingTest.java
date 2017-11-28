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

    ExecutorService es = Executors.newCachedThreadPool();
    Transaction transaction = new Transaction(1,null);
    TransactionBookKeeping transactionBookKeeping;

    /**
     * task1~task6均为正常完成任务，test pass
     * @throws InterruptedException
     */
    @Test
    public void bookKeepingTest() throws InterruptedException {
        SimpleTransactionService simpleTransactionService = new SimpleTransactionService();
        checkService(simpleTransactionService);
    }

    @Test
    public void submitInWrongOrder() throws InterruptedException {
        TaskSubmitInWrongOrderTransactionService service = new TaskSubmitInWrongOrderTransactionService();
        checkService(service);
    }

    /**
     * 正常回滚pass
     * @throws Exception
     */
    @Test
    public void rollbackSuccessfully() throws Exception{
        RollbackSuccessService service = new RollbackSuccessService();
        checkService(service);
    }

    @Test
    public void rollbackFailed() throws Exception{
        RollbackFailedService service = new RollbackFailedService();
        checkService(service);
    }

    private void checkService(TransactionService service) throws InterruptedException {
        transactionBookKeeping = new TransactionBookKeeping(transaction,service);
        es.submit(service);
        TimeUnit.SECONDS.sleep(2);
    }
}
