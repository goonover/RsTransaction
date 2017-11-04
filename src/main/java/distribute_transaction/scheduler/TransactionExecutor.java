package distribute_transaction.scheduler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 事务执行线程池
 * Created by swqsh on 2017/9/5.
 */
class TransactionExecutor extends Thread{

    private BlockingQueue<Transaction> toBeExecuteTransactionQueue;
    private BlockingQueue<Transaction> releaseQueue;
    private ExecutorService executorService = Executors.newFixedThreadPool(1);
    AtomicInteger total = new AtomicInteger(0);
    long startTime;

    private volatile boolean shutdown = true;

    TransactionExecutor(BlockingQueue<Transaction> toBeExecuteTransactionQueue, BlockingQueue<Transaction> releaseQueue){
        super("TransactionExecutor");
        this.toBeExecuteTransactionQueue = toBeExecuteTransactionQueue;
        this.releaseQueue = releaseQueue;
    }

    public void start(){
        shutdown = false;
        super.start();
    }

    public void run(){
        startTime = System.currentTimeMillis();
        while (!shutdown || toBeExecuteTransactionQueue.size()>0){
            Transaction transaction = null;
            try {
                transaction = toBeExecuteTransactionQueue.poll(300, TimeUnit.MICROSECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if(transaction!=null){
                submitTask(transaction);
            }
        }
    }

    private void submitTask(final Transaction transaction){
        executorService.submit(new Runnable() {
            public void run() {
                //System.out.println(total.get()+":"+transaction.getRequestStr());
                //transaction.testSleep();
                /*System.out.println("transaction "+transaction.getTransactionId()+": start");
                try {
                    transaction.complete();
                }catch (Exception e){
                    e.printStackTrace();
                }
                //System.out.println("transaction "+transaction.getTransactionId()+": finish");
                if(total.incrementAndGet()==10000){
                    System.out.println(System.currentTimeMillis()-startTime);
                    System.out.println("finish");
                }*/
                if(total.incrementAndGet()==1000000){
                    System.out.println(System.currentTimeMillis()-startTime);
                    System.out.println("finish");
                }
                try {
                    releaseQueue.put(transaction);
                } catch (InterruptedException e) {
                    submitTask(transaction);
                }
            }
        });
    }

    public void close() {
        shutdown = true;
    }
}
