package distribute_transaction.scheduler;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 负责调度{@link Transaction}的执行顺序
 * Created by swqsh on 2017/9/4.
 */
public class Scheduler extends Thread{

    private final int unAllocatedSize = 500;
    private final int allocatedSize = 500;
    private final int toExecutedSize = 500;

    //待调度事务队列
    BlockingQueue<Transaction> unAllocatedTransactions;
    //事务已经申请了资源，但还没完全得到所有资源，需要等待的队列
    BlockingQueue<Transaction> allocatedTransactions;
    //成功获取到要执行所需的资源的事务队列
    BlockingQueue<Transaction> toExecuteTransactions;
    //运行失败或者调度失败的队列
    private LinkedBlockingQueue<Transaction> failedTransactions;
    //运行完毕，等待释放
    BlockingQueue<Transaction> releaseTransactions;

    //真正的调度实现由resourceManager来实现的
    private ResourceManager resourceManager;

    //执行线程池
    private TransactionExecutor transactionExecutor;

    private AtomicBoolean shutdown = new AtomicBoolean(true);

    //ResourceManager的配置文件路径
    private String configPath = "./src/main/resources/resources.json";

    public Scheduler(){
        super("scheduler");
        resourceManager = new ResourceManager(configPath);
    }

    public Scheduler(String configPath){
        this.configPath = configPath;
        resourceManager = new ResourceManager(configPath);
    }

    @Override
    public void start(){
        init();
        transactionExecutor.start();
        super.start();
    }

    public void close(){
        this.shutdown.set(true);
        transactionExecutor.close();
    }

    private void init(){
        unAllocatedTransactions = new ArrayBlockingQueue<Transaction>(unAllocatedSize);
        allocatedTransactions = new ArrayBlockingQueue<Transaction>(allocatedSize);
        toExecuteTransactions = new ArrayBlockingQueue<Transaction>(toExecutedSize);
        failedTransactions = new LinkedBlockingQueue<Transaction>();
        releaseTransactions = new LinkedBlockingQueue<Transaction>();
        transactionExecutor = new TransactionExecutor(toExecuteTransactions,releaseTransactions);
        shutdown.set(false);
    }

    //Sequencer使用Scheduler的唯一接口
    public void schedule(Transaction transaction){
        try {
            unAllocatedTransactions.put(transaction);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void run(){
        while (!shutdown.get()) {
            Transaction transactionToRelease;
            while ((transactionToRelease=releaseTransactions.poll())!=null){
                transactionToRelease.complete();
            }

            Transaction transactionToAllocate = null;
            if(allocatedTransactions.size()<allocatedSize) {
                transactionToAllocate = unAllocatedTransactions.poll();
            }

            if(transactionToAllocate!=null) {
                transactionToAllocate.setScheduler(this);
                resourceManager.schedule(transactionToAllocate);
            }
        }
    }

    /**
     * 事务满足执行条件，如果再allocatedTransaction中存在事务实例，先将其去除然后添加到执行队列中
     * @param transaction
     */
    synchronized void fireTransaction(Transaction transaction){
        if(allocatedTransactions.contains(transaction)){
            allocatedTransactions.remove(transaction);
        }
        try {
            toExecuteTransactions.put(transaction);
        } catch (InterruptedException e) {
            fireTransaction(transaction);
        }
    }

    void waitTransaction(Transaction transaction){
        try {
            allocatedTransactions.put(transaction);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    void transactionCompleted(Transaction transaction){
        if(unAllocatedTransactions.contains(transaction))
            unAllocatedTransactions.remove(transaction);
        if(allocatedTransactions.contains(transaction))
            allocatedTransactions.remove(transaction);
        if(toExecuteTransactions.contains(transaction))
            toExecuteTransactions.remove(transaction);
    }

    TableResource getTableResource(String name){
       return resourceManager.getTableResource(name);
    }

}
