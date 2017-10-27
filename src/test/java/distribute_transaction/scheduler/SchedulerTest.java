package distribute_transaction.scheduler;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Created by swqsh on 2017/9/25.
 */
public class SchedulerTest {

    private Scheduler scheduler;
    private Logger logger;

    @Before
    public void init(){
        scheduler = new Scheduler();
        scheduler.start();
        logger = Logger.getLogger(SchedulerTest.class);
    }

    @Test
    public void fireTransaction() throws Exception {
        Transaction transaction = new Transaction(1,null,"requestStr");
        transaction.setMessage("transaction 1");
        scheduler.fireTransaction(transaction);
        TimeUnit.SECONDS.sleep(5);
    }

    @Test
    public void scheduleSingleTransaction() throws InterruptedException {
        Range<Integer> requestRange = new Range<Integer>(1,5, Lock.X,"user");
        List<Range<Integer>> ranges = new ArrayList<Range<Integer>>();
        ranges.add(requestRange);
        Transaction transaction = new Transaction(1,ranges,"requestStr");
        transaction.setMessage("transaction 1");
        scheduler.schedule(transaction);
        TimeUnit.SECONDS.sleep(5);
    }

    /**
     * 请求如下：
     * 事务id		申请资源范围(id)		操作类型(r表示读,w表示写)
     1			20~50				r
     2			10~30				w
     3			40~60				r
     4			30~45				w
     5			70~80				r
     6			75~90				r
     7			120~200				w
     */
    @Test
    public void multipleTransaction() throws InterruptedException {
        List<Range<Integer>> ranges1 = new ArrayList<Range<Integer>>();
        ranges1.add(new Range<Integer>(20,50, Lock.S,"user"));
        Transaction transaction1 = new Transaction(1,ranges1,"transaction1");
        transaction1.setMessage("transaction1");

        List<Range<Integer>> ranges2 = new ArrayList<Range<Integer>>();
        ranges2.add(new Range<Integer>(10,30, Lock.X,"user"));
        Transaction transaction2 = new Transaction(2,ranges2,"transaction2");
        transaction2.setMessage("transaction2");

        List<Range<Integer>> ranges9 = new ArrayList<Range<Integer>>();
        ranges9.add(new Range<Integer>(30,30, Lock.X,"user"));
        Transaction transaction9 = new Transaction(9,ranges9,"transaction9");
        transaction9.setMessage("transaction9");

        List<Range<Integer>> ranges3 = new ArrayList<Range<Integer>>();
        ranges3.add(new Range<Integer>(40,60, Lock.S,"user"));
        Transaction transaction3 = new Transaction(3,ranges3,"transaction3");
        transaction3.setMessage("transaction3");

        List<Range<Integer>> ranges4 = new ArrayList<Range<Integer>>();
        ranges4.add(new Range<Integer>(30,45, Lock.X,"user"));
        Transaction transaction4 = new Transaction(4,ranges4,"transaction4");
        transaction4.setMessage("transaction4");

        List<Range<Integer>> ranges5 = new ArrayList<Range<Integer>>();
        ranges5.add(new Range<Integer>(70,80, Lock.S,"user"));
        Transaction transaction5 = new Transaction(5,ranges5,"transaction5");
        transaction5.setMessage("transaction5");

        List<Range<Integer>> ranges6 = new ArrayList<Range<Integer>>();
        ranges6.add(new Range<Integer>(75,90, Lock.S,"user"));
        Transaction transaction6 = new Transaction(6,ranges6,"transaction6");
        transaction6.setMessage("transaction6");

        List<Range<Integer>> ranges7 = new ArrayList<Range<Integer>>();
        ranges7.add(new Range<Integer>(90,120, Lock.X,"user"));
        Transaction transaction7 = new Transaction(7,ranges7,"transaction7");
        transaction7.setMessage("transaction7");
        scheduler.schedule(transaction1);
        scheduler.schedule(transaction2);
        scheduler.schedule(transaction3);
        scheduler.schedule(transaction4);
        scheduler.schedule(transaction5);
        scheduler.schedule(transaction6);
        scheduler.schedule(transaction7);
        scheduler.schedule(transaction9);
        TimeUnit.SECONDS.sleep(1000);
    }

    @Test
    public void replay() throws InterruptedException {
        List<Range<Integer>> ranges1 = new ArrayList<Range<Integer>>();
        ranges1.add(new Range<Integer>(55,57, Lock.X,"user"));
        Transaction transaction1 = new Transaction(1,ranges1,"transaction1");
        transaction1.setMessage("transaction1");

        List<Range<Integer>> ranges2 = new ArrayList<Range<Integer>>();
        ranges2.add(new Range<Integer>(78,83, Lock.S,"user"));
        Transaction transaction2 = new Transaction(2,ranges2,"transaction2");
        transaction2.setMessage("transaction2");

        List<Range<Integer>> ranges3 = new ArrayList<Range<Integer>>();
        ranges3.add(new Range<Integer>(33,60, Lock.S,"user"));
        Transaction transaction3 = new Transaction(3,ranges3,"transaction3");
        transaction3.setMessage("transaction3");

        List<Range> ranges4 = new ArrayList<Range>();
        ranges4.add(new Range<Integer>(80,85, Lock.X,"user"));
        ranges4.add(new Range<String>("hello","world", Lock.X,"hello"));
        Transaction transaction4 = new Transaction(4,ranges4,"transaction4");
        transaction4.setMessage("transaction4");

        scheduler.schedule(transaction1);
        scheduler.schedule(transaction2);
        scheduler.schedule(transaction3);
        scheduler.schedule(transaction4);
        TimeUnit.SECONDS.sleep(10);
    }

    @Test
    public void longTest() throws InterruptedException {
        Random random = new Random();
        //List<Transaction> transactions = new ArrayList<Transaction>();
        //List<Range<Integer>> rangesList = new ArrayList<Range<Integer>>();
        for(int i=0;i<100000;i++){
            int left = random.nextInt(100000);
            int right = left+ random.nextInt(100000-left);
            int lockType = random.nextInt(2);
            Lock lock;
            if(lockType==1)
                lock = Lock.S;
            else
                lock = Lock.X;
            Range<Integer> range = new Range<Integer>(left,right,lock,"user");
            List<Range<Integer>> ranges = new ArrayList<Range<Integer>>();
            //rangesList.add(range);
            ranges.add(range);
            Transaction transaction = new Transaction(i,ranges,"transaction"+i);
            //transactions.add(transaction);
            scheduler.schedule(transaction);
        }
        TableResource resource = scheduler.getTableResource("user");
        TimeUnit.SECONDS.sleep(2);
        resource.printProcessTimes();
        /*for(int i=0;i<transactions.size();i++){
            scheduler.schedule(transactions.get(i));
        }*/
        TimeUnit.SECONDS.sleep(1000);
    }

    @After
    public void tearDown(){
        scheduler.close();
    }

}