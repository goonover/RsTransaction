package distribute_transaction.scheduler;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Console;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
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
        scheduler.fireTransaction(transaction);
        TimeUnit.SECONDS.sleep(5);
    }

    @Test
    public void scheduleSingleTransaction() throws InterruptedException {
        Range<Integer> requestRange = new Range<Integer>(1,5, Lock.X,"user");
        List<Range> ranges = new ArrayList<Range>();
        ranges.add(requestRange);
        Transaction transaction = new Transaction(1L,ranges,"requestStr");
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
        List<Range> ranges1 = new ArrayList<Range>();
        ranges1.add(new Range<Integer>(20,50, Lock.S,"user"));
        Transaction transaction1 = new Transaction(1,ranges1,"transaction1");
        List<Range> ranges2 = new ArrayList<Range>();
        ranges2.add(new Range<Integer>(10,30, Lock.X,"user"));
        Transaction transaction2 = new Transaction(2,ranges2,"transaction2");

        List<Range> ranges3 = new ArrayList<Range>();
        ranges3.add(new Range<Integer>(40,60, Lock.S,"user"));
        Transaction transaction3 = new Transaction(3,ranges3,"transaction3");

        List<Range> ranges4 = new ArrayList<Range>();
        ranges4.add(new Range<Integer>(30,45, Lock.X,"user"));
        Transaction transaction4 = new Transaction(4,ranges4,"transaction4");

        List<Range> ranges5 = new ArrayList<Range>();
        ranges5.add(new Range<Integer>(70,80, Lock.S,"user"));
        Transaction transaction5 = new Transaction(5,ranges5,"transaction5");

        List<Range> ranges6 = new ArrayList<Range>();
        ranges6.add(new Range<Integer>(75,90, Lock.S,"user"));
        Transaction transaction6 = new Transaction(6,ranges6,"transaction6");

        List<Range> ranges7 = new ArrayList<Range>();
        ranges7.add(new Range<Integer>(90,120, Lock.X,"user"));
        Transaction transaction7 = new Transaction(7,ranges7,"transaction7");
        scheduler.schedule(transaction1);
        scheduler.schedule(transaction2);
        scheduler.schedule(transaction3);
        scheduler.schedule(transaction4);
        scheduler.schedule(transaction5);
        scheduler.schedule(transaction6);
        scheduler.schedule(transaction7);
        TimeUnit.SECONDS.sleep(1000);
    }

    @Test
    public void replay() throws InterruptedException {
        List<Transaction> transactions = new ArrayList<>();

        String lastError = "transaction:0;   left:14;   right:16;   lockModel:X\n" +
                "transaction:1;   left:5;   right:18;   lockModel:X\n" +
                "transaction:2;   left:6;   right:12;   lockModel:X\n" +
                "transaction:3;   left:8;   right:16;   lockModel:S\n" +
                "transaction:4;   left:15;   right:18;   lockModel:S\n";
        transactions = ErrorReplyGenerator.generateTransaction(lastError,"user");

        for(Transaction transaction:transactions){
            scheduler.schedule(transaction);
        }
        TimeUnit.SECONDS.sleep(1000);
    }

    @Test
    public void longTest() throws InterruptedException {
        Random random = new Random();
        List<Transaction> transactions = new ArrayList<Transaction>();
        List<Range> rangesList = new ArrayList<Range>();
        while (true) {
            for (int i = 0; i < 1000000; i++) {
                int left = random.nextInt(20);
                int right = left + random.nextInt(20 - left);
                int lockType = random.nextInt(2);
                Lock lock;
                if (lockType == 1)
                    lock = Lock.S;
                else
                    lock = Lock.X;
                Range<Integer> range = new Range<Integer>(left, right, lock, "user");
                List<Range> ranges = new ArrayList<Range>();
                rangesList.add(range);
                ranges.add(range);
                Transaction transaction = new Transaction(i, ranges, "transaction" + i);
                transactions.add(transaction);
                //scheduler.schedule(transaction);
            }

                for (Transaction transaction : transactions) {
                    try {
                        scheduler.schedule(transaction);
                    }catch (Exception e){
                        e.printStackTrace();
                        TimeUnit.SECONDS.sleep(1);
                        for (Range range : rangesList) {
                            System.out.println(range);
                        }
                        TimeUnit.SECONDS.sleep(10);
                    }
                }

                TimeUnit.SECONDS.sleep(1);
            rangesList.clear();
            transactions.clear();
            TimeUnit.SECONDS.sleep(2);
        }

        /*for(int i=0;i<transactions.size();i++){
            scheduler.schedule(transactions.get(i));
        }*/
        //TimeUnit.SECONDS.sleep(1000);
    }

    public static void main(String[] args) throws InterruptedException {
        Scheduler scheduler = new Scheduler();
        scheduler.start();
        Random random = new Random();
        List<Transaction> transactions = new ArrayList<Transaction>();
        List<Range> rangesList = new ArrayList<Range>();
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("next round:");
            String input = scanner.nextLine();
            if(input.equalsIgnoreCase("n")){
                for(Range range:rangesList){
                    System.out.println(range);
                }
                break;
            }else{
                rangesList.clear();
                transactions.clear();
            }
            for (int i = 0; i < 10; i++) {
                int left = random.nextInt(20);
                int right = left + random.nextInt(20 - left);
                int lockType = random.nextInt(2);
                Lock lock;
                if (lockType == 1)
                    lock = Lock.S;
                else
                    lock = Lock.X;
                Range<Integer> range = new Range<Integer>(left, right, lock, "user");
                List<Range> ranges = new ArrayList<Range>();
                rangesList.add(range);
                ranges.add(range);
                Transaction transaction = new Transaction(i, ranges, "transaction" + i);
                transactions.add(transaction);
                //scheduler.schedule(transaction);
            }

            for (Transaction transaction : transactions) {
                scheduler.schedule(transaction);
            }
        }
    }


    @After
    public void tearDown(){
        scheduler.close();
    }

}