package distribute_transaction.scheduler;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static distribute_transaction.scheduler.Lock.S;

/**
 * Created by swqsh on 2017/11/4.
 */
public class ErrorReplyGenerator {

    public static List<Transaction> generateTransaction(String rangeStr,String tableName){
        int indexOfTransaction = 0;
        int indexOfLeft = 0;
        int indexOfRight = 0;
        int indexOfLockModel = 0;
        List<Transaction> res = new ArrayList<>();
        while ((indexOfTransaction=rangeStr.indexOf("transaction:"))!=-1){
            rangeStr = rangeStr.substring(indexOfTransaction+"transaction:".length());
            int indexOfNext = rangeStr.indexOf(";");
            long transactionId = Long.parseLong(rangeStr.substring(0,indexOfNext));
            indexOfLeft = rangeStr.indexOf("left:");
            rangeStr = rangeStr.substring(indexOfLeft+"left:".length());
            indexOfNext = rangeStr.indexOf(";");
            Integer left = Integer.parseInt(rangeStr.substring(0,indexOfNext));
            indexOfRight = rangeStr.indexOf("right:");
            rangeStr = rangeStr.substring(indexOfRight+"right:".length());
            indexOfNext = rangeStr.indexOf(";");
            Integer right = Integer.parseInt(rangeStr.substring(0,indexOfNext));
            indexOfLockModel = rangeStr.indexOf("lockModel:");
            rangeStr = rangeStr.substring(indexOfLockModel+"lockModel:".length());
            indexOfNext = rangeStr.indexOf("\n");
            String lockModel = rangeStr.substring(0,indexOfNext);
            Lock lock;
            if(lockModel.equals("X")){
                lock = Lock.X;
            }else{
                lock = S;
            }
            Range range = new Range(left,right,lock,tableName);
            List<Range> ranges = new ArrayList<>();
            ranges.add(range);
            String transactionStr = "transaction:"+transactionId;
            Transaction newTransaction = new Transaction(transactionId,ranges,transactionStr);
            res.add(newTransaction);
        }
        return res;
    }

    @Test
    public void testGenerateTransaction(){
        String applyStr = "transaction:0;   left:18;   right:18;   lockModel:S\n" +
                "transaction:1;   left:15;   right:18;   lockModel:S\n" +
                "transaction:2;   left:17;   right:17;   lockModel:S\n" +
                "transaction:3;   left:12;   right:17;   lockModel:S\n" +
                "transaction:4;   left:14;   right:18;   lockModel:X\n" +
                "transaction:5;   left:11;   right:12;   lockModel:S\n" +
                "transaction:6;   left:14;   right:15;   lockModel:X\n" +
                "transaction:7;   left:11;   right:13;   lockModel:X\n" +
                "transaction:8;   left:4;   right:19;   lockModel:X\n" +
                "transaction:9;   left:4;   right:8;   lockModel:S\n";
        generateTransaction(applyStr,"user");
    }
}
