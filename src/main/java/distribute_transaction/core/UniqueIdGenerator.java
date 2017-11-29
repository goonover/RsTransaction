package distribute_transaction.core;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * ID生成器，用于生成事务ID，采用snowflake算法，最高位保留，具体
 * 实现参考voltdb UniqueIdGenerator
 * TIMESTAMP------>40位
 * COUNTER BIT----->10位
 * REMAINS BIT------>13位
 * 最高位保留    TIMESTAMP BIT       REMAINS BIT         COUNTER BIT
 * 初始时间为东八区 2017年1月1日
 * Created by swqsh on 2017/11/29.
 */
public class UniqueIdGenerator {

    static final long TIMESTAMP_BIT = 40;
    static final long REMAINS_BIT = 10;
    static final long COUNTER_BIT = 13;

    static final long RS_EPOCH = getInitEpoch();

    private long lastUnqId = 0;
    private long lastUsedTime = -1;
    private long lastCounterValue = 0;
    private long BACKWARD_TIME_FORGIVENESS_WINDOW_MS = 3000;
    private static final int COUNTER_MAXVALUE = (1<<COUNTER_BIT) -1;

    private static long getInitEpoch() {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(0);
        c.setTimeZone(TimeZone.getTimeZone("GMT+8"));
        c.set(2017,0,1,0,0);
        return c.getTimeInMillis();
    }

    public interface Clock{
        long get();
        void sleep(long millis) throws InterruptedException;
    }

    Clock clock;

    public UniqueIdGenerator(){
        clock = new Clock() {
            @Override
            public long get() {
                return System.currentTimeMillis();
            }

            @Override
            public void sleep(long millis) throws InterruptedException {
                Thread.sleep(millis);
            }
        };
    }

    public UniqueIdGenerator(Clock clock){
        this.clock = clock;
    }

    public synchronized long getNextUniqueId(){
        long currentTime = clock.get();
        if(currentTime == lastUsedTime) {
            lastCounterValue++;
            if (lastCounterValue > COUNTER_MAXVALUE) {
                while (currentTime == lastUsedTime) {
                    currentTime = clock.get();
                }
                lastUsedTime = currentTime;
                lastCounterValue = 0;
            }
        }else{
            if(currentTime<lastUsedTime){
                try {
                    clock.sleep(lastUsedTime-currentTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        lastUsedTime = currentTime;
        lastUnqId = makeIdFromComponentPart(currentTime,lastCounterValue);
        return lastUnqId;
    }

    public static long makeIdFromComponentPart(long currentTime, long lastCounterValue) {
        long uniqueId = currentTime - RS_EPOCH;
        uniqueId = uniqueId << (REMAINS_BIT+COUNTER_BIT);
        uniqueId = uniqueId|lastCounterValue;
        return uniqueId;
    }

    public static long getTimestampFromUniqueId(long uniqueId){
        long time = uniqueId >> (REMAINS_BIT+COUNTER_BIT);
        return time+RS_EPOCH;
    }

    public static long getSequenceFromUniqueId(long uniqueId){
        return uniqueId & COUNTER_MAXVALUE;
    }

    public static Date getDateFromUniqueId(long uniqueId){
        long time = uniqueId >> (REMAINS_BIT+COUNTER_BIT);
        return new Date(time+RS_EPOCH);
    }

    public static void main(String[] args){
        UniqueIdGenerator idGenerator = new UniqueIdGenerator();
        idGenerator.getNextUniqueId();
        long start = System.currentTimeMillis();
        long id = idGenerator.getNextUniqueId();
        System.out.println(id);
        System.out.println(getSequenceFromUniqueId(id));
        System.out.println(getDateFromUniqueId(id));
    }

}
