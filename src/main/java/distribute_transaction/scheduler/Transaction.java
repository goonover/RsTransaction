package distribute_transaction.scheduler;

import java.util.List;

/**
 * 事务抽象，主要暴露给序列层使用
 * Created by swqsh on 2017/10/29.
 */
public class Transaction {

    //事务的状态，分别为等待、运行、完成和放弃四种状态
    enum State{
        WAITING,RUNNING,FINISH,ABORT
    }

    //事务Id
    private long transactionId;
    //事务申请的资源范围
    private List<Range> applyRanges;
    //事务的状态
    protected State state;
    //请求原语
    private String requestStr;

    public Transaction(long transactionId, List<Range> applyRanges) {
        this.transactionId = transactionId;
        this.applyRanges = applyRanges;
        transactionInit();
    }

    public Transaction(long transactionId, List<Range> applyRanges, String requestStr){
        this.transactionId = transactionId;
        this.applyRanges = applyRanges;
        this.requestStr = requestStr;
        transactionInit();
    }

    private void transactionInit(){
        state = State.WAITING;
        if(applyRanges==null)
            return;
        for(Range range:applyRanges){
            range.setTransaction(this);
        }
    }

    public long getTransactionId() {
        return transactionId;
    }

    public List<Range> getApplyRanges() {
        return applyRanges;
    }

    public String getRequestStr(){
        return this.requestStr;
    }

}
