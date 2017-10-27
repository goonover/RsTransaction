package distribute_transaction.scheduler;

/**
 * Created by swqsh on 2017/9/25.
 */
public class Range<T extends Comparable<T>>{

    //范围左边界、右边界
    public T left;

    public T right;
    //当前请求范围所属事务
    public Transaction transaction;
    //请求范围的锁类型
    public Lock lockType;

    //要封锁的表名
    public String tableName;

    public Range(T left, T right, Lock lockType,String tableName){
        this.left = left;
        this.right = right;
        this.lockType = lockType;
        this.tableName = tableName;
    }

    public Range(Transaction transaction, T left, T right, Lock lockType) {
        this.left = left;
        this.right = right;
        this.transaction = transaction;
        this.lockType = lockType;
    }

    public Range(Transaction transaction, T left, T right, Lock lockType, String tableName) {
        this.left = left;
        this.right = right;
        this.transaction = transaction;
        this.lockType = lockType;
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == this)
            return true;
        if(!(obj instanceof Range))
            return false;
        Range<T> range = (Range<T>) obj;
        if(range.left.compareTo(left)==0&&range.right.compareTo(right)==0
                &&range.tableName.equals(tableName)&&lockType.equals(range.lockType))
            return true;
        return false;
    }

    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    @Override
    public String toString() {
        String res = transaction==null ? "transaction:null" : "transaction:"+transaction.getTransactionId();
        res += ";   left:"+left;
        res += ";   right:"+right;
        res += ";   lockType:"+lockType;
        return res;
    }
}
