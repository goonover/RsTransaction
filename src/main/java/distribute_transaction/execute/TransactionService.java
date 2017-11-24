package distribute_transaction.execute;

/**
 * 当事务获取得到所有的资源之后，执行器将会调用TransactionService的
 * run方法，具体run方法的参数及实现由使用者自行实现
 * TODO:集成数据库连接管理等
 * Created by swqsh on 2017/11/10.
 */
public abstract class TransactionService {

    TransactionBookKeeping bookKeeping;

    public boolean submit(UnitTask unitTask){
        return this.bookKeeping.submitUnitTask(unitTask);
    }

}
