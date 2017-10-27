package distribute_transaction.scheduler;

/**
 * 四种锁级别，分别为：
 * S------>读锁
 * X------>写锁
 * Is----->读意向锁
 * SIX---->写意向锁
 * Created by swqsh on 2017/9/4.
 */
enum Lock {

    S,
    X
}
