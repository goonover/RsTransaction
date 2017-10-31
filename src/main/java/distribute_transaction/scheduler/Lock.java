package distribute_transaction.scheduler;

/**
 * 目前只支持两种锁级别，分别为：
 * S------>读锁
 * X------>写锁
 * 由于放弃了初始的B+树封锁策略，暂时没有用意向锁的必要
 * Created by swqsh on 2017/9/4.
 */
enum Lock {

    S,
    X
}
