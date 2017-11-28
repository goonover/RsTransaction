package distribute_transaction.execute;

/**
 * Created by swqsh on 2017/11/27.
 */
class SimpleUnitTask extends BasicUnitTask {

    SimpleUnitTask(String taskName){
        super(taskName);
    }

}

class RollbackSuccessUnitTask extends BasicUnitTask{

    RollbackSuccessUnitTask(String taskName) {
        super(taskName);
    }

    @Override
    public <T> T run() {
        super.run();
        throw new RSAbortException(taskName+" should rollback");
    }

    @Override
    boolean shouldRollback() {
        return true;
    }
}

class RollbackFailedUnitTask extends BasicUnitTask{

    RollbackFailedUnitTask(String taskName) {
        super(taskName);
    }

    @Override
    public <T> T run() {
        return super.run();
    }

    @Override
    void rollback() {
        super.rollback();
        throw new RSRollbackException(taskName+" rollback failed");
    }
}

class BasicUnitTask extends UnitTask{

    String taskName;
    int remains = 1;

    BasicUnitTask(String taskName){
        this.taskName = taskName;
    }

    @Override
    public <T> T run() {
        System.out.println("UnitTask--"+taskName+" run");
        return null;
    }

    @Override
    void rollback() {
        System.out.println("UnitTask--"+taskName+" rollback");
    }

    @Override
    void rollbackFailed() {
        System.out.println("UnitTask--"+taskName+" rollback failed");
    }

    @Override
    boolean shouldRollback() {
        return remains-->0;
    }
}