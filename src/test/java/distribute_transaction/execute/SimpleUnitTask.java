package distribute_transaction.execute;

/**
 * Created by swqsh on 2017/11/27.
 */
public class SimpleUnitTask extends UnitTask {

    public String taskName;

    public SimpleUnitTask(String taskName){
        this.taskName = taskName;
    }

    @Override
    public <T> T run() {
        System.out.println("SimpleUnitTask--"+taskName+" run");
        return null;
    }

    @Override
    void rollback() {
        System.out.println("SimpleUnitTask--"+taskName+" rollback");
    }

    @Override
    void rollbackFailed() {
        System.out.println("SimpleUnitTask--"+taskName+" rollbackFailed");
    }

    @Override
    boolean shouldRollback() {
        return false;
    }
}
