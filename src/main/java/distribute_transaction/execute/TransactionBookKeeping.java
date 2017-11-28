package distribute_transaction.execute;

import distribute_transaction.scheduler.Transaction;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 记录事务执行相关的事务服务{@link TransactionService}以及子任务执行情况
 * Created by swqsh on 2017/11/23.
 */
public class TransactionBookKeeping {

    static AtomicLong globalBookKeepingId = new AtomicLong(0);
    //当前实例id
    private long bookKeepingId;
    //当前实例相关的事务及其事务服务
    Transaction transaction;
    TransactionService transactionService;

    HashMap<Integer,Generation> generations = new HashMap<>();
    Integer currentGeneration;
    Integer maxGeneration;
    Logger logger = Logger.getLogger(TransactionBookKeeping.class);

    public TransactionBookKeeping(Transaction transaction, TransactionService transactionService) {
        this.bookKeepingId = globalBookKeepingId.getAndIncrement();
        this.transaction = transaction;
        this.transactionService = transactionService;
        transactionService.setBookKeeping(this);
    }

    boolean submitUnitTask(UnitTask task){
        //第一个任务
        if(maxGeneration==null){
            Generation newGeneration = new Generation(task,this);
            generations.put(task.getPriority(),newGeneration);
            maxGeneration = task.getPriority();
            fireGeneration(newGeneration);
            return true;
        }else if(task.getPriority()<maxGeneration){
            return false;
        }

        Generation max = generations.get(maxGeneration);
        if (task.getPriority().equals(maxGeneration)) {
            max.enqueue(task);
        }else{
            //当前的generation比最大的generation还要大
            Generation newGeneration = new Generation(task,this);
            newGeneration.setPrevGenerationId(maxGeneration);
            maxGeneration = newGeneration.getGenerationId();
            generations.put(newGeneration.getGenerationId(),newGeneration);
            max.setNextGenerationId(newGeneration.getGenerationId());
        }

        return true;
    }

    private void fireGeneration(Generation generation){
        currentGeneration = generation.getGenerationId();
        generation.fire();
    }

    void generationSucceedAll(Generation generation){
        //TODO:判断当前的TransactionService是否完成，如完成则代表事务正常执行完成
        logger.info(this+" completeSuccessfully");
    }

    void generationFinished(Generation generation){
        Generation nextGeneration = generations.get(generation.getNextGenerationId());
        if(nextGeneration!=null){
            nextGeneration.fire();
        }
    }

    /**
     * 一个generation的task回调执行完成，如果出现回调错误，则停止继续执行
     * 其他generation的回调，作为异常情况让使用者处理
     * @param generation    回调执行完的generation
     * @param success   回调过程中是否出现了{@link distribute_transaction.execute.UnitTask.RSRollbackException}
     */
    void generationRollbackCompleted(Generation generation,boolean success){
        if(!success){
            handleRollbackFailure(generation);
            return;
        }

        if(generation.getPrevGenerationId()==null){
            rollbackFinish();
        }else{
            prevCallbackInvoke(generation);
        }
    }

    private void handleRollbackFailure(Generation generation){
        //TODO:implement handleRollbackFailure
        logger.info(generation+" rollback failed");
    }

    private void prevCallbackInvoke(Generation generation){
        Generation prevGeneration = generations.get(generation.getPrevGenerationId());
        if(prevGeneration!=null)
            prevGeneration.rollback(false);
    }

    private void rollbackFinish(){
        //TODO:implementRollbackFinish
        logger.info(this+" rollbackFinish");
    }
}
