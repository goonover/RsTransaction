package distribute_transaction.core;

/**
 * Created by swqsh on 2017/10/27.
 */
public class Common {
    /**
     *  comparable接口的相关封装
     */
    public static <T extends Comparable<T>> boolean comparableEquals(T source,T target){
        return source.compareTo(target)==0;
    }

    public static <T extends Comparable<T>> boolean comparableSmaller(T source,T target){
        return source.compareTo(target)<0;
    }

    public static <T extends Comparable<T>> boolean comparableBigger(T source,T target){
        return source.compareTo(target)>0;
    }

    public static <T extends Comparable<T>> boolean comparableNotBigger(T source,T target){
        return source.compareTo(target)<=0;
    }

    public static <T extends Comparable<T>> boolean comparableNotSmaller(T source,T target){
        return source.compareTo(target)<0;
    }

}
