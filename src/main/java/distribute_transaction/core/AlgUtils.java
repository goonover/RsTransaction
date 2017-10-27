package distribute_transaction.core;

import java.util.ArrayList;
import java.util.List;

/**
 * 一些常用算法
 * Created by swqsh on 2017/10/18.
 */
public class AlgUtils {

    /**
     * 查找相对与key的前一个元素的位置
     * @param list  升序列表
     * @param key   目标key
     * @param <T>
     * @return      目标index,小于0表示不存在。如果存在多个与key大小相同的值，返回第一个
     */
    public static <T extends Comparable<T>> int findPreOfList(List<T> list,T key){
        if(list.size()==0||list.get(0).compareTo(key)>=0)
            return -1;

        int low = 0;
        int high = list.size()-1;
        while (low<high){
            int mid = (low+high+1)/2;
            T midKey = (T) list.get(mid);
            if(midKey.compareTo(key)==0){
                while (list.get(mid).compareTo(key)==0){
                    mid--;
                }
                return mid;
            }else if(midKey.compareTo(key)>0){
                high = mid-1;
            }else{
                low = mid;
            }
        }
        return low;
    }

    /**
     * 查找相对与key的后一个元素的位置
     * @param list  升序列表
     * @param key   目标key
     * @param <T>
     * @return      目标index,小于0表示不存在。如果存在多个与key大小相同的值，返回第一个
     */
    public static <T extends Comparable<T>> int findPostOfList(List<T> list,T key){
        if(list.size()==0||list.get(list.size()-1).compareTo(key)<=0)
            return -1;

        int low = 0;
        int high = list.size()-1;
        while (low<high){
            int mid = (low+high)/2;
            T midKey = list.get(mid);
            if(midKey.compareTo(key)==0) {
                while (list.get(mid).compareTo(key) == 0)
                    mid++;
                return mid;
            }
            else if(midKey.compareTo(key)<0){
                low = mid+1;
            }else{
                high = mid;
            }
        }
        return low;
    }

    /**
     * 查找相对与key相等的位置，如找不到，则返回它的前一个元素的位置
     * @param list  升序列表
     * @param key   目标key
     * @param <T>
     * @return      目标index,小于0表示不存在。如果存在多个与key大小相同的值，返回第一个
     */
    public static <T extends Comparable<T>> int findPreOrSpecOfList(List<T> list,T key){
        if(list.size()==0||list.get(0).compareTo(key)>0)
            return -1;

        int low = 0;
        int high = list.size()-1;
        while (low<high){
            int mid = (low+high+1)/2;
            T midKey = (T) list.get(mid);
            if(midKey.compareTo(key)==0){
                while (mid>=1&&list.get(mid-1).compareTo(key)==0)
                    mid--;
                return mid;
            }else if(midKey.compareTo(key)>0){
                high = mid-1;
            }else{
                low = mid;
            }
        }
        return low;
    }

    /**
     * 查找相对与key相等的位置，如找不到，则返回它的后一个元素的位置
     * @param list  有序且不存在重复元素的列表，升序
     * @param key   目标key
     * @param <T>
     * @return      目标index,小于0表示不存在
     */
    public static <T extends Comparable<T>> int findPostOrSpecOfList(List<T> list,T key){
        if(list.size()==0||list.get(list.size()-1).compareTo(key)<0)
            return -1;

        int low = 0;
        int high = list.size()-1;
        while (low<high){
            int mid = (low+high)/2;
            T midKey = list.get(mid);
            if(midKey.compareTo(key)==0) {
                while (mid>=1&&list.get(mid-1).compareTo(key)==0)
                    mid--;
                return mid;
            }
            else if(midKey.compareTo(key)<0){
                low = mid+1;
            }else{
                high = mid;
            }
        }
        return low;
    }

    /**
     * 把list分为两半，新list从index到list的最后一个元素
     * @param list  原始list
     * @param index 开始位置
     * @param <T>
     * @return
     */
    public static <T> List<T> listPartition(List<T> list,final int index){
        if(index>list.size()-1)
            return null;
        List<T> res = new ArrayList<T>(list.subList(index,list.size()));
        while (list.size()>index){
            list.remove(list.size()-1);
        }
        return res;
    }

}
