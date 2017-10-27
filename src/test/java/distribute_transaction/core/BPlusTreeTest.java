package distribute_transaction.core;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.*;

/**
 * Created by swqsh on 2017/10/24.
 */
public class BPlusTreeTest {
    @Test
    public void insert() throws Exception {
        BPlusTree<Integer> bPlusTree = new BPlusTree<Integer>(100);
        Random random = new Random();
        for(int i=0;i<10000;i++){
            try {
                int value = random.nextInt(100000);
                bPlusTree.insert(i);
            }catch (Exception e){
                System.out.println(i);
                throw e;
            }
        }
        for(int i=0;i<10000;i++){
            bPlusTree.find(i);
        }
        //bPlusTree.printAll();
        String s = "abc";
    }

    @Test
    public void replay(){
        BPlusTree<Integer> bPlusTree = new BPlusTree<Integer>(4);
        int[] nums = new int[]{2,88,34,25,92,83,69,96,27,38,6,22,54};
        for(int i=0;i<nums.length;i++){
            if(nums[i]==22){
                System.out.println();
            }
            bPlusTree.insert(nums[i]);
        }
    }

}