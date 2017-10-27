package distribute_transaction.core;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by swqsh on 2017/10/22.
 */
public class SimpleListTest {

    @Test
    public void listCopy(){
        List<Integer> preList = new ArrayList<Integer>();
        preList.add(1);
        preList.add(3);
        preList.add(5);
        preList.add(7);
        preList.add(9);
        List<Integer> postList;
        postList = preList.subList(2,preList.size());
        postList.add(11);
        System.out.println(postList);
        System.out.println(preList);
    }

}
