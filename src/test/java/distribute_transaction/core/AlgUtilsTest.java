package distribute_transaction.core;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by swqsh on 2017/10/18.
 */
public class AlgUtilsTest {

    @Test
    public void testFindPreOfList(){
        ArrayList<Integer> list = new ArrayList<Integer>();
        list.add(1);
        list.add(3);
        list.add(6);
        list.add(7);
        list.add(10);
        list.add(12);
        list.add(13);
        assertEquals(AlgUtils.findPreOfList(list,1),-1);
        assertEquals(AlgUtils.findPreOfList(list,0),-1);
        assertEquals(AlgUtils.findPreOfList(list,5),1);
        assertEquals(AlgUtils.findPreOfList(list,10),3);
        assertEquals(AlgUtils.findPreOfList(list,7),2);
        assertEquals(AlgUtils.findPreOfList(list,15),6);
    }
    
    @Test
    public void testFindPreOrSpecOfList(){
        ArrayList<Integer> list = new ArrayList<Integer>();
        list.add(1);
        list.add(3);
        list.add(6);
        list.add(7);
        list.add(10);
        list.add(12);
        list.add(13);
        assertEquals(AlgUtils.findPreOrSpecOfList(list,1),0);
        assertEquals(AlgUtils.findPreOrSpecOfList(list,0),-1);
        assertEquals(AlgUtils.findPreOrSpecOfList(list,5),1);
        assertEquals(AlgUtils.findPreOrSpecOfList(list,10),4);
        assertEquals(AlgUtils.findPreOrSpecOfList(list,7),3);
        assertEquals(AlgUtils.findPreOrSpecOfList(list,15),6);
    }

    @Test
    public void testFindPostOfList(){
        ArrayList<Integer> list = new ArrayList<Integer>();
        list.add(1);
        list.add(3);
        list.add(6);
        list.add(7);
        list.add(10);
        list.add(12);
        list.add(13);
        assertEquals(AlgUtils.findPostOfList(list,15),-1);
        assertEquals(AlgUtils.findPostOfList(list,1),1);
        assertEquals(AlgUtils.findPostOfList(list,2),1);
        assertEquals(AlgUtils.findPostOfList(list,5),2);
        assertEquals(AlgUtils.findPostOfList(list,6),3);
        assertEquals(AlgUtils.findPostOfList(list,7),4);
        assertEquals(AlgUtils.findPostOfList(list,8),4);
    }

    @Test
    public void testFindPostOrSpecOfList(){
        ArrayList<Integer> list = new ArrayList<Integer>();
        list.add(1);
        list.add(3);
        list.add(6);
        list.add(7);
        list.add(10);
        list.add(12);
        list.add(13);
        assertEquals(AlgUtils.findPostOrSpecOfList(list,15),-1);
        assertEquals(AlgUtils.findPostOrSpecOfList(list,1),0);
        assertEquals(AlgUtils.findPostOrSpecOfList(list,2),1);
        assertEquals(AlgUtils.findPostOrSpecOfList(list,5),2);
        assertEquals(AlgUtils.findPostOrSpecOfList(list,6),2);
        assertEquals(AlgUtils.findPostOrSpecOfList(list,7),3);
        assertEquals(AlgUtils.findPostOrSpecOfList(list,8),4);
    }

    @Test
    public void testListPartition(){
        ArrayList<Integer> list = new ArrayList<Integer>();
        list.add(1);
        list.add(3);
        list.add(6);
        list.add(7);
        list.add(10);
        list.add(12);
        list.add(13);
        List<Integer> subList = AlgUtils.listPartition(list,6);
        assertEquals(list.size(),6);
        assertEquals(subList.size(),1);
    }

}
