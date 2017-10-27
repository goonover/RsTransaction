package distribute_transaction.core;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

import static distribute_transaction.core.RBTreeNodeColor.BLACK;
import static distribute_transaction.core.RBTreeNodeColor.RED;
import static org.junit.Assert.*;

/**
 * Created by swqsh on 2017/9/20.
 */
public class RBTreeTest {
    @Test
    public void findPredecessor() throws Exception {
        RBTree<Integer> tree = new RBTree<Integer>();
        tree.insert(100);
        tree.insert(50);
        tree.insert(150);
        tree.insert(23);
        tree.insert(76);
        tree.insert(65);
        tree.insert(87);
        assertEquals(tree.getSize(),7);
        assertEquals(tree.findPredecessor(75).key,(Integer)65);
        assertEquals(tree.findPredecessor(76),tree.findPredecessor(75));
        assertEquals(tree.findPredecessor(87).key,(Integer)76);
        assertNull(tree.findPredecessor(23));
    }

    @Test
    public void findSuccessor() throws Exception {
        RBTree<Integer> tree = new RBTree<Integer>();
        tree.insert(100);
        tree.insert(50);
        tree.insert(150);
        tree.insert(23);
        tree.insert(76);
        tree.insert(65);
        tree.insert(87);
        assertEquals(tree.getSize(),7);
        assertEquals(tree.findSuccessor(50).key,(Integer)65);
        assertEquals(tree.findSuccessor(23).key,(Integer)50);
        assertEquals(tree.findSuccessor(65).key,(Integer)76);
        assertNull(tree.findSuccessor(150));
    }

    @Test
    public void findNodeOrSuccessor() throws Exception {
        RBTree<Integer> tree = new RBTree<Integer>();
        tree.insert(100);
        tree.insert(50);
        tree.insert(150);
        tree.insert(23);
        tree.insert(76);
        tree.insert(65);
        tree.insert(87);
        assertEquals(tree.getSize(),7);
        assertEquals(tree.findNodeOrSuccessor(50).key,(Integer)50);
        assertEquals(tree.findNodeOrSuccessor(24).key,(Integer)50);
        assertEquals(tree.findNodeOrSuccessor(59).key,(Integer)65);
        assertEquals(tree.findNodeOrSuccessor(88).key,(Integer)100);
    }

    @Test
    public void findNodeOrPredecessor() throws Exception {
        RBTree<Integer> tree = new RBTree<Integer>();
        tree.insert(100);
        tree.insert(50);
        tree.insert(150);
        tree.insert(23);
        tree.insert(76);
        tree.insert(65);
        tree.insert(87);
        assertNull(tree.findNodeOrPredecessor(22));
        assertEquals(tree.findNodeOrPredecessor(59).key,new Integer(50));
        assertEquals(tree.findNodeOrPredecessor(88).key,(Integer) 87);
    }

    @Test
    public void remove(){
        RBTree<Integer> tree = new RBTree<Integer>();
        tree.insert(20);
        tree.insert(10);
        tree.insert(50);
        tree.insert(100);
        tree.delete(100);
        tree.nil.parent = new RBTreeNode<Integer>(null,null,null,10000,BLACK);
        tree.traverse();
        tree.delete(20);
    }

    @Test
    public void delete() throws Exception {
        RBTree<Integer> tree = new RBTree<Integer>();
        tree.insert(20);
        tree.insert(15);
        tree.insert(25);
        tree.insert(23);
        tree.insert(27);
        assertEquals(tree.getSize(),5);
        Assert.assertEquals(tree.getRoot().right.key, (Integer)25);
        Assert.assertEquals(tree.getRoot().right.left.key, (Integer)23);
        Assert.assertEquals((tree.getRoot().right.left).color, RED);
        tree.delete(25);
        Assert.assertEquals(tree.getSize(), 4);
        Assert.assertEquals(tree.getRoot().key, (Integer)20);
        Assert.assertEquals(tree.getRoot().right.key, (Integer)27);
        Assert.assertEquals((tree.getRoot().right).color, BLACK);
        Assert.assertEquals(tree.getRoot().right.right.key, null);
        Assert.assertEquals(tree.getRoot().right.left.key, (Integer)23);
        Assert.assertEquals((tree.getRoot().right.left).color, RED);

        testTreeBSTProperties(tree.getRoot(),tree);
    }

    @Test
    public void search() throws Exception {
        RBTree<Integer> tree = new RBTree<Integer>();
        tree.insert(15);
        tree.insert(20);
        assertEquals(tree.search(20).key,new Integer(20));
        assertNull(tree.search(30));
    }

    @org.junit.Test
    public void insert() throws Exception {
        RBTree<Integer> tree = new RBTree<Integer>();
        tree.insert(20);
        tree.insert(15);
        assertEquals(tree.search(15).color, RED);
        tree.insert(25);
        tree.insert(10);
        //case1
        assertEquals(tree.getRoot().color, BLACK);
        assertEquals(tree.search(15).color, BLACK);
        assertEquals(tree.search(25).color, BLACK);
        tree.insert(17);
        tree.insert(8);
        assertEquals(tree.search(15).color, RED);
        assertEquals(tree.search(10).color, BLACK);
        assertEquals(tree.search(17).color, BLACK);
        assertEquals(tree.search(8).color, RED);
        //case 2
        tree.insert(9);
        assertEquals(tree.search(9).color, BLACK);
        assertEquals(tree.search(10).color, RED);
        assertEquals(tree.search(9).right,tree.search(10));

        assertNull(tree.search(99));
        testTreeBSTProperties(tree.getRoot(),tree);
    }



    private void testTreeBSTProperties(RBTreeNode entry, RBTree tree) {
        if (entry != tree.nil) {
            // test heap properties and BST properties
            if (entry.left != tree.nil) {
                Assert.assertTrue(entry.key .compareTo(entry.left.key)>0);
            }
            if (entry.right != tree.nil) {
                Assert.assertTrue(entry.key .compareTo(entry.right.key)<0);
            }
            testTreeBSTProperties(entry.left,tree);
            testTreeBSTProperties(entry.right,tree);
        }
    }

    @Test
    public void insertPerformTest(){
        long startTime = System.currentTimeMillis();
        RBTree<Integer> rbTree = new RBTree<Integer>();
        Random random = new Random();
        for(int i=0;i<70000;i++){
            rbTree.insert(random.nextInt(100000));
        }
        for(int i=0;i<70000;i++){
            rbTree.search(random.nextInt(100000));
        }
        for(int i=0;i<70000;i++){
            rbTree.delete(random.nextInt(100000));
        }
        System.out.println("consume:    "+(System.currentTimeMillis()-startTime));
    }

}