package distribute_transaction.core;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by swqsh on 2017/10/18.
 */
public class BPlusTreeNode<T extends Comparable<T>> {
    //B+树的度
    int degree;

    boolean isLeaf;

    List<T> keys;

    List<BPlusTreeNode<T>> children;

    BPlusTreeNode next;

    BPlusTreeNode parent;

    public BPlusTreeNode(int degree,boolean isLeaf){
        this.degree = degree;
        this.isLeaf = isLeaf;
        nodeInit();
    }

    private void nodeInit(){
        if(!isLeaf){
            keys = new ArrayList<T>(degree-1);
            children = new ArrayList<BPlusTreeNode<T>>(degree);
        }else{
            keys = new ArrayList<T>(degree-1);
        }
    }

    public boolean isFull(){
        return keys.size() >= degree-1;
    }

    protected void setParent(BPlusTreeNode<T> parent){
        this.parent = parent;
    }

    boolean hasNext(){
        return next!=null;
    }

}
