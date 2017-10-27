package distribute_transaction.core;

/**
 * 红黑树叶结点
 * Created by swqsh on 2017/9/20.
 */
public class RBTreeNode<T extends Comparable<T>> {

    public RBTreeNode<T> parent;
    public RBTreeNode<T> left;
    public RBTreeNode<T> right;

    T key;
    RBTreeNodeColor color;

    public RBTreeNode(RBTreeNode<T> parent, RBTreeNode<T> left, RBTreeNode<T> right, T key, RBTreeNodeColor color) {
        this.parent = parent;
        this.left = left;
        this.right = right;
        this.key = key;
        this.color = color;
    }

    public T getKey(){
        return key;
    }
}

enum RBTreeNodeColor{
    RED,BLACK
}
