package distribute_transaction.core;

import java.util.Collections;
import java.util.List;

/**
 * B+树
 * Created by swqsh on 2017/10/18.
 */
public class BPlusTree<T extends Comparable<T>> {

    BPlusTreeNode<T> root;
    //B+树的度数，默认值为100，可通过初始化参数设置
    int degree = 100;

    public BPlusTree(){}

    public BPlusTree(int degree){
        this.degree = degree;
    }

    /**
     * 定位一个键值，需要该键值所在的叶节点加上其所在的index
     */
    public class BPlusTreeEntry<T extends Comparable<T>>{

        BPlusTreeNode<T> node;

        int index;

        BPlusTreeEntry(BPlusTreeNode node,int index){
            this.node = node;
            this.index = index;
        }

        public T getKey(){
            return node.keys.get(index);
        }
    }

    /**
     * 按键值查找
     * @param key
     * @return  找不到返回空，否则返回相对应的BPlusTreeEntry
     */
    public BPlusTreeEntry<T> find(T key){
        if(root==null)
            return null;
        BPlusTreeNode<T> current = findTargetLeafNode(key);
        int index = Collections.binarySearch(current.keys,key);
        if(index<0)
            return null;
        else
            return new BPlusTreeEntry<T>(current,index);
    }


    /**
     * 向B+树插入
     */
    public void insert(T key){
        if(root == null){
            BPlusTreeNode<T> node = new BPlusTreeNode<T>(degree,true);
            node.keys.add(key);
            root = node;
        }else{
            //找到将要插入的叶结点
            BPlusTreeNode<T> targetNode = findTargetLeafNode(key);
            insertIntoLeaf(targetNode,key);
        }
    }

    private void insertIntoLeaf(BPlusTreeNode<T> targetNode, T key) {
        if(!targetNode.isFull()){
            int index = AlgUtils.findPreOfList(targetNode.keys,key)+1;
            targetNode.keys.add(index,key);
        }else{
            //根结点已满，分裂
            if(targetNode == root){
                BPlusTreeNode<T> sibling = insertFullLeafNode(targetNode,key);
                BPlusTreeNode<T> newRoot = new BPlusTreeNode<T>(degree,false);
                newRoot.children.add(targetNode);
                newRoot.children.add(sibling);
                newRoot.keys.add(sibling.keys.get(0));
                targetNode.setParent(newRoot);
                sibling.setParent(newRoot);
                root = newRoot;
            }else{
                int indexOfTargetNode = targetNode.parent.children.indexOf(targetNode);
                /**
                 * 当当前结点已满时，先向未满的兄弟结点借空间，如兄弟结点均已满，
                 * 则当前叶结点分裂，把分裂新得到的结点插入父结点中
                 */

                if(!borrowFromSibling(targetNode,indexOfTargetNode,key)){
                    //TODO:sibling没有指定父结点
                    BPlusTreeNode<T> sibling = insertFullLeafNode(targetNode,key);
                    BPlusTreeNode<T> parent = targetNode.parent;
                    insertInternalNode(parent,sibling,sibling.keys.get(0));
                }
            }
        }
    }

    /**
     * 把分裂出的key值以及相应指针插入到父结点中
     * @param parent    父结点
     * @param newNode   分裂得到的结点指针
     * @param key       指向分裂得到的指针的相对应的键值
     */
    private void insertInternalNode(BPlusTreeNode<T> parent,BPlusTreeNode<T> newNode,T key){
        int indexToBeInsert = AlgUtils.findPreOfList(parent.keys,key)+1;
        if(!parent.isFull()){
            parent.keys.add(indexToBeInsert,key);
            parent.children.add(indexToBeInsert+1,newNode);
            newNode.setParent(parent);
        }else{
            insertFullInternalNode(parent,newNode,indexToBeInsert,key);
        }
    }

    /**
     * 向一个已满的父结点中插入新结点，已满的父节点需要分裂，要么插入到上一级，要么成为新的根节点
     * @param parent    父结点
     * @param newNode   新结点
     * @param indexOfKey    插入位置
     * @param key   新结点的索引值
     */
    private void insertFullInternalNode(BPlusTreeNode<T> parent, BPlusTreeNode<T> newNode, int indexOfKey,T key) {
        parent.keys.add(indexOfKey,key);
        parent.children.add(indexOfKey+1,newNode);
        newNode.setParent(parent);
        //插入到上一级结点parent.parent中的key与结点
        int midKey = degree/2;
        BPlusTreeNode<T> newInternalNode = new BPlusTreeNode<T>(degree,false);
        newInternalNode.keys = AlgUtils.listPartition(parent.keys,midKey);
        //查到上一级目录的key
        T nextKey = newInternalNode.keys.remove(0);
        newInternalNode.children = AlgUtils.listPartition(parent.children,midKey+1);
        updateChildrenParent(newInternalNode);

        if(parent != root) {
            //插入前使内部结点的parent指针指向兄弟结点的parent
            newInternalNode.parent = parent.parent;
            insertInternalNode(parent.parent, newInternalNode, nextKey);
        }else{
            BPlusTreeNode<T> newRoot = new BPlusTreeNode<T>(degree,false);
            newRoot.keys.add(nextKey);
            newRoot.children.add(parent);
            newRoot.children.add(newInternalNode);
            parent.setParent(newRoot);
            newInternalNode.setParent(newRoot);
            root = newRoot;
        }
    }

    /**
     * 当新内部结点产生时，它会从原满结点中得到部分的key以及子结点，
     * 需要更新所有子结点的指针，使其指向新产生的结点
     * @param newInternalNode   新产生的结点
     */
    private void updateChildrenParent(BPlusTreeNode<T> newInternalNode) {
        List<BPlusTreeNode<T>> children = newInternalNode.children;
        for(BPlusTreeNode<T> child:children){
            child.setParent(newInternalNode);
        }
    }

    /**
     * 只有插入叶结点时，才可以向左右兄弟结点借空间，内部结点不允许
     * @param targetNode    目标结点
     * @param indexOfTargetNode 目标节点再父结点的位置
     * @param key   插入值
     * @return  是否借成功了
     */
    private boolean borrowFromSibling(BPlusTreeNode<T> targetNode,int indexOfTargetNode,T key){
        BPlusTreeNode<T> leftSibling = indexOfTargetNode==0 ? null :
                (BPlusTreeNode<T>) targetNode.parent.children.get(indexOfTargetNode - 1);
        BPlusTreeNode<T> rightSibling = indexOfTargetNode==targetNode.parent.children.size()-1 ? null:
                (BPlusTreeNode<T>) targetNode.parent.children.get(indexOfTargetNode + 1);
        if(leftSibling!=null && !leftSibling.isFull()){
            T migrateKey = targetNode.keys.remove(0);
            leftSibling.keys.add(migrateKey);
            int index = AlgUtils.findPreOfList(targetNode.keys,key)+1;
            targetNode.keys.add(index,key);
            targetNode.parent.keys.remove(indexOfTargetNode-1);
            targetNode.parent.keys.add(indexOfTargetNode-1,targetNode.keys.get(0));
        }else if(rightSibling!=null && !rightSibling.isFull()){
            targetNode.parent.keys.remove(indexOfTargetNode);
            T replaceKey;
            int index = 0;
            if(key.compareTo(targetNode.keys.get(degree-2))>0){
                replaceKey = key;
            }else{
                replaceKey = targetNode.keys.get(degree-2);
                targetNode.keys.remove(degree-2);
                index = AlgUtils.findPreOfList(targetNode.keys,key)+1;
                targetNode.keys.add(index,key);
            }
            rightSibling.keys.add(0,replaceKey);
            targetNode.parent.keys.add(indexOfTargetNode,replaceKey);
        }else{
            return false;
        }
        return true;
    }

    /**
     * 向一已满的叶子结点插入键值
     * @param node  目标结点
     * @param key   值
     * @return  新的兄弟结点
     */
    private BPlusTreeNode<T> insertFullLeafNode(BPlusTreeNode<T> node,T key){
        int mid = (degree-1)/2;
        BPlusTreeNode<T> sibling = new BPlusTreeNode<T>(degree,true);
        //key小于中间值，插入前半部分，否则插入后半部分
        if(node.keys.get(mid).compareTo(key)>0){
            sibling.keys = AlgUtils.listPartition(node.keys,mid);
            int index = AlgUtils.findPreOfList(node.keys,key)+1;
            node.keys.add(index,key);
        }else{
            sibling.keys = AlgUtils.listPartition(node.keys,mid+1);
            int index = AlgUtils.findPreOfList(sibling.keys,key)+1;
            sibling.keys.add(index,key);
        }
        //更新后继结点
        sibling.next = node.next;
        node.next = sibling;
        return sibling;
    }

    /**
     * 找到key值应该插入的叶结点
     */
    private BPlusTreeNode<T> findTargetLeafNode(T key) {
        BPlusTreeNode<T> currentNode = root;
        while (!currentNode.isLeaf){
            int nextGen = AlgUtils.findPreOrSpecOfList(currentNode.keys,key)+1;
            currentNode = (BPlusTreeNode<T>) currentNode.children.get(nextGen);
        }
        return currentNode;
    }

    public BPlusTreeNode<T> findFirst(){
        BPlusTreeNode<T> current = root;
        while (!current.isLeaf){
            current = (BPlusTreeNode<T>) current.children.get(0);
        }
        return current;
    }

    public void printAll(){
        BPlusTreeNode<T> first = findFirst();
        while (first!=null){
            System.out.println(first.keys);
            first = first.next;
        }
    }

}
