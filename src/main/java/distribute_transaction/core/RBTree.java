package distribute_transaction.core;

import java.util.Stack;

/**
 * 红黑树，详细算法参考算法导论
 * Created by swqsh on 2017/9/20.
 */
public class RBTree<T extends Comparable<T>> {

    RBTreeNode<T> nil = new RBTreeNode<T>(null,null,null,null, RBTreeNodeColor.BLACK);

    private RBTreeNode<T> root = nil;

    private int size = 0;

    public RBTreeNode<T> search(T key){
        RBTreeNode<T> temp = root;
        while (temp!=nil){
            if(temp.key.compareTo(key)==0)
                return temp;
            else if(temp.key.compareTo(key)>0){
                temp = temp.left;
            }else
                temp = temp.right;
        }
        return null;
    }

    /**
     * 红黑树的插入操作,树内所有的key值唯一，不允许重复key存在
     * @param key   插入值
     * @return  是否插入成功，在树中已有T时会返回失败
     */
    public boolean insert(T key){
        RBTreeNode<T> target = new RBTreeNode<T>(nil,nil,nil,key, RBTreeNodeColor.RED);
        RBTreeNode<T> parentOfTarget = nil;
        RBTreeNode<T> temp = root;
        while (temp!=nil){
            parentOfTarget = temp;
            if(temp.key.compareTo(key)==0) {
                return false;
            } else if(temp.key.compareTo(key)>0){
                temp = temp.left;
            }else{
                temp = temp.right;
            }
        }
        target.parent = parentOfTarget;

        if(parentOfTarget==nil)
            root = target;
        else if(parentOfTarget.key.compareTo(key)>0)
            parentOfTarget.left = target;
        else
            parentOfTarget.right = target;
        insertFixUp(target);
        size++;

        return true;
    }

    /**
     * 插入红结点后可能违反红黑树的红节点不存在红色子结点这一性质，
     * 由于如果target.parent == nil时，nil的颜色永远是黑的，循环终止，
     * 所以没有检查target.parent.child是否为空的必要。而在deleteFixUp
     * 则不一样
     * @param target    插入点
     */
    private void insertFixUp(RBTreeNode target){
        RBTreeNode parent;
        RBTreeNode uncle;
        while (target.parent.color == RBTreeNodeColor.RED){
            parent = target.parent;
            //镜像操作
            if(parent == parent.parent.left){
                uncle = parent.parent.right;
                //case 1
                if(uncle.color == RBTreeNodeColor.RED){
                    parent.color = RBTreeNodeColor.BLACK;
                    uncle.color = RBTreeNodeColor.BLACK;
                    parent.parent.color = RBTreeNodeColor.RED;
                    target = parent.parent;
                }else{
                    //case 2
                    if(target==parent.right){
                        target = target.parent;
                        rotateLeft(target);
                    }
                    //case 3
                    target.parent.color = RBTreeNodeColor.BLACK;
                    target.parent.parent.color = RBTreeNodeColor.RED;
                    rotateRight(target.parent.parent);
                }
            }else{
                uncle = parent.parent.left;
                if(uncle.color == RBTreeNodeColor.RED){
                    parent.color = RBTreeNodeColor.BLACK;
                    uncle.color = RBTreeNodeColor.BLACK;
                    parent.parent.color = RBTreeNodeColor.RED;
                    target = parent.parent;
                }else{
                    if(target==parent.left){
                        target = target.parent;
                        rotateRight(target);
                    }
                    target.parent.color = RBTreeNodeColor.BLACK;
                    target.parent.parent.color = RBTreeNodeColor.RED;
                    rotateLeft(target.parent.parent);
                }
            }
        }
        //处理插入新节点或者case 1传播到root的情况
        root.color = RBTreeNodeColor.BLACK;
    }

    /**
     * 删除key为key的结点，如果成功删除，会返回该结点，否则返回null
     * @param key
     * @return
     */
    public RBTreeNode<T> delete(T key){
        RBTreeNode<T> target;
        if((target=search(key))!= null) {
            return delete(target);
        }
        else
            return null;
    }

    /**
     * 如果在树中存在结点的值为目标值则返回值为target的结点，否则返回值小于key的结点
     * 中值最大的结点。如不存在前继结点，则返回空
     * @param target    目标值
     * @return  null---------------------->不存在前继结点
     *          res.key==target------------>值为target的结点
     *          res.key<target------------->target的前继结点
     */
    public RBTreeNode<T> findNodeOrPredecessor(T target){
        Stack<RBTreeNode<T>> stack = new Stack<RBTreeNode<T>>();
        RBTreeNode<T> node = root;
        while (node!=nil){
            stack.push(node);
            //找到结点
            if(node.key.compareTo(target)==0)
                return node;
            else if(node.key.compareTo(target)>0){
                node = node.left;
            }else
                node = node.right;
        }
        return getPredecessorFromStack(stack,target);
    }

    /**
     * 返回目标值target的前继结点，不存在时返回null
     * @param target    目标值
     * @return          目标值对应的前继节点
     */
    public RBTreeNode<T> findPredecessor(T target){
        Stack<RBTreeNode<T>> stack = new Stack<RBTreeNode<T>>();
        RBTreeNode<T> node = root;
        while (node!=nil){
            stack.push(node);
            if(node.key.compareTo(target)==0){
                if(node.left!=nil){
                   return findMaximumOfNode(node.left);
                }else{
                   return getPredecessorFromStack(stack,target);
                }
            }else if(node.key.compareTo(target)>0){
                node = node.left;
            }else{
                node = node.right;
            }
        }

        return getPredecessorFromStack(stack,target);
    }

    private RBTreeNode<T> getPredecessorFromStack(Stack<RBTreeNode<T>> stack, T target){
        RBTreeNode<T> node;
        while (!stack.empty()){
            node = stack.pop();
            //找到前继
            if(node.key.compareTo(target)<0)
                return node;
        }
        return null;
    }

    /**
     * 如果在树中存在结点的值为目标值则返回值为target的结点，否则返回值大于key的结点
     * 中值最小的结点。如不存在前继结点，则返回空
     * @param target    目标值
     * @return  null---------------------->不存在前继结点
     *          res.key==target------------>值为target的结点
     *          res.key>target------------->target的前继结点
     */
    public RBTreeNode<T> findNodeOrSuccessor(T target){
        Stack<RBTreeNode<T>> stack = new Stack<RBTreeNode<T>>();
        RBTreeNode<T> node = root;
        while (node!=nil){
            stack.push(node);
            if(node.key.compareTo(target)==0)
                return node;
            else if(node.key.compareTo(target)<0){
                node = node.right;
            }else{
                node = node.left;
            }
        }
        return  getSuccessorFromStack(stack,target);
    }

    public RBTreeNode<T> findSuccessor(T target){
        Stack<RBTreeNode<T>> stack = new Stack<RBTreeNode<T>>();
        RBTreeNode<T> node = root;
        while (node!=nil){
            stack.push(node);
            if(node.key.compareTo(target)==0){
                if(node.right!=nil){
                    return findMinimumOfNode(node.right);
                }else{
                    return getSuccessorFromStack(stack,target);
                }
            }else if(node.key.compareTo(target)>0){
                node = node.left;
            }else{
                node = node.right;
            }
        }
        return getSuccessorFromStack(stack,target);
    }

    /**
     * 从遍历过的结点中寻找后继结点
     * @param stack
     * @param target
     * @return
     */
    private RBTreeNode<T> getSuccessorFromStack(Stack<RBTreeNode<T>> stack, T target){
        RBTreeNode<T> node;
        while (!stack.empty()){
            node = stack.pop();
            if(node.key.compareTo(target)>0)
                return node;
        }
        return null;
    }

    private RBTreeNode<T> delete(RBTreeNode<T> nodeToBeRemoved){
        //当fixColor为黑时，会导致树的结构被破坏
        RBTreeNodeColor fixColor = nodeToBeRemoved.color;
        RBTreeNode replacer;
        RBTreeNode fixUpNode;
        if(nodeToBeRemoved.left == nil){
            replacer = nodeToBeRemoved.right;
            RBTransplant(nodeToBeRemoved,replacer);
            fixUpNode = replacer;
        }else if(nodeToBeRemoved.right == nil){
            replacer = nodeToBeRemoved.left;
            RBTransplant(nodeToBeRemoved,replacer);
            fixUpNode = replacer;
        }else{
            //获取nodeToBeRemoved的后继节点
            replacer = findMinimumOfNode(nodeToBeRemoved.right);
            fixColor = replacer.color;
            RBTreeNode rcOfReplacer = replacer.right;
            fixUpNode = rcOfReplacer;
            if(replacer.parent!=nodeToBeRemoved){
                RBTransplant(replacer,rcOfReplacer);
                replacer.right = nodeToBeRemoved.right;
                nodeToBeRemoved.right.parent = replacer;
            }else if(fixUpNode == nil){
                fixUpNode.parent = replacer;
            }
            RBTransplant(nodeToBeRemoved,replacer);
            replacer.left = nodeToBeRemoved.left;
            replacer.left.parent = replacer;
            replacer.color = nodeToBeRemoved.color;
        }
        if(fixColor == RBTreeNodeColor.BLACK)
            deleteFixUp(fixUpNode);
        return nodeToBeRemoved;
    }

    private void deleteFixUp(RBTreeNode fixUpNode) {
        while (fixUpNode!=root && isBlack(fixUpNode)){
            RBTreeNode brother;
            if(fixUpNode == fixUpNode.parent.left){
                brother = fixUpNode.parent.right;
                //case1
                if(isRed(brother)){
                    brother.color = RBTreeNodeColor.BLACK;
                    fixUpNode.parent.color = RBTreeNodeColor.RED;
                    rotateLeft(fixUpNode.parent);
                    brother = fixUpNode.parent.right;
                }
                //case 2
                if(isBlack(brother.left) && isBlack(brother.right)){
                    brother.color = RBTreeNodeColor.RED;
                    fixUpNode = fixUpNode.parent;
                }else if(brother != nil) {
                    if (isBlack(brother.right)) {
                        //case 3
                        brother.left.color = RBTreeNodeColor.BLACK;
                        brother.color = RBTreeNodeColor.RED;
                        rotateRight(brother);
                        brother = fixUpNode.parent.right;
                    }
                    //case 4
                    brother.color = fixUpNode.parent.color;
                    fixUpNode.parent.color = RBTreeNodeColor.BLACK;
                    brother.right.color = RBTreeNodeColor.BLACK;
                    rotateLeft(fixUpNode.parent);
                    fixUpNode = root;
                }else{
                    //case 5 CLRS忽略了兄弟节点为nil节点的情况，直接往上走即可
                    fixUpNode.color = RBTreeNodeColor.BLACK;
                    fixUpNode = fixUpNode.parent;
                }
            }else{
                brother = fixUpNode.parent.left;
                //case 1
                if(isRed(brother)){
                    brother.color = RBTreeNodeColor.BLACK;
                    fixUpNode.parent.color = RBTreeNodeColor.RED;
                    rotateRight(fixUpNode.parent);
                    brother = fixUpNode.parent.left;
                }
                //case 2
                if(isBlack(brother.left) && isBlack(brother.right)){
                    brother.color = RBTreeNodeColor.RED;
                    fixUpNode = fixUpNode.parent;
                }else if(brother != nil) {
                    //case 3
                    if(isBlack(brother.left)){
                        brother.color = RBTreeNodeColor.RED;
                        brother.right.color = RBTreeNodeColor.BLACK;
                        rotateLeft(brother);
                        brother = fixUpNode.parent.left;
                    }
                    //case 4
                    brother.color = fixUpNode.parent.color;
                    fixUpNode.parent.color = RBTreeNodeColor.BLACK;
                    brother.left.color = RBTreeNodeColor.BLACK;
                    rotateRight(fixUpNode.parent);
                    fixUpNode = root;
                }else{
                    fixUpNode.color = RBTreeNodeColor.BLACK;
                    fixUpNode = fixUpNode.parent;
                }
            }
        }
        fixUpNode.color = RBTreeNodeColor.BLACK;
    }

    private boolean isBlack(RBTreeNode node){
        return node != null && node.color == RBTreeNodeColor.BLACK;
    }

    private boolean isRed(RBTreeNode node){
        return node != null && node.color == RBTreeNodeColor.RED;
    }

    /**
     * 找当前子树的最小节点
     * @param node
     * @return
     */
    private RBTreeNode<T> findMinimumOfNode(RBTreeNode node){
        RBTreeNode<T> res = node;
        while (res.left != nil)
            res = res.left;
        return res;
    }

    private RBTreeNode<T> findMaximumOfNode(RBTreeNode<T> node){
        RBTreeNode<T> res = node;
        while (res.right != nil)
            res = res.right;
        return res;
    }

    /**
     * 用一个结点取代另一个结点
     * @param prev  原结点
     * @param target    目标结点
     */
    private void RBTransplant(RBTreeNode prev, RBTreeNode target){
        if(prev.parent == nil)
            root = target;
        else if(prev == prev.parent.left)
            prev.parent.left = target;
        else
            prev.parent.right = target;
        target.parent = prev.parent;
    }

    //左旋操作
    private void rotateLeft(RBTreeNode target){
        RBTreeNode rc = target.right;
        RBTreeNode parent = target.parent;
        target.right = rc.left;
        if(rc.left!=nil){
            rc.left.parent = target;
        }
        if(parent==nil)
            root = rc;
        else if(target == parent.left){
            parent.left = rc;
        }else{
            parent.right = rc;
        }
        rc.parent = parent;
        rc.left = target;
        target.parent = rc;
    }

    //右旋
    private void rotateRight(RBTreeNode target){
        RBTreeNode lc = target.left;
        RBTreeNode parent = target.parent;
        target.left = lc.right;
        if(lc.right!=nil)
            lc.right.parent = target;
        target.parent = lc;
        if(parent==nil){
            root = lc;
        }else if(target == parent.left){
            parent.left = lc;
        }else{
            parent.right = lc;
        }
        lc.parent = parent;
        lc.right = target;
    }

    public RBTreeNode getRoot() {
        return root;
    }

    public int getSize() {
        return size;
    }

    //遍历
    public void traverse(){
        if(nil.parent!=null)
        System.out.println("nil.parent:"+nil.parent.getKey()+";  nil.left:"+nil.left+";  nil.right:"+nil.right);
        traverse(root);
    }

    private void traverse(RBTreeNode node){
        if(node!=nil){
            System.out.println(node.color+" "+node.getKey());
            traverse(node.left);
            traverse(node.right);
        }
    }
}
