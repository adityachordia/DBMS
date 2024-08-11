package index.bplusTree;

import storage.AbstractFile;

import java.util.List;
import java.util.Queue;

import java.util.ArrayList;
import java.util.LinkedList;

/*
    * Tree is a collection of BlockNodes
    * The first BlockNode is the metadata block - stores the order and the block_id of the root node
    * The total number of keys in all leaf nodes is the total number of records in the records file.
*/

public class BPlusTreeIndexFile<T> extends AbstractFile<BlockNode> {

    Class<T> typeClass;

    // Constructor - creates the metadata block and the root node
    public BPlusTreeIndexFile(int order, Class<T> typeClass) {

        super();
        this.typeClass = typeClass;
        BlockNode node = new BlockNode(); // the metadata block
        LeafNode<T> root = new LeafNode<>(typeClass);

        // 1st 2 bytes in metadata block is order
        byte[] orderBytes = new byte[2];
        orderBytes[0] = (byte) (order >> 8);
        orderBytes[1] = (byte) order;
        node.write_data(0, orderBytes);

        // next 2 bytes are for root_node_id, here 1
        byte[] rootNodeIdBytes = new byte[2];
        rootNodeIdBytes[0] = 0;
        rootNodeIdBytes[1] = 1;
        node.write_data(2, rootNodeIdBytes);

        // push these nodes to the blocks list
        blocks.add(node);
        blocks.add(root);
    }

    private boolean isFull(int id){
        // 0th block is metadata block
        assert(id > 0);
        return blocks.get(id).getNumKeys() == getOrder() - 1;
    }

    private int getRootId() {
        BlockNode node = blocks.get(0);
        byte[] rootBlockIdBytes = node.get_data(2, 2);
        return ((rootBlockIdBytes[0] << 8)) | (rootBlockIdBytes[1] & 0xFF);
    }

    public int getOrder() {
        BlockNode node = blocks.get(0);
        byte[] orderBytes = node.get_data(0, 2);
        return ((orderBytes[0] << 8)) | (orderBytes[1] & 0xFF);
    }

    private boolean isLeaf(BlockNode node){
        return node instanceof LeafNode;
    }

    private boolean isLeaf(int id){
        return isLeaf(blocks.get(id));
    }

    private int comparator(T key1, T key2) {
        if (key1 instanceof Integer && key2 instanceof Integer) {
            return Integer.compare((Integer) key1, (Integer) key2);
        }
        else if (key1 instanceof String && key2 instanceof String) {
            return ((String) key1).compareTo((String) key2);
        }
        else if (key1 instanceof Boolean && key2 instanceof Boolean) {
            return Boolean.compare((Boolean) key1, (Boolean) key2);
        }
        else if (key1 instanceof Float && key2 instanceof Float) {
            return Float.compare((Float) key1, (Float) key2);
        }
        else if (key1 instanceof Double && key2 instanceof Double) {
            return Double.compare((Double) key1, (Double) key2);
        }
        else {
            throw new IllegalArgumentException("Unsupported key types");
        }
    }


    // will be evaluated
    public void insert(T key, int block_id) {

        /* Write your code here */

        // initialize the parent array
        ArrayList<Integer> parent = new ArrayList<>();

        // find the leaf node to which key should be inserted
        int id = getRootId();
        while(!isLeaf(id)){
            parent.add(id);
            id = ((InternalNode<T>) blocks.get(id)).search(key);
        }

        if(!isFull(id)){
            //insert directly if not full
            ((LeafNode<T>) blocks.get(id)).insert(key, block_id);
        }
        else{
            LeafNode<T> left = ((LeafNode<T>) blocks.get(id));
            int left_id = id;
            T[] keys = left.getKeys();
            int[] blockIds = left.getBlockIds();
            int[] lenKeys = left.getLenKeys();

            List<T> new_keys = new ArrayList<>();
            List<Integer> new_blockIds = new ArrayList<>();

            LeafNode<T> right = new LeafNode<>(this.typeClass);
            int right_id = blocks.size();
            blocks.add(right);

            int nKeys = keys.length;
            int split = (nKeys+1)/2;

            // find index where new node will be inserted
            int idx = nKeys;
            for(int i = 0; i < nKeys; i++){
                if(comparator(keys[i], key) > 0){
                    idx = i;
                    break;
                }
            }

            // write new data into new_keys and new_blocks
            for(int i = 0; i < idx; i++){
                new_keys.add(keys[i]);
                new_blockIds.add(blockIds[i]);
            }

            new_keys.add(key);
            new_blockIds.add(block_id);

            for(int i = idx; i < nKeys; i++){
                new_keys.add(keys[i]);
                new_blockIds.add(blockIds[i]);
            }

            // update length of p
            byte[] wrt_sz = new byte[2];
            wrt_sz[0] = (byte) (0 >> 8);
            wrt_sz[1] = (byte) (0);
            left.write_data(0, wrt_sz);

            // calculate new last offset for p
            int offs1 = 8;

            // update new last offset for p
            byte[] offs1_byte = new byte[2];
            offs1_byte[0] = (byte) (offs1 >> 8);
            offs1_byte[1] = (byte) (offs1);;
            left.write_data(6, offs1_byte);

            for(int i = 0; i < split; i++){
                left.insert(new_keys.get(i), new_blockIds.get(i));
            }

            // store next pointer of p and update next pointer of p
            byte[] old_point = left.get_data(4, 2);

            byte[] next_q = new byte[2];
            next_q[0] = (byte) (right_id >> 8);
            next_q[1] = (byte) (right_id);
            left.write_data(4, next_q);

            //  write remaining keys to q it will automatically update length and last offset
            for(int i = split; i < new_keys.size(); i++){
                right.insert(new_keys.get(i), new_blockIds.get(i));
            }

            // update prev and next pointer for q
            byte[] p_id = new byte[2];
            p_id[0] = (byte) (left_id >> 8);
            p_id[1] = (byte) (left_id);
            right.write_data(2, p_id);

            right.write_data(4, old_point);

            // update prev of next of p
            int old_id = ((old_point[0] << 8)) | (old_point[1] & 0xFF);
            if(old_id != 0){
                LeafNode<T> old_leaf = (LeafNode<T>) blocks.get(old_id);
                old_leaf.write_data(2, next_q);
            }

            insert_internal(new_keys.get(split), left_id, right_id, parent, parent.size()-1);

        }
        return;
    }

    public void insert_internal(T key, int left_block_id, int right_block_id, ArrayList<Integer> parent, int index){
        if(left_block_id == getRootId()){
//            System.out.println("boo");
            // just create a single Node with entry l key r
            InternalNode<T> new_root = new InternalNode<>(key,left_block_id,right_block_id,this.typeClass);
            byte[] rootNodeIdBytes = new byte[2];
            int blk_sz = blocks.size();
//            System.out.println("new root block: "+blk_sz);
//            System.out.println("right block: "+right_block_id);
            rootNodeIdBytes[0] = (byte) (blk_sz >> 8);
            rootNodeIdBytes[1] = (byte) (blk_sz);

            blocks.get(0).write_data(2, rootNodeIdBytes);
            blocks.add(new_root);
//            System.out.println(getRootId());
            return;
        }

        InternalNode<T> p = ((InternalNode<T>) blocks.get(parent.get(index)));
        if(!isFull(parent.get(index))){
            p.insert(key, right_block_id);
        }
        else{
            T[] keys = p.getKeys();
            int[] blockIds = p.getChildren();
            int[] lenKeys = p.getLenK();

            int nKeys = keys.length;
            int split = (nKeys+1)/2;

            List<T> new_keys = new ArrayList<>();
            List<Integer> new_blockIds = new ArrayList<>();

            // find index where new node will be inserted
            int idx = nKeys;
            for(int i = 0; i < nKeys; i++){
                if(comparator(keys[i], key) > 0){
                    idx = i;
                    break;
                }
            }

            // write new data into new_keys and new_blocks
            new_blockIds.add(blockIds[0]);
            for(int i = 0; i < idx; i++){
                new_keys.add(keys[i]);
                new_blockIds.add(blockIds[i+1]);
            }

            new_keys.add(key);
            new_blockIds.add(right_block_id);

            for(int i = idx; i < nKeys; i++){
                new_keys.add(keys[i]);
                new_blockIds.add(blockIds[i+1]);
            }

            // create new right node
            InternalNode<T> q = new InternalNode<>(new_blockIds.get(split+1),this.typeClass);
            int q_id = blocks.size();
            blocks.add(q);

            // update length of p
            byte[] wrt_sz = new byte[2];
            wrt_sz[0] = (byte) (0 >> 8);
            wrt_sz[1] = (byte) (0);
            p.write_data(0, wrt_sz);

            // calculate new last offset for p
            int offs1 = 6;

            // update new last offset for p
            byte[] offs1_byte = new byte[2];
            offs1_byte[0] = (byte) (offs1 >> 8);
            offs1_byte[1] = (byte) (offs1);;
            p.write_data(2, offs1_byte);

            // write to p
            for(int i = 0; i < split; i++){
                p.insert(new_keys.get(i), new_blockIds.get(i+1));
            }

            //  write remaining keys to q it will automatically update length and last offset of q
            for(int i = split+1; i < new_keys.size(); i++){
                q.insert(new_keys.get(i), new_blockIds.get(i+1));
            }

            // finally recursively call insert_internal for p, keys[p_sz], q
            insert_internal(new_keys.get(split), parent.get(index), q_id, parent, index-1);
        }
    }


    // will be evaluated
    // returns the block_id of the leftmost leaf node containing the key
    public int search(T key) {

        /* Write your code here */
        int id = getRootId();
        while(!isLeaf(id)){
            id = ((InternalNode<T>) blocks.get(id)).search(key);
        }
//        return id;

        if(((LeafNode<T>) blocks.get(id)).search(key) == -1) return id;
        else{
            LeafNode<T> l = (LeafNode<T>) blocks.get(id);
            while (id != 0){
                byte[] temp = l.get_data(2, 2);

                int prev = ((temp[0] << 8)) | (temp[1] & 0xFF);
                if(prev == 0 || ((LeafNode<T>) blocks.get(prev)).search(key) == -1) break;

                id = prev;
                l = (LeafNode<T>) blocks.get(id);
            }
            return id;
        }
    }

    // returns true if the key was found and deleted, false otherwise
    // (Optional for Assignment 3)
    public boolean delete(T key) {

        /* Write your code here */
        return false;
    }

    // DO NOT CHANGE THIS - will be used for evaluation
    public void print_bfs() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            if(isLeaf(id)) {
                ((LeafNode<T>) blocks.get(id)).print();
            }
            else {
                ((InternalNode<T>) blocks.get(id)).print();
                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                }
            }
        }
        return;
    }

    // DO NOT CHANGE THIS - will be used for evaluation
    public ArrayList<T> return_bfs() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        ArrayList<T> bfs = new ArrayList<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            if(isLeaf(id)) {
                T[] keys = ((LeafNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
                    bfs.add((T) keys[i]);
                }
            }
            else {
                T[] keys = ((InternalNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
                    bfs.add((T) keys[i]);
                }
                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                }
            }
        }
        return bfs;
    }

    public void prt_bfs() {
        System.out.println();
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
//        ArrayList<T> bfs = new ArrayList<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            if(isLeaf(id)) {
                T[] keys = ((LeafNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
//                    bfs.add((T) keys[i]);
                    System.out.print(keys[i] + " ");
                }
            }
            else {
                T[] keys = ((InternalNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
//                    bfs.add((T) keys[i]);
                    System.out.print(keys[i] + " ");
                }
                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                }
            }
            System.out.println();
        }
    }

    public void print() {
        print_bfs();
        return;
    }

}
