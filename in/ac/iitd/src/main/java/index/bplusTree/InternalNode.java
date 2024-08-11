package index.bplusTree;

/*
    * Internal Node - num Keys | ptr to next free offset | P_1 | len(K_1) | K_1 | P_2 | len(K_2) | K_2 | ... | P_n
    * Only write code where specified

    * Remember that each Node is a block in the Index file, thus, P_i is the block_id of the child node
 */
public class InternalNode<T> extends BlockNode implements TreeNode<T> {

    // Class of the key
    Class<T> typeClass;

    // Constructor - expects the key, left and right child ids
    public InternalNode(T key, int left_child_id, int right_child_id, Class<T> typeClass) {
        super();
        this.typeClass = typeClass;
//        System.out.println("fasuioerfneiufn");

        byte[] numKeysBytes = new byte[2];
        numKeysBytes[0] = 0;
        numKeysBytes[1] = 0;

        this.write_data(0, numKeysBytes);

        byte[] child_1 = new byte[2];
        child_1[0] = (byte) (left_child_id >> 8);
        child_1[1] = (byte) (left_child_id);

        this.write_data(4, child_1);

        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 6;

        this.write_data(2, nextFreeOffsetBytes);

        // also calls the insert method
        this.insert(key, right_child_id);
        return;
    }

    public InternalNode(int left_child_id, Class<T> typeClass) {

        super();
        this.typeClass = typeClass;

        byte[] numKeysBytes = new byte[2];
        numKeysBytes[0] = 0;
        numKeysBytes[1] = 0;

        this.write_data(0, numKeysBytes);

        byte[] child_1 = new byte[2];
        child_1[0] = (byte) (left_child_id >> 8);
        child_1[1] = (byte) (left_child_id);

        this.write_data(4, child_1);

        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 6;

        this.write_data(2, nextFreeOffsetBytes);

        return;
    }

    // returns the keys in the node - will be evaluated
    @Override
    public T[] getKeys() {

        int numKeys = getNumKeys();
        T[] keys = (T[]) new Object[numKeys];

        /* Write your code here */
        int startPos = 4;
        for (int i = 0; i < numKeys; i++) {
            startPos += 2;
            byte[] key_lenByte = this.get_data(startPos, 2);
            int key_len = ((key_lenByte[0]) << 8) | (key_lenByte[1] & 0xFF);
            startPos += 2;
            byte[] key_data = this.get_data(startPos, key_len);
            keys[i] = convertBytesToT(key_data, typeClass); //doubt
            startPos += key_len;
        }
        return keys;
    }

    public int[] getLenK(){
        int numKeys = getNumKeys();
        int[] len_keys = new int[numKeys];

        int startPos = 4;
        for(int i = 0; i < numKeys; i++){
            startPos += 2;
            byte[] keyLen = this.get_data(startPos, 2);
            len_keys[i] = ((keyLen[0]) << 8) | (keyLen[1] & 0xFF);
            startPos += 2;
            startPos += len_keys[i];
        }
        return len_keys;
    }


    // can be used as helper function -
    @Override
    public void insert(T key, int right_block_id) {
        /* Write your code here */
//        System.out.println("Search: "+key+" : "+search(key));
        byte[] key_to_array = convertToByteArray(key);
        int n = key_to_array.length;

        T[] keys = getKeys();
        int[] blockIds = getChildren();
        int[] lenKeys = getLenK();

//        System.out.println("size of keys:" + keys.length);
//        System.out.print("keys: ");
//        for(int i = 0; i < keys.length; i++){
//            System.out.print(keys[i] + " ");
//        }
//        System.out.println();
//        System.out.print("Blockids: ");
//        for(int i = 0; i < blockIds.length; i++){
//            System.out.print(blockIds[i] + " ");
//        }
//        System.out.println();

        int idx = keys.length;
        int offs = 4;
        for(int i = 0; i < keys.length; i++){
            if(comparator(keys[i], key) > 0){
                idx = i;
                break;
            }
            offs += lenKeys[i];
            offs += 4;
        }
        offs += 2;
        // write directly the data using offs variable

        // length
        byte[] temp2 = new byte[] {(byte) (n >> 8), (byte) n};
        this.write_data(offs, temp2);
        offs += 2;

        // data (key)
        this.write_data(offs, key_to_array);
        offs += n;

        // block_id
        byte[] temp = new byte[] {(byte) (right_block_id >> 8), (byte) right_block_id};
        this.write_data(offs, temp);
        offs += 2;

        // write remaining blocks
        for(int i = idx; i < keys.length; i++){
            // length
            byte[] temp4 = new byte[] {(byte) (lenKeys[i] >> 8), (byte) lenKeys[i]};
            this.write_data(offs, temp4);
            offs += 2;

            // data (key)
            byte[] temp5 = convertToByteArray(keys[i]);
            this.write_data(offs, temp5);
            offs += lenKeys[i];

            // block_id
            byte[] temp3 = new byte[] {(byte) (blockIds[i+1] >> 8), (byte) blockIds[i+1]};
            this.write_data(offs, temp3);
            offs += 2;
        }

        // write the last block id // Incorrect
//        byte[] temp6 = new byte[] {(byte) (blockIds[keys.length] >> 8), (byte) blockIds[keys.length]};
//        this.write_data(offs, temp6);
//        offs += 2;

        // update last offset pointer
        byte[] temp7 = new byte[] {(byte) (offs >> 8), (byte) (offs)};
        this.write_data(2, temp7);

        // update key count
        byte[] temp8 = this.get_data(0, 2);
        int sz2 = ((temp8[0]) << 8) | (temp8[1] & 0xFF);
        sz2 += 1;
        byte[] temp9 = new byte[] {(byte) (sz2 >> 8), (byte) (sz2)};
        this.write_data(0, temp9);

//        System.out.println("fisuefnawuifnaeuifnawfiuoaefn");

//        T[] keys2 = getKeys();
//        int[] blockIds2 = getChildren();
//        int[] lenKeys2 = getLenK();
//        System.out.print("keys: ");
//        for(int i = 0; i < keys2.length; i++){
//            System.out.print(keys2[i] + " ");
//        }
//        System.out.println();
//        System.out.print("Blockids: ");
//        for(int i = 0; i < blockIds2.length; i++){
//            System.out.print(blockIds2[i] + " ");
//        }
//        System.out.println();


        return;
    }

    // can be used as helper function - won't be evaluated
    @Override
    public int search(T key) {
        /* Write your code here */
        T[] keys = this.getKeys();
        int[] children = this.getChildren();

        for (int i = 0; i < keys.length; i++) {
            if (comparator(keys[i], key) > 0) {        // Doubt
                return children[i];
            }
        }

        // If no such key is found, return the last child
        return children[keys.length];
    }
//    The for loop iterates over each key in the current internal node.
//comparator(keys[i], key) > 0 compares the current key (keys[i]) with the given key.
//If comparator(keys[i], key) > 0, it means that the given key is less than keys[i].
//When a key is found that is greater than the given key, the method returns the child node
// ID corresponding to that key (children[i]).


    /*Let's illustrate how this search method works with a B+ Tree example:

Example Internal Node
Suppose we have an internal node with the following keys and children:

Keys: [10, 20, 30]
Children: [1, 2, 3, 4]
Here, 1, 2, 3, and 4 are IDs of the child nodes corresponding to the ranges defined by the keys:

Child 1: Contains keys less than 10
Child 2: Contains keys between 10 and 20
Child 3: Contains keys between 20 and 30
Child 4: Contains keys greater than 30
Example 1: Key 15
Iteration:
Compare 15 with 10: comparator(10, 15) < 0 (so 15 is greater than 10)
Compare 15 with 20: comparator(15, 20) < 0 (so 15 is less than 20)
Result: The key 15 should be in Child 2 (ID 2), so the method returns 2.
Example 2: Key 25
Iteration:
Compare 25 with 10: comparator(10, 25) < 0 (so 25 is greater than 10)
Compare 25 with 20: comparator(20, 25) < 0 (so 25 is greater than 20)
Compare 25 with 30: comparator(25, 30) < 0 (so 25 is less than 30)
Result: The key 25 should be in Child 3 (ID 3), so the method returns 3.
Example 3: Key 35
Iteration:
Compare 35 with 10: comparator(10, 35) < 0 (so 35 is greater than 10)
Compare 35 with 20: comparator(20, 35) < 0 (so 35 is greater than 20)
Compare 35 with 30: comparator(30, 35) < 0 (so 35 is greater than 30)
Result: Since 35 is greater than all keys, it should be in Child 4 (ID 4), so the method returns 4.
Summary
The search method in an internal node of a B+ Tree:

Compares the given key with each key in the node.
Determines which child node to traverse based on these comparisons.
Returns the ID of the appropriate child node, ensuring the search continues down the tree to find or insert the key.


*/

    // should return the block_ids of the children - will be evaluated
    public int[] getChildren() {

        byte[] numKeysBytes = this.get_data(0, 2);
        int numKeys = (numKeysBytes[0] << 8) | (numKeysBytes[1] & 0xFF);

        int[] children = new int[numKeys + 1];

        /* Write your code here */
        int startPos = 4;
        for (int i = 0; i < numKeys+1; i++) {
            byte[] id_data = this.get_data(startPos, 2);
            children[i] = ((id_data[0]) << 8) | (id_data[1] & 0xFF);

            startPos += 2;
            // break if last iteration
            if(i == numKeys) break;

            // if not last then move offset by key and len
            byte[] key_lenByte = this.get_data(startPos, 2);
            int key_len = ((key_lenByte[0]) << 8) | (key_lenByte[1] & 0xFF);
            startPos += 2;
            startPos += key_len;
        }
        return children;
    }
}

