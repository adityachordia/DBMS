package index.bplusTree;

/*
    * A LeafNode contains keys and block ids.
    * Looks Like -
    * # entries | prev leafnode | next leafnode | ptr to next free offset | blockid_1 | len(key_1) | key_1 ...
    * Note: Only write code where specified!
 */
public class LeafNode<T> extends BlockNode implements TreeNode<T>{

    Class<T> typeClass;

    public LeafNode(Class<T> typeClass) {
        
        super();
        this.typeClass = typeClass;

        // set numEntries to 0
        byte[] numEntriesBytes = new byte[2];
        numEntriesBytes[0] = 0;
        numEntriesBytes[1] = 0;
        this.write_data(0, numEntriesBytes);

        // set ptr to next free offset to 8
        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 8;
        this.write_data(6, nextFreeOffsetBytes);

        return;
    }

    // returns the keys in the node - will be evaluated
    @Override
    public T[] getKeys() {

        int numKeys = getNumKeys();
        T[] keys = (T[]) new Object[numKeys];

        /* Write your code here */
        int startPos = 8;
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

    // returns the block ids in the node - will be evaluated
    public int[] getBlockIds() {

        int numKeys = getNumKeys();
        int[] block_ids = new int[numKeys];

        /* Write your code here */
        int startPos = 8;
        for (int i = 0; i < numKeys; i++) {
            byte[] key_data = this.get_data(startPos, 2);
            block_ids[i] = ((key_data[0]) << 8) | (key_data[1] & 0xFF);

            startPos += 2;

            byte[] key_lenByte = this.get_data(startPos, 2);
            int key_len = ((key_lenByte[0]) << 8) | (key_lenByte[1] & 0xFF);
            startPos += 2;
            startPos += key_len;
        }
        return block_ids;
    }

    public int[] getLenKeys(){
        int numKeys = getNumKeys();
        int[] len_keys = new int[numKeys];

        int startPos = 8;
        for(int i = 0; i < numKeys; i++){
            startPos += 2;
            byte[] keyLen = this.get_data(startPos, 2);
            len_keys[i] = ((keyLen[0]) << 8) | (keyLen[1] & 0xFF);
            startPos += 2;
            startPos += len_keys[i];
        }
        return len_keys;
    }


    // can be used as helper function - won't be evaluated
    @Override
    public void insert(T key, int block_id) {
//        System.out.println("sjkfheasou");
        /* Write your code here */
//        if(this.search(key) != -1) return;           // Doubt if duplicates allowed then change
        byte[] key_to_array = convertToByteArray(key);
        int n = key_to_array.length;

        T[] keys = getKeys();
//        System.out.println("size of keys:" + keys.length);
        int[] blockIds = getBlockIds();
        int[] lenKeys = getLenKeys();
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
        int offs = 8;
        for(int i = 0; i < keys.length; i++){
            if(comparator(keys[i], key) > 0){
                idx = i;
                break;
            }
            offs += lenKeys[i];
            offs += 4;
        }
//        System.out.println("idx "+ idx);
        // write directly the data using offs variable

        // block_id
        byte[] temp = new byte[] {(byte) (block_id >> 8), (byte) block_id};
        this.write_data(offs, temp);
        offs += 2;

        // length
        byte[] temp2 = new byte[] {(byte) (n >> 8), (byte) n};
        this.write_data(offs, temp2);
        offs += 2;

        // data (key)
        this.write_data(offs, key_to_array);
        offs += n;

        for(int i = idx; i < keys.length; i++){
            // block_id
            byte[] temp3 = new byte[] {(byte) (blockIds[i] >> 8), (byte) blockIds[i]};
            this.write_data(offs, temp3);
            offs += 2;

            // length
            byte[] temp4 = new byte[] {(byte) (lenKeys[i] >> 8), (byte) lenKeys[i]};
            this.write_data(offs, temp4);
            offs += 2;

            // data (key)
            byte[] temp5 = convertToByteArray(keys[i]);
            this.write_data(offs, temp5);
            offs += lenKeys[i];
        }

        // update pointer to last offset
        byte[] temp6 = new byte[] {(byte) (offs >> 8), (byte) (offs)};
        this.write_data(6, temp6);

        // update key count
        byte[] temp7 = this.get_data(0, 2);
        int sz2 = ((temp7[0]) << 8) | (temp7[1] & 0xFF);
        sz2 += 1;
        byte[] temp8 = new byte[] {(byte) (sz2 >> 8), (byte) (sz2)};
        this.write_data(0, temp8);


//        T[] keys2 = getKeys();
//        System.out.println("size of keys:" + keys2.length);
//        int[] blockIds2 = getBlockIds();
//        int[] lenKeys2 = getLenKeys();
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
//        System.out.println("hi");

        return;
    }

    // can be used as helper function - won't be evaluated
    @Override
    public int search(T key) {

        /* Write your code here */

        T[] keys = getKeys();
        int[] blockIds = getBlockIds();

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
//
//        System.out.println("key:  " + key);
        for (int i = 0; i < keys.length; i++) {
            if (comparator(key, keys[i]) == 0) {
                return blockIds[i];
            }
        }
        return -1; // Key not found
    }
}

