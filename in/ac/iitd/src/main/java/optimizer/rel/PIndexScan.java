package optimizer.rel;

import index.bplusTree.BPlusTreeIndexFile;
import index.bplusTree.LeafNode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import java.util.HashSet;
import java.util.Set;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.*;
import manager.StorageManager;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import storage.AbstractBlock;
import storage.AbstractFile;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

// Operator trigged when doing indexed scan
// Matches SFW queries with indexed columns in the WHERE clause
public class PIndexScan extends TableScan implements PRel {
    
        private final List<RexNode> projects;
        private final RelDataType rowType;
        private final RelOptTable table;
        private final RexNode filter;
    
        public PIndexScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, RexNode filter, List<RexNode> projects) {
            super(cluster, traitSet, table);
            this.table = table;
            this.rowType = deriveRowType();
            this.filter = filter;
            this.projects = projects;
        }
    
        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new PIndexScan(getCluster(), traitSet, table, filter, projects);
        }
    
        @Override
        public RelOptTable getTable() {
            return table;
        }

        @Override
        public String toString() {
            return "PIndexScan";
        }

        public String getTableName() {
            return table.getQualifiedName().get(1);
        }

        public int comparator(Object key1, Object key2) {
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

    public Object converter(byte[] bytes, Object value){

        /* Write your code here */

        if (value instanceof Integer) {
            return convertBytesToInteger(bytes);
        } else if (value instanceof String) {
            return new String(bytes);
        } else if (value instanceof Boolean) {
            return convertBytesToBoolean(bytes);
        } else if (value instanceof Float) {
            return convertBytesToFloat(bytes);
        } else if (value instanceof Double) {
            return convertBytesToDouble(bytes);
        } else {
            throw new UnsupportedOperationException("Conversion for the specified type is not supported");
        }
    }

    public Object convertBytesToInteger(byte[] bytes) {
        validateBArrayLength(bytes, 4);
        return ((bytes[0] << 24) | ((bytes[1] & 0xFF) << 16) | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF));
    }

    public Object convertBytesToBoolean(byte[] bytes) {
        validateBArrayLength(bytes, 1);
        return (bytes[0] != 0);
    }

    public Object convertBytesToFloat(byte[] bytes) {
        validateBArrayLength(bytes, 4);
        return (Float.intBitsToFloat(byteArrayToFloat(bytes)));
    }

    public Object convertBytesToDouble(byte[] bytes) {
        validateBArrayLength(bytes, 8);
        return (Double.longBitsToDouble(byteArrayToLong(bytes)));
    }

    public void validateBArrayLength(byte[] bytes, int expectedLength) {
        if (bytes.length != expectedLength) {
            throw new IllegalArgumentException("Byte array length does not match the expected length for conversion");
        }
    }

    public int byteArrayToFloat(byte[] bytes) {
        return (bytes[0] << 24) | ((bytes[1] & 0xFF) << 16) | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
    }

    public long byteArrayToLong(byte[] bytes) {
        long value = 0;
        for (int i = 0; i < bytes.length; i++) {
            value |= ((long) (bytes[i] & 0xFF)) << (8 * (bytes.length-1-i)); //Doubt
        }
        return value;
    }


    @Override
        public List<Object[]> evaluate(StorageManager storage_manager) {
            String tableName = getTableName();
            System.out.println("Evaluating PIndexScan for table: " + tableName);

            /* Write your code here */

            // we have to implement this query
            // we are given that for the given table and column, an index is already created (using B+ tree)
            // you need to use function create_index which is specified below

            // returns the block id of leaf node where the key is present
//            public <T> int search_index(int file_id, T key){
//                if(file_id >= files.size()){
//                    return -1;
//                }
//                AbstractFile<? extends AbstractBlock> file = files.get(file_id);
//                if(file instanceof BPlusTreeIndexFile){
//                    return ((BPlusTreeIndexFile<T>) file).search(key);
//                }
//                return -1;
//            }

            // Using this function you need to get the block id of leaf node by searching the key in B+ tree
            // then return the list of block ids of entries (of B+ tree) which match the given key

//            String tableName = getTableName();
//            System.out.println("Evaluating PIndexScan for table: " + tableName);
            if (!storage_manager.check_file_exists(tableName)) {
                return null;
            }


            List<Object[]> final_list = new ArrayList<>();

    // Extract column name and value from the filter condition

            // Get the file ID of the table
            int file_id = storage_manager.convert_file_to_id(tableName);

            // get the col_name from the schema of the file
            byte[] schema_data = storage_manager.get_data_block(tableName, 0);

            // get number of columns
            int num_columns = ((schema_data[1]) << 8) | (schema_data[0] & 0xFF);

            // initialize the iterator for schema and type array for columns and also length of columns
            int schema_iterator = 2;
            int[] type_col = new int[num_columns];
            String[] name_col = new String[num_columns];

            // iterate over schema to fill type_col also get num of fixed and num of variable length columns
            int fixed_col = 0;
            int variable_col = 0;

            for(int i = 0; i < num_columns; i++){
                int offs = ((schema_data[schema_iterator+1]) << 8) | (schema_data[schema_iterator] & 0xFF);
                type_col[i] = (schema_data[offs] & 0xFF);
                int len_col = (schema_data[offs+1] & 0xFF);

                byte[] temp = new byte[len_col];
                System.arraycopy(schema_data, offs+2, temp, 0, len_col);

                name_col[i] = new String(temp);

                if(type_col[i] == 0){
                    variable_col++;
                }
                else if(type_col[i] == 1){
                    fixed_col++;
                }
                else if(type_col[i] == 2){
                    fixed_col++;
                }
                else if(type_col[i] == 3){
                    fixed_col++;
                }
                else{
                    fixed_col++;
                }

                schema_iterator += 2;
            }

            int columnName = -1;
            Object value = null;
            String operator = null;
            String real_colname = null;

            if (filter != null && filter instanceof RexCall) {
                RexCall call = (RexCall) filter;
                List<RexNode> operands = call.operands;
                if (operands.size() == 2) {
                    RexNode leftOperand = operands.get(0);
                    RexNode rightOperand = operands.get(1);
                    if (leftOperand instanceof RexInputRef && rightOperand instanceof RexLiteral) {
                        // Extract column name
                        columnName = ((RexInputRef) leftOperand).getIndex();

                        // Extract value
                        RexLiteral literal = (RexLiteral) rightOperand;
                        value = literal.getValue();
                        real_colname = rowType.getFieldList().get(columnName).getName();

                        SqlTypeName typeName = rowType.getFieldList().get(columnName).getType().getSqlTypeName();
                        switch (typeName) {
                            case VARCHAR:
                                value = literal.getValueAs(String.class);
                                break;
                            case INTEGER:
                                value = literal.getValueAs(Integer.class);
                                break;
                            case BOOLEAN:
                                value = literal.getValueAs(Boolean.class);
                                break;
                            case FLOAT:
                                value = literal.getValueAs(Float.class);
                                break;
                            case DOUBLE:
                                value = literal.getValueAs(Double.class);
                                break;
                            case DECIMAL:
                                value = literal.getValueAs(Double.class);
                                break;
                            // Add cases for other supported types if needed
                            default:
                                throw new IllegalArgumentException("Unsupported data type");
                                // Handle unsupported types
                        }

                        // Extract operator
                        SqlKind opKind = call.getKind();
                        operator = null;
                        switch (opKind) {
                            case EQUALS:
                                operator = "=";
                                break;
                            case LESS_THAN:
                                operator = "<";
                                break;
                            case GREATER_THAN:
                                operator = ">";
                                break;
                            case LESS_THAN_OR_EQUAL:
                                operator = "<=";
                                break;
                            case GREATER_THAN_OR_EQUAL:
                                operator = ">=";
                                break;
                            // Add cases for other comparison operators if needed
                            default:
                                // Handle unsupported operators
                                break;
                        }
                        // Now you have columnName, value, and operator
                    }
                }
            }

            if (columnName == -1 || value == null) {
                return null;
            }


//            String real_colname = rowType.getFieldList().get(columnName);


            // find the column index which matches the column name
            int idx = -1;
            for(int i = 0; i < name_col.length; i++){
                if(Objects.equals(name_col[i], real_colname)){
                    idx = i;
                    break;
                }
            }
            if(idx == -1){
                throw new IllegalArgumentException("Column name does not match any of the columns");
            }

            if(Objects.equals(operator, "=")){
                // Search for the value in the index
                String index_file_name = tableName + "_" + real_colname + "_index";
                int index_file_id = storage_manager.convert_file_to_id(index_file_name);

                // store blocks in a set
                Set<Integer> set_blocks = new HashSet<>();

                // get the leaf node block
                int block_id = storage_manager.getDb().search_index(index_file_id, value);
                if (block_id == -1) {
                    return final_list;
                }

                // initialize curr leaf id
                int curr_leaf_id = block_id;

                while(curr_leaf_id != 0){
                    // get the leaf block data
                    byte[] curr_leaf_block = storage_manager.get_data_block(index_file_name, curr_leaf_id);

                    // get last offs
                    int last_offs = ((curr_leaf_block[6] << 8)) | (curr_leaf_block[7] & 0xFF);

                    // check if at least one key matches the value
                    // take all that match into set_blocks and move to the right
                    boolean check = true;

                    // initialise start offs
                    int start_offs = 8;

                    while (start_offs < last_offs) {
                        int curr_block = ((curr_leaf_block[start_offs] << 8)) | (curr_leaf_block[start_offs + 1] & 0xFF);
                        start_offs += 2;

                        int curr_len = ((curr_leaf_block[start_offs] << 8)) | (curr_leaf_block[start_offs + 1] & 0xFF);
                        start_offs += 2;

                        byte[] byte_key = new byte[curr_len];
                        System.arraycopy(curr_leaf_block, start_offs, byte_key, 0, curr_len);

                        Object key = converter(byte_key, value);
                        if (comparator(key, value) == 0) {
                            set_blocks.add(curr_block);
                            check = false;
                        }
                        start_offs += curr_len;
                    }
                    if(check){
                        break;
                    }
                    curr_leaf_id = ((curr_leaf_block[4] << 8)) | (curr_leaf_block[5] & 0xFF);
                }

                for (Integer element : set_blocks) {
                    List<Object[]> l = storage_manager.get_records_from_block(tableName, element);
                    for (int i = 0; i < l.size(); i++) {
                        if(l.get(i)[idx] == null) continue;
                        if (comparator(l.get(i)[idx], value) == 0) {
                            final_list.add(l.get(i));
                        }
                    }
                }
                return final_list;

            }

            else if(Objects.equals(operator, "<")){
                // Search for the value in the index
                String index_file_name = tableName + "_" + real_colname + "_index";
                int index_file_id = storage_manager.convert_file_to_id(index_file_name);

                // store blocks in a set
                Set<Integer> set_blocks = new HashSet<>();

                // get the leaf node block
                int block_id = storage_manager.getDb().search_index(index_file_id, value);
                if (block_id == -1) {
                    return final_list;
                }

                // initialize curr leaf id
                int curr_leaf_id = block_id;

                while(curr_leaf_id != 0) {
                    // get the leaf block data
                    byte[] curr_leaf_block = storage_manager.get_data_block(index_file_name, curr_leaf_id);

                    // get last offs
                    int last_offs = ((curr_leaf_block[6] << 8)) | (curr_leaf_block[7] & 0xFF);

                    // initialise start offs
                    int start_offs = 8;

                    while (start_offs < last_offs) {
                        int curr_block = ((curr_leaf_block[start_offs] << 8)) | (curr_leaf_block[start_offs + 1] & 0xFF);
                        start_offs += 2;

                        int curr_len = ((curr_leaf_block[start_offs] << 8)) | (curr_leaf_block[start_offs + 1] & 0xFF);
                        start_offs += 2;

                        byte[] byte_key = new byte[curr_len];
                        System.arraycopy(curr_leaf_block, start_offs, byte_key, 0, curr_len);

                        Object key = converter(byte_key, value);

                        if (comparator(key, value) < 0) {
                            set_blocks.add(curr_block);
                        }
                        start_offs += curr_len;
                    }
                    curr_leaf_id = ((curr_leaf_block[2] << 8)) | (curr_leaf_block[3] & 0xFF);
                }

                for (Integer element : set_blocks) {
                    List<Object[]> l = storage_manager.get_records_from_block(tableName, element);
                    for (int i = 0; i < l.size(); i++) {
                        if(l.get(i)[idx] == null) continue;
                        if (comparator(l.get(i)[idx], value) < 0) {
                            final_list.add(l.get(i));
                        }
                    }
                }
                return final_list;
            }
            else if(Objects.equals(operator, ">")){
                // Search for the value in the index
                String index_file_name = tableName + "_" + real_colname + "_index";
                int index_file_id = storage_manager.convert_file_to_id(index_file_name);

                // store blocks in a set
                Set<Integer> set_blocks = new HashSet<>();

                // get the leaf node block
                int block_id = storage_manager.getDb().search_index(index_file_id, value);
                if (block_id == -1) {
                    return final_list;
                }

                // initialize curr leaf id
                int curr_leaf_id = block_id;

                while(curr_leaf_id != 0){
                    // get the leaf block data
                    byte[] curr_leaf_block = storage_manager.get_data_block(index_file_name, curr_leaf_id);

                    // get last offs
                    int last_offs = ((curr_leaf_block[6] << 8)) | (curr_leaf_block[7] & 0xFF);

                    // initialise start offs
                    int start_offs = 8;

                    while (start_offs < last_offs) {
                        int curr_block = ((curr_leaf_block[start_offs] << 8) & 0xFF) | (curr_leaf_block[start_offs + 1] & 0xFF);
                        start_offs += 2;

                        int curr_len = ((curr_leaf_block[start_offs] << 8) & 0xFF) | (curr_leaf_block[start_offs + 1] & 0xFF);
                        start_offs += 2;

                        byte[] byte_key = new byte[curr_len];
                        System.arraycopy(curr_leaf_block, start_offs, byte_key, 0, curr_len);

                        Object key = converter(byte_key, value);
                        if (comparator(key, value) > 0) {
                            set_blocks.add(curr_block);
                        }
                        start_offs += curr_len;
                    }
                    curr_leaf_id = ((curr_leaf_block[4] << 8)) | (curr_leaf_block[5] & 0xFF);
                }

                for (Integer element : set_blocks) {
                    List<Object[]> l = storage_manager.get_records_from_block(tableName, element);
                    for (int i = 0; i < l.size(); i++) {
                        if(l.get(i)[idx] == null) continue;
                        if (comparator(l.get(i)[idx], value) > 0) {
                            final_list.add(l.get(i));
                        }
                    }
                }
                return final_list;

            }

            else if(Objects.equals(operator, "<=")){
                // Search for the value in the index
                String index_file_name = tableName + "_" + real_colname + "_index";
                int index_file_id = storage_manager.convert_file_to_id(index_file_name);

                // store blocks in a set
                Set<Integer> set_blocks = new HashSet<>();

                // get the leaf node block
                int block_id = storage_manager.getDb().search_index(index_file_id, value);
                if (block_id == -1) {
                    return final_list;
                }

                // initialize curr leaf id
                int curr_leaf_id = block_id;

                while(curr_leaf_id != 0){
                    // get the leaf block data
                    byte[] curr_leaf_block = storage_manager.get_data_block(index_file_name, curr_leaf_id);

                    // get last offs
                    int last_offs = ((curr_leaf_block[6] << 8)) | (curr_leaf_block[7] & 0xFF);

                    // check if at least one key matches the value
                    // take all that match into set_blocks and move to the right
                    boolean check = true;

                    // initialise start offs
                    int start_offs = 8;

                    while (start_offs < last_offs) {
                        int curr_block = ((curr_leaf_block[start_offs] << 8)) | (curr_leaf_block[start_offs + 1] & 0xFF);
                        start_offs += 2;

                        int curr_len = ((curr_leaf_block[start_offs] << 8)) | (curr_leaf_block[start_offs + 1] & 0xFF);
                        start_offs += 2;

                        byte[] byte_key = new byte[curr_len];
                        System.arraycopy(curr_leaf_block, start_offs, byte_key, 0, curr_len);

                        Object key = converter(byte_key, value);
                        if (comparator(key, value) == 0) {
                            set_blocks.add(curr_block);
                            check = false;
                        }
                        start_offs += curr_len;
                    }
                    if(check){
                        break;
                    }

                    curr_leaf_id = ((curr_leaf_block[4] << 8)) | (curr_leaf_block[5] & 0xFF);
                }

//                for (Integer element : set_blocks) {
//                    List<Object[]> l = storage_manager.get_records_from_block(tableName, element);
//                    for (int i = 0; i < l.size(); i++) {
//                        if (comparator(l.get(i)[idx], value) <= 0) {
//                            final_list.add(l.get(i));
//                        }
//                    }
//                }
//                return final_list;


                // Search for the value in the index
//                String index_file_name = tableName + "_" + columnName + "_index";
//                int index_file_id = storage_manager.convert_file_to_id(index_file_name);

                // store blocks in a set
//                Set<Integer> set_blocks = new HashSet<>();

                // get the leaf node block
//                int block_id = storage_manager.getDb().search_index(index_file_id, value);
//                if (block_id != -1) {
//                    return final_list;
//                }

                // initialize curr leaf id
                curr_leaf_id = block_id;

                while(curr_leaf_id != 0) {
                    // get the leaf block data
                    byte[] curr_leaf_block = storage_manager.get_data_block(index_file_name, curr_leaf_id);

                    // get last offs
                    int last_offs = ((curr_leaf_block[6] << 8)) | (curr_leaf_block[7] & 0xFF);

                    // initialise start offs
                    int start_offs = 8;

                    while (start_offs < last_offs) {
                        int curr_block = ((curr_leaf_block[start_offs] << 8)) | (curr_leaf_block[start_offs + 1] & 0xFF);
                        start_offs += 2;

                        int curr_len = ((curr_leaf_block[start_offs] << 8)) | (curr_leaf_block[start_offs + 1] & 0xFF);
                        start_offs += 2;

                        byte[] byte_key = new byte[curr_len];
                        System.arraycopy(curr_leaf_block, start_offs, byte_key, 0, curr_len);

                        Object key = converter(byte_key, value);
                        if (comparator(key, value) <= 0) {
                            set_blocks.add(curr_block);
                        }
                        start_offs += curr_len;
                    }
                    curr_leaf_id = ((curr_leaf_block[2] << 8)) | (curr_leaf_block[3] & 0xFF);
                }

                for (Integer element : set_blocks) {
                    List<Object[]> l = storage_manager.get_records_from_block(tableName, element);
                    for (int i = 0; i < l.size(); i++) {
                        if(l.get(i)[idx] == null) continue;
                        if (comparator(l.get(i)[idx], value) <= 0) {
                            final_list.add(l.get(i));
                        }
                    }
                }
                return final_list;
            }

            else if(Objects.equals(operator, ">=")){
                // Search for the value in the index
                String index_file_name = tableName + "_" + real_colname + "_index";
                int index_file_id = storage_manager.convert_file_to_id(index_file_name);

                // store blocks in a set
                Set<Integer> set_blocks = new HashSet<>();

                // get the leaf node block
                int block_id = storage_manager.getDb().search_index(index_file_id, value);
                if (block_id == -1) {
                    return final_list;
                }

                // initialize curr leaf id
                int curr_leaf_id = block_id;

                while(curr_leaf_id != 0){
                    // get the leaf block data
                    byte[] curr_leaf_block = storage_manager.get_data_block(index_file_name, curr_leaf_id);

                    // get last offs
                    int last_offs = ((curr_leaf_block[6] << 8)) | (curr_leaf_block[7] & 0xFF);

                    // initialise start offs
                    int start_offs = 8;

                    while (start_offs < last_offs) {
                        int curr_block = ((curr_leaf_block[start_offs] << 8)) | (curr_leaf_block[start_offs + 1] & 0xFF);
                        start_offs += 2;

                        int curr_len = ((curr_leaf_block[start_offs] << 8)) | (curr_leaf_block[start_offs + 1] & 0xFF);
                        start_offs += 2;

                        byte[] byte_key = new byte[curr_len];
                        System.arraycopy(curr_leaf_block, start_offs, byte_key, 0, curr_len);

                        Object key = converter(byte_key, value);
                        if (comparator(key, value) >= 0) {
                            set_blocks.add(curr_block);
                        }
                        start_offs += curr_len;
                    }
                    curr_leaf_id = ((curr_leaf_block[4] << 8)) | (curr_leaf_block[5] & 0xFF);
                }

                for (Integer element : set_blocks) {
                    List<Object[]> l = storage_manager.get_records_from_block(tableName, element);
                    for (int i = 0; i < l.size(); i++) {
                        if(l.get(i)[idx] == null) continue;
                        if (comparator(l.get(i)[idx], value) >= 0) {
                            final_list.add(l.get(i));
                        }
                    }
                }
                return final_list;

            }
            else{
                return null;
            }


            // If block_id is -1, key not found
//            if (block_id != -1) {
//                // Retrieve the block using block_id
//                AbstractBlock block = storage_manager.get_block(tableName, block_id);
//                if (block != null) {
//                    // Return the list of entries in the block
//                    return block.getEntries();
//                }
//            }

            // Return null if the value is not found or if there's an error
//            return null;
        }
}
