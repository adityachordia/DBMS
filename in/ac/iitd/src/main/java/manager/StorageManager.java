package manager;

import java.lang.Boolean;
import index.bplusTree.BPlusTreeIndexFile;
import storage.DB;
import storage.File;
import storage.Block;
import Utils.CsvRowConverter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.Sources;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class StorageManager {

    private HashMap<String, Integer> file_to_fileid;
    private DB db;

    enum ColumnType {
        VARCHAR, INTEGER, BOOLEAN, FLOAT, DOUBLE
    };

    public StorageManager() {
        file_to_fileid = new HashMap<>();
        db = new DB();
    }

    // loads CSV files into DB362
    public void loadFile(String csvFile, List<RelDataType> typeList) {

        System.out.println("Loading file: " + csvFile);

        String table_name = csvFile;

        if(csvFile.endsWith(".csv")) {
            table_name = table_name.substring(0, table_name.length() - 4);
        }

        // check if file already exists
        assert(file_to_fileid.get(table_name) == null);

        File f = new File();
        try{
            csvFile = getFsPath() + "/" + csvFile;
            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            String line = "";
            int lineNum = 0;

            while ((line = br.readLine()) != null) {

                // csv header line
                if(lineNum == 0){

                    String[] columnNames = CsvRowConverter.parseLine(line);
                    List<String> columnNamesList = new ArrayList<>();

                    for(String columnName : columnNames) {
                        // if columnName contains ":", then take part before ":"
                        String c = columnName;
                        if(c.contains(":")) {
                            c = c.split(":")[0];
                        }
                        columnNamesList.add(c);
                    }

                    Block schemaBlock = createSchemaBlock(columnNamesList, typeList);
                    f.add_block(schemaBlock);
                    lineNum++;
                    continue;
                }

                String[] parsedLine = CsvRowConverter.parseLine(line);
                Object[] row = new Object[parsedLine.length];

                for(int i = 0; i < parsedLine.length; i++) {
                    row[i] = CsvRowConverter.convert(typeList.get(i), parsedLine[i]);
                }

                // convert row to byte array
                byte[] record = convertToByteArray(row, typeList);

                boolean added = f.add_record_to_last_block(record);
                if(!added) {
                    f.add_record_to_new_block(record);
                }
                lineNum++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        System.out.println("Done writing file\n");
        int counter = db.addFile(f);
        file_to_fileid.put(table_name, counter);
        return;
    }

    // converts a row to byte array to write to relational file
    private byte[] convertToByteArray(Object[] row, List<RelDataType> typeList) {

        List<Byte> fixed_length_Bytes = new ArrayList<>();
        List<Byte> variable_length_Bytes = new ArrayList<>();
        List<Integer> variable_length = new ArrayList<>();
        List<Boolean> fixed_length_nullBitmap = new ArrayList<>();
        List<Boolean> variable_length_nullBitmap = new ArrayList<>();

        for(int i = 0; i < row.length; i++) {

            if(typeList.get(i).getSqlTypeName().getName().equals("INTEGER")) {
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    int val = (int) row[i];
                    byte[] intBytes = new byte[4];
                    intBytes[0] = (byte) (val & 0xFF);
                    intBytes[1] = (byte) ((val >> 8) & 0xFF);
                    intBytes[2] = (byte) ((val >> 16) & 0xFF);
                    intBytes[3] = (byte) ((val >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(intBytes[j]);
                    }
                }
            } else if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                if(row[i] == null){
                    variable_length_nullBitmap.add(true);
                    for(int j = 0; j < 1; j++) {
                        variable_length_Bytes.add((byte) 0);
                    }
                } else {
                    variable_length_nullBitmap.add(false);
                    String val = (String) row[i];
                    byte[] strBytes = val.getBytes();
                    for(int j = 0; j < strBytes.length; j++) {
                        variable_length_Bytes.add(strBytes[j]);
                    }
                    variable_length.add(strBytes.length);
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("BOOLEAN")) {         
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    fixed_length_Bytes.add((byte) 0);
                } else {
                    fixed_length_nullBitmap.add(false);
                    boolean val = (boolean) row[i];
                    fixed_length_Bytes.add((byte) (val ? 1 : 0));
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("FLOAT")) {
                
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    float val = (float) row[i];
                    byte[] floatBytes = new byte[4];
                    int intBits = Float.floatToIntBits(val);
                    floatBytes[0] = (byte) (intBits & 0xFF);
                    floatBytes[1] = (byte) ((intBits >> 8) & 0xFF);
                    floatBytes[2] = (byte) ((intBits >> 16) & 0xFF);
                    floatBytes[3] = (byte) ((intBits >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(floatBytes[j]);
                    }
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("DOUBLE")) {
                
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    double val = (double) row[i];
                    byte[] doubleBytes = new byte[8];
                    long longBits = Double.doubleToLongBits(val);
                    doubleBytes[0] = (byte) (longBits & 0xFF);
                    doubleBytes[1] = (byte) ((longBits >> 8) & 0xFF);
                    doubleBytes[2] = (byte) ((longBits >> 16) & 0xFF);
                    doubleBytes[3] = (byte) ((longBits >> 24) & 0xFF);
                    doubleBytes[4] = (byte) ((longBits >> 32) & 0xFF);
                    doubleBytes[5] = (byte) ((longBits >> 40) & 0xFF);
                    doubleBytes[6] = (byte) ((longBits >> 48) & 0xFF);
                    doubleBytes[7] = (byte) ((longBits >> 56) & 0xFF);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add(doubleBytes[j]);
                    }
                }
            } else {
                System.out.println("Unsupported type");
                throw new RuntimeException("Unsupported type");
            }
        }

        short num_bytes_for_bitmap = (short) ((fixed_length_nullBitmap.size() + variable_length_nullBitmap.size() + 7) / 8); // should be in multiples of bytes

        //                       bytes for fixed length and variable length fields          offset & length of var fields
        byte[] result = new byte[fixed_length_Bytes.size() + variable_length_Bytes.size() + 4 * variable_length.size() + num_bytes_for_bitmap];
        int variable_length_offset = 4 * variable_length.size() + fixed_length_Bytes.size() + num_bytes_for_bitmap;

        int idx = 0;
        for(; idx < variable_length.size() ; idx ++){
            // first 2 bytes should be offset
            result[idx * 4] = (byte) (variable_length_offset & 0xFF);
            result[idx * 4 + 1] = (byte) ((variable_length_offset >> 8) & 0xFF);

            // next 2 bytes should be length
            result[idx * 4 + 2] = (byte) (variable_length.get(idx) & 0xFF);
            result[idx * 4 + 3] = (byte) ((variable_length.get(idx) >> 8) & 0xFF);

            variable_length_offset += variable_length.get(idx);
        }

        idx = idx * 4;

        // write fixed length fields
        for(int i = 0; i < fixed_length_Bytes.size(); i++, idx++) {
            result[idx] = fixed_length_Bytes.get(i);
        }

        // write null bitmap
        int bitmap_idx = 0;
        for(int i = 0; i < fixed_length_nullBitmap.size(); i++) {
            if(fixed_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }
        for(int i = 0; i < variable_length_nullBitmap.size(); i++) {
            if(variable_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }

        if(bitmap_idx != 0) {
            idx++;
        }

        // write variable length fields
        for(int i = 0; i < variable_length_Bytes.size(); i++, idx++) {
            result[idx] = variable_length_Bytes.get(i);
        }

        return result;
    }

    // helper function for loadFile
    private String getFsPath() throws IOException, ParseException {

        String modelPath = Sources.of(CsvRowConverter.class.getResource("/" + "model.json")).file().getAbsolutePath();
        JSONObject json = (JSONObject) new JSONParser().parse(new FileReader(modelPath));
        JSONArray schemas = (JSONArray) json.get("schemas");

        Iterator itr = schemas.iterator();

        while (itr.hasNext()) {
            JSONObject next = (JSONObject) itr.next();
            if (next.get("name").equals("FILM_DB")) {
                JSONObject operand = (JSONObject) next.get("operand");
                String directory = operand.get("directory").toString();
                return Sources.of(CsvRowConverter.class.getResource("/" + directory)).file().getAbsolutePath();
            }
        }
        return null;
    }

    // write schema block for a relational file
    private Block createSchemaBlock(List<String> columnNames, List<RelDataType> typeList) {

        Block schema = new Block();

        // write number of columns
        byte[] num_columns = new byte[2];
        num_columns[0] = (byte) (columnNames.size() & 0xFF);
        num_columns[1] = (byte) ((columnNames.size() >> 8) & 0xFF);

        schema.write_data(0, num_columns);

        int idx = 0, curr_offset = schema.get_block_capacity();
        for(int i = 0 ; i < columnNames.size() ; i ++){
            // if column type is fixed, then write it
            if(!typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                
                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF);
                schema.write_data(2 + 2 * idx, offset);
                
                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        // write variable length fields
        for(int i = 0; i < columnNames.size(); i++) {
            if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                
                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF); 
                // IMPORTANT: Take care of endianness
                schema.write_data(2 + 2 * idx, offset);
                
                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        return schema;
    }

    public int convert_file_to_id(String table_name){
        return file_to_fileid.get(table_name);
    }

    // should only read one block at a time
    public byte[] get_data_block(String table_name, int block_id){
        int file_id = file_to_fileid.get(table_name);
        return db.get_data(file_id, block_id);
    }

    public boolean check_file_exists(String table_name) {
        return file_to_fileid.get(table_name) != null;
    }

    public boolean check_index_exists(String table_name, String column_name) {
        String index_file_name = table_name + "_" + column_name + "_index";
        return file_to_fileid.get(index_file_name) != null;
    }

    // the order of returned columns should be same as the order in schema
    // i.e., first all fixed length columns, then all variable length columns
    public List<Object[]> get_records_from_block(String table_name, int block_id){
        /* Write your code here */
        // return null if file does not exist, or block_id is invalid
        // return list of records otherwise
        if(!check_file_exists(table_name) || get_data_block(table_name, block_id) == null){
            return null;
        }

        // get the schema block as byte array
        byte[] schema = get_data_block(table_name, 0);

        // get the number of columns from schema
        int num_columns = ((schema[1] & 0xFF) << 8) | (schema[0] & 0xFF);

        // initialize the iterator for schema and type array for columns
        int schema_iterator = 2;
        int[] type_col = new int[num_columns];

        // iterate over schema to fill type_col also get num of fixed and num of variable length columns
        int fixed_col = 0;
        int variable_col = 0;

        for(int i = 0; i < num_columns; i++){
            int offs = ((schema[schema_iterator+1] & 0xFF) << 8) | (schema[schema_iterator] & 0xFF);
            type_col[i] = (schema[offs] & 0xFF);

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

        // get the block data now
        byte[] block_data = get_data_block(table_name, block_id);

        // initialize an empty records list
        List<Object[]> records = new ArrayList<>();

        // get num of records
        int num_records = ((block_data[0] & 0xFF) << 8) | (block_data[1] & 0xFF);

        // initialize block iterator
        int block_iterator = 2;

        for(int i = 0; i < num_records; i++){
            // get the record iterator
            int record_iterator = ((block_data[block_iterator] & 0xFF) << 8) | (block_data[block_iterator+1] & 0xFF);
            block_iterator += 2;
            int base_record = record_iterator;
            // initialize the object array for storing the record data
            Object[] instance = new Object[fixed_col+variable_col];

            // initialize array for storing offset and length of each variable length column
            int[] var_offs = new int[variable_col];
            int[] var_lens = new int[variable_col];

            // fill the above tables
            for(int j = 0; j < variable_col; j++){
                var_offs[j] = ((block_data[record_iterator+1] & 0xFF) << 8) | (block_data[record_iterator] & 0xFF) + base_record;
                record_iterator += 2;

                var_lens[j] = ((block_data[record_iterator+1] & 0xFF) << 8) | (block_data[record_iterator] & 0xFF);
                record_iterator += 2;
            }

            // get the fixed columns
            for(int j = 0; j < fixed_col; j++){
                if(type_col[j] == 1){
                    instance[j] =
                            ((block_data[record_iterator+3] & 0xFF) << 24) |
                            ((block_data[record_iterator+2] & 0xFF) << 16) |
                            ((block_data[record_iterator+1] & 0xFF) << 8) |
                            (block_data[record_iterator] & 0xFF);

                    record_iterator += 4;
                }
                else if(type_col[j] == 2){
                    instance[j] = (block_data[record_iterator] & 0xFF);
                    record_iterator += 1;
                }
                else if(type_col[j] == 3){
                    instance[j] = Float.intBitsToFloat(
                            ((block_data[record_iterator+3] & 0xFF) << 24) |
                            ((block_data[record_iterator+2] & 0xFF) << 16) |
                            ((block_data[record_iterator+1] & 0xFF) << 8) |
                            (block_data[record_iterator] & 0xFF));

                    record_iterator += 4;
                }
                else if(type_col[j] == 4){
                    instance[j] = Double.longBitsToDouble(
                            ((long)(block_data[record_iterator+7] & 0xFF) << 56) |
                            ((long)(block_data[record_iterator+6] & 0xFF) << 48) |
                            ((long)(block_data[record_iterator+5] & 0xFF) << 40) |
                            ((long)(block_data[record_iterator+4] & 0xFF) << 32) |
                            ((long)(block_data[record_iterator+3] & 0xFF) << 24) |
                            ((long)(block_data[record_iterator+2] & 0xFF) << 16) |
                            ((long)(block_data[record_iterator+1] & 0xFF) << 8) |
                            (long)(block_data[record_iterator] & 0xFF));

                    record_iterator += 8;
                }
                else{
                    throw new IllegalArgumentException("How did this happen");
                }
            }

            // get the null bitmap
            boolean[] null_bit_map = new boolean[num_columns];
            int bit_map_index = 0;

            for(int j=0; j<num_columns; j++){
                if((block_data[record_iterator] & (1 << (7 - bit_map_index))) != 0){
                    null_bit_map[j] = true;
                }
                bit_map_index++;
                if(bit_map_index == 8 && j != num_columns-1){
                    bit_map_index = 0;
                    record_iterator++;
                }
            }

//            // reset the record iterator to the first
//            if(variable_col > 0) record_iterator = va1r_offs[0];

            // now fill in the variable length column
            for(int j = fixed_col; j < fixed_col+variable_col; j++){
                // get the offset to and length of the variable columns
                int offs = var_offs[j-fixed_col];
                int lens = var_lens[j-fixed_col];

                byte[] temp = new byte[lens];
                System.arraycopy(block_data, offs, temp, 0, lens);

                instance[j] = new String(temp);
            }

            for(int z = 0; z < instance.length; z++){
                if(null_bit_map[z]){
                    instance[z] = null;
                }
            }
            records.add(instance);
        }
        return records;
    }

    public boolean create_index(String table_name, String column_name, int order) {
        /* Write your code here */

        // get the schema block as byte array
        byte[] schema = get_data_block(table_name, 0);

        // get the number of columns from schema
        int num_columns = ((schema[1] & 0xFF) << 8) | (schema[0] & 0xFF);

        // initialize the iterator for schema and type array for columns and also length of columns
        int schema_iterator = 2;
        int[] type_col = new int[num_columns];
        String[] name_col = new String[num_columns];

        // iterate over schema to fill type_col also get num of fixed and num of variable length columns
        int fixed_col = 0;
        int variable_col = 0;

        for(int i = 0; i < num_columns; i++){
            int offs = ((schema[schema_iterator+1] & 0xFF) << 8) | (schema[schema_iterator] & 0xFF);
            type_col[i] = (schema[offs] & 0xFF);
            int len_col = (schema[offs+1] & 0xFF);

            byte[] temp = new byte[len_col];
            System.arraycopy(schema, offs+2, temp, 0, len_col);

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

        // find the column index which matches the column name
        int idx = -1;
        for(int i = 0; i < name_col.length; i++){
            if(Objects.equals(name_col[i], column_name)){
                idx = i;
                break;
            }
        }
        if(idx == -1){
            throw new IllegalArgumentException("Column name does not match any of the columns");
        }
        int bl_id;
        String index_file_name = table_name + "_" + column_name + "_index";

        switch (type_col[idx]) {
            case 0:
                BPlusTreeIndexFile<String> bPT1 = new BPlusTreeIndexFile<>(order, String.class);

                bl_id = 1;
                while(get_data_block(table_name, bl_id) != null){
                    List<Object[]> list_records = get_records_from_block(table_name, bl_id);
                    for(int i = 0; i < list_records.size(); i++){
                        if((list_records.get(i))[idx] != null){
                            bPT1.insert((String)((list_records.get(i))[idx]), bl_id);
                        }
                    }
                    bl_id++;
                }
                int index1 = this.getDb().addFile(bPT1);
                file_to_fileid.put(index_file_name, index1);

                break;
            case 1:
                BPlusTreeIndexFile<Integer> bPT2 = new BPlusTreeIndexFile<>(order, Integer.class);

                bl_id = 1;
                while(get_data_block(table_name, bl_id) != null){
                    List<Object[]> list_records = get_records_from_block(table_name, bl_id);
                    for(int i = 0; i < list_records.size(); i++){
                        if((list_records.get(i))[idx] != null) {
                            bPT2.insert((Integer) ((list_records.get(i))[idx]), bl_id);
                        }
                    }
                    bl_id++;
                }
                int index2 = this.getDb().addFile(bPT2);
                file_to_fileid.put(index_file_name, index2);

                break;
            case 2:
                BPlusTreeIndexFile<Boolean> bPT3 = new BPlusTreeIndexFile<>(order, Boolean.class);
                bl_id = 1;
                while(get_data_block(table_name, bl_id) != null){
                    List<Object[]> list_records = get_records_from_block(table_name, bl_id);
                    for(int i = 0; i < list_records.size(); i++){
                        if((list_records.get(i))[idx] != null){
                            bPT3.insert((Boolean)((list_records.get(i))[idx]), bl_id);
                        }
                    }
                    bl_id++;
                }
                int index3 = this.getDb().addFile(bPT3);
                file_to_fileid.put(index_file_name, index3);

                break;
            case 3:
                BPlusTreeIndexFile<Float> bPT4 = new BPlusTreeIndexFile<>(order, Float.class);
                bl_id = 1;
                while(get_data_block(table_name, bl_id) != null){
                    List<Object[]> list_records = get_records_from_block(table_name, bl_id);
                    for(int i = 0; i < list_records.size(); i++){
                        if((list_records.get(i))[idx] != null){
                            bPT4.insert((Float) ((list_records.get(i))[idx]), bl_id);
                        }
                    }
                    bl_id++;
                }
                int index4 = this.getDb().addFile(bPT4);
                file_to_fileid.put(index_file_name, index4);

                break;
            case 4:
                BPlusTreeIndexFile<Double> bPT5 = new BPlusTreeIndexFile<>(order, Double.class);
                bl_id = 1;
                while(get_data_block(table_name, bl_id) != null){
                    List<Object[]> list_records = get_records_from_block(table_name, bl_id);
                    for(int i = 0; i < list_records.size(); i++){
                        if((list_records.get(i))[idx] != null){
                            bPT5.insert((Double) ((list_records.get(i))[idx]), bl_id);
                        }
                    }
                    bl_id++;
                }
                int index5 = this.getDb().addFile(bPT5);
                file_to_fileid.put(index_file_name, index5);

                break;
            default:
                throw new IllegalArgumentException("Invalid value for variable");
        }

        return true;
    }

    // returns the block_id of the leaf node where the key is present
    public int search(String table_name, String column_name, RexLiteral value) {
        /* Write your code here */
        if(!check_index_exists(table_name, column_name)){
            return -1;
        }

        String index_file_name = table_name + "_" + column_name + "_index";
        int file_id = file_to_fileid.get(index_file_name);

        return db.search_index(file_id, value);
    }

    public boolean delete(String table_name, String column_name, RexLiteral value) {
        /* Write your code here */
        // Hint: You need to delete from both - the file and the index
        return false;
    }

    // will be used for evaluation - DO NOT modify
    public DB getDb() {
        return db;
    }

    public <T> ArrayList<T> return_bfs_index(String table_name, String column_name) {
        if(check_index_exists(table_name, column_name)) {
            int file_id = file_to_fileid.get(table_name + "_" + column_name + "_index");
            return db.return_bfs_index(file_id);
        } else {
            System.out.println("Index does not exist");
        }
        return null;
    }

}
