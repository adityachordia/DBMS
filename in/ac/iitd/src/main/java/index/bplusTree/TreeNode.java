package index.bplusTree;

// TreeNode interface - will be implemented by InternalNode and LeafNode
public interface TreeNode <T> {

    public T[] getKeys();
    public void insert(T key, int block_id);

    public int search(T key);

    // DO NOT modify this - may be used for evaluation
    default public void print() {
        T[] keys = getKeys();
        for (T key : keys) {
            System.out.print(key + " ");
        }
        return;
    }
    
    // Might be useful for you - will not be evaluated
    default public T convertBytesToT(byte[] bytes, Class<T> typeClass){
        
        /* Write your code here */

        if (typeClass == Integer.class) {
            return convertBytesToInteger(bytes, typeClass);
        } else if (typeClass == String.class) {
            return typeClass.cast(new String(bytes));
        } else if (typeClass == Boolean.class) {
            return convertBytesToBoolean(bytes, typeClass);
        } else if (typeClass == Float.class) {
            return convertBytesToFloat(bytes, typeClass);
        } else if (typeClass == Double.class) {
            return convertBytesToDouble(bytes, typeClass);
        } else {
            throw new UnsupportedOperationException("Conversion for the specified type is not supported");
        }
    }

    default public T convertBytesToInteger(byte[] bytes, Class<T> typeClass) {
        validateBArrayLength(bytes, 4);
        return typeClass.cast((bytes[0] << 24) | ((bytes[1] & 0xFF) << 16) | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF));
    }

    default public T convertBytesToBoolean(byte[] bytes, Class<T> typeClass) {
        validateBArrayLength(bytes, 1);
        return typeClass.cast(bytes[0] != 0);
    }

    default public T convertBytesToFloat(byte[] bytes, Class<T> typeClass) {
        validateBArrayLength(bytes, 4);
        return typeClass.cast(Float.intBitsToFloat(byteArrayToFloat(bytes)));
    }

    default public T convertBytesToDouble(byte[] bytes, Class<T> typeClass) {
        validateBArrayLength(bytes, 8);
        return typeClass.cast(Double.longBitsToDouble(byteArrayToLong(bytes)));
    }

    default public void validateBArrayLength(byte[] bytes, int expectedLength) {
        if (bytes.length != expectedLength) {
            throw new IllegalArgumentException("Byte array length does not match the expected length for conversion");
        }
    }

    default public int byteArrayToFloat(byte[] bytes) {
        return (bytes[0] << 24) | ((bytes[1] & 0xFF) << 16) | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
    }

    default public long byteArrayToLong(byte[] bytes) {
        long value = 0;
        for (int i = 0; i < bytes.length; i++) {
            value |= ((long) (bytes[i] & 0xFF)) << (8 * (bytes.length-1-i)); //Doubt
        }
        return value;
    }

    default public int comparator(T key1, T key2) {
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

    default public byte[] convertToByteArray(Object value) {
        if (value instanceof String) {
            return convertStringToBytes((String) value);
        }
        else if (value instanceof Integer) {
            return convertIntToBytes((Integer) value);
        }
        else if (value instanceof Float) {
            return convertFloatToBytes((Float) value);
        }
        else if (value instanceof Double) {
            return convertDoubleToBytes((Double) value);
        }
        else if (value instanceof Boolean) {
            return convertBooleanToBytes((Boolean) value);
        }
        else {
            throw new IllegalArgumentException("Unsupported data type");
        }
    }

    default public byte[] convertStringToBytes(String value) {
        char[] chars = value.toCharArray();
        byte[] bytes = new byte[chars.length];
        for (int i = 0; i < chars.length; i++) {
            bytes[i] = (byte) chars[i];
        }
        return bytes;
    }

    default public byte[] convertIntToBytes(int value) {
        return new byte[] {
                (byte) (value >> 24),
                (byte) (value >> 16),
                (byte) (value >> 8),
                (byte) value
        };
    }

    default public byte[] convertFloatToBytes(float value) {
        return convertIntToBytes(Float.floatToIntBits(value));
    }

    default public byte[] convertDoubleToBytes(double value) {
        long longValue = Double.doubleToRawLongBits(value);
        return new byte[] {
                (byte) (longValue >> 56),
                (byte) (longValue >> 48),
                (byte) (longValue >> 40),
                (byte) (longValue >> 32),
                (byte) (longValue >> 24),
                (byte) (longValue >> 16),
                (byte) (longValue >> 8),
                (byte) longValue
        };
    }

    default public byte[] convertBooleanToBytes(boolean value) {
        return new byte[] { (byte) (value ? 1 : 0) };
    }
}

