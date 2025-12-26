package ru.open.cu.student.storage;

import ru.open.cu.student.memory.model.DataType;
import ru.open.cu.student.memory.model.HeapTuple;
import ru.open.cu.student.memory.serializer.TupleSerializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


public final class RowCodec {
    private static final int UINT16_MAX = 0xFFFF;

    private RowCodec() {
    }

    public static byte[] encodeRow(List<Object> values, List<DataType> types, TupleSerializer serializer) {
        Objects.requireNonNull(values, "values");
        Objects.requireNonNull(types, "types");
        Objects.requireNonNull(serializer, "serializer");
        if (values.size() != types.size()) {
            throw new IllegalArgumentException("values/types size mismatch");
        }
        if (values.size() > UINT16_MAX) {
            throw new IllegalArgumentException("too many columns: " + values.size());
        }

        byte[][] fields = new byte[values.size()][];
        int totalBytes = Short.BYTES; 

        for (int i = 0; i < values.size(); i++) {
            HeapTuple tuple = serializer.serialize(values.get(i), types.get(i));
            byte[] field = tuple.data();
            if (field.length > UINT16_MAX) {
                throw new IllegalArgumentException("field too large: " + field.length);
            }
            fields[i] = field;
            totalBytes += Short.BYTES + field.length;
        }

        ByteBuffer buf = ByteBuffer.allocate(totalBytes);
        buf.putShort((short) values.size());
        for (byte[] field : fields) {
            buf.putShort((short) field.length);
            buf.put(field);
        }
        return buf.array();
    }

    public static List<Object> decodeRow(byte[] rowBytes, List<DataType> types, TupleSerializer serializer) {
        Objects.requireNonNull(rowBytes, "rowBytes");
        Objects.requireNonNull(types, "types");
        Objects.requireNonNull(serializer, "serializer");

        ByteBuffer buf = ByteBuffer.wrap(rowBytes);
        int colCount = Short.toUnsignedInt(buf.getShort());
        if (colCount != types.size()) {
            throw new IllegalArgumentException("row columnCount mismatch: row=" + colCount + " schema=" + types.size());
        }

        List<Object> row = new ArrayList<>(colCount);
        for (int i = 0; i < colCount; i++) {
            int len = Short.toUnsignedInt(buf.getShort());
            byte[] fieldBytes = new byte[len];
            buf.get(fieldBytes);
            row.add(serializer.deserialize(new HeapTuple(fieldBytes, types.get(i))));
        }
        return row;
    }
}


