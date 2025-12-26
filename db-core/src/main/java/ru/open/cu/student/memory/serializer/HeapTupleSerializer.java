package ru.open.cu.student.memory.serializer;

import ru.open.cu.student.memory.model.DataType;
import ru.open.cu.student.memory.model.HeapTuple;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;


public final class HeapTupleSerializer implements TupleSerializer {
    private static final ByteOrder ORDER = ByteOrder.BIG_ENDIAN;

    @Override
    public <T> HeapTuple serialize(T value, DataType type) {
        if (type == null) throw new IllegalArgumentException("type is null");
        if (value == null) throw new IllegalArgumentException("value is null");

        return switch (type) {
            case INT64 -> {
                if (!(value instanceof Number n)) {
                    throw new IllegalArgumentException("INT64 expects Number (e.g., Long)");
                }
                byte[] data = new byte[Long.BYTES];
                ByteBuffer.wrap(data).order(ORDER).putLong(n.longValue());
                yield new HeapTuple(data, DataType.INT64);
            }
            case VARCHAR -> {
                if (!(value instanceof String s)) {
                    throw new IllegalArgumentException("VARCHAR expects String");
                }
                byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
                if (utf8.length > 255) {
                    throw new IllegalArgumentException("VARCHAR exceeds 255 bytes in UTF-8: " + utf8.length);
                }
                byte[] out = new byte[1 + utf8.length];
                out[0] = (byte) (utf8.length & 0xFF);
                System.arraycopy(utf8, 0, out, 1, utf8.length);
                yield new HeapTuple(out, DataType.VARCHAR);
            }
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T deserialize(HeapTuple tuple) {
        if (tuple == null) throw new IllegalArgumentException("tuple is null");
        byte[] data = tuple.data();
        if (data == null) throw new IllegalArgumentException("tuple.data is null");
        DataType type = tuple.type();
        if (type == null) throw new IllegalArgumentException("tuple.type is null");

        return switch (type) {
            case INT64 -> {
                if (data.length != Long.BYTES) {
                    throw new IllegalArgumentException("INT64 must be exactly 8 bytes, got " + data.length);
                }
                long v = ByteBuffer.wrap(data).order(ORDER).getLong();
                yield (T) Long.valueOf(v);
            }
            case VARCHAR -> {
                if (data.length == 0) {
                    throw new IllegalArgumentException("VARCHAR payload is empty, missing length byte");
                }
                int len = Byte.toUnsignedInt(data[0]);
                if (data.length != 1 + len) {
                    throw new IllegalArgumentException(
                            "VARCHAR length mismatch: header=" + len + ", bytes=" + (data.length - 1)
                    );
                }
                yield (T) new String(data, 1, len, StandardCharsets.UTF_8);
            }
        };
    }
}


