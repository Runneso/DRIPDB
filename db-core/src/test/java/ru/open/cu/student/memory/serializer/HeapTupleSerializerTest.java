package ru.open.cu.student.memory.serializer;

import org.junit.jupiter.api.Test;
import ru.open.cu.student.memory.model.DataType;
import ru.open.cu.student.memory.model.HeapTuple;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class HeapTupleSerializerTest {

    private final HeapTupleSerializer serializer = new HeapTupleSerializer();

    @Test
    void long_roundTrip_extremes() {
        long[] values = {0L, 1L, -1L, Long.MAX_VALUE, Long.MIN_VALUE, 42L, -42L};
        for (long v : values) {
            HeapTuple t = serializer.serialize(v, DataType.INT64);
            assertEquals(DataType.INT64, t.type());
            assertEquals(8, t.data().length);

            Long restored = serializer.deserialize(t);
            assertEquals(v, restored);
        }
    }

    @Test
    void varchar_roundTrip_basic_and_utf8() {
        String[] values = {"", "a", "hello", "Привет", "世界", "emoji🙂"};
        for (String s : values) {
            HeapTuple t = serializer.serialize(s, DataType.VARCHAR);
            assertEquals(DataType.VARCHAR, t.type());

            String restored = serializer.deserialize(t);
            assertEquals(s, restored);
        }
    }

    @Test
    void varchar_255_bytes_boundary() {
        String s255 = "a".repeat(255);
        HeapTuple t = serializer.serialize(s255, DataType.VARCHAR);
        assertEquals(1 + 255, t.data().length);
        assertEquals(255, t.data()[0] & 0xFF);

        String restored = serializer.deserialize(t);
        assertEquals(s255, restored);
    }

    @Test
    void varchar_256_bytes_throws() {
        String s256 = "a".repeat(256);
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> serializer.serialize(s256, DataType.VARCHAR)
        );
        assertTrue(ex.getMessage().contains("exceeds 255"));
    }

    @Test
    void varchar_multibyte_just_under_limit_ok() {
        String s63Emojis = "🙂".repeat(63);
        HeapTuple t = serializer.serialize(s63Emojis, DataType.VARCHAR);
        String restored = serializer.deserialize(t);
        assertEquals(s63Emojis, restored);
        assertEquals(1 + 252, t.data().length);
        assertEquals(252, t.data()[0] & 0xFF);
    }

    @Test
    void varchar_multibyte_over_limit_throws() {
        String s64Emojis = "🙂".repeat(64);
        assertThrows(IllegalArgumentException.class,
                () -> serializer.serialize(s64Emojis, DataType.VARCHAR));
    }

    @Test
    void varchar_nonString_value_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> serializer.serialize(123, DataType.VARCHAR));
    }

    @Test
    void int64_accepts_Integer_Number() {
        HeapTuple t = serializer.serialize(Integer.valueOf(5), DataType.INT64);
        Long restored = serializer.deserialize(t);
        assertEquals(5L, restored);
    }

    @Test
    void int64_wrong_value_type_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> serializer.serialize("42", DataType.INT64));
    }

    @Test
    void int64_bigEndian_layout() {
        long v = 0x0102030405060708L;
        HeapTuple t = serializer.serialize(v, DataType.INT64);
        byte[] bytes = t.data();
        assertEquals(8, bytes.length);
        byte[] expected = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
        assertArrayEquals(expected, bytes);
        Long back = serializer.deserialize(t);
        assertEquals(v, back);
    }

    @Test
    void varchar_empty_payload_throws() {
        HeapTuple bad = new HeapTuple(new byte[0], DataType.VARCHAR);
        assertThrows(IllegalArgumentException.class, () -> serializer.deserialize(bad));
    }

    @Test
    void varchar_length_mismatch_shorter_throws() {
        byte[] payload = new byte[1 + 5];
        payload[0] = 10;
        HeapTuple bad = new HeapTuple(payload, DataType.VARCHAR);
        assertThrows(IllegalArgumentException.class, () -> serializer.deserialize(bad));
    }

    @Test
    void varchar_length_mismatch_longer_throws() {
        byte[] utf8 = "hi".getBytes(StandardCharsets.UTF_8);
        byte[] payload = new byte[1 + utf8.length + 3];
        payload[0] = 2;
        System.arraycopy(utf8, 0, payload, 1, utf8.length);
        Arrays.fill(payload, 1 + utf8.length, payload.length, (byte) 0x7F);
        HeapTuple bad = new HeapTuple(payload, DataType.VARCHAR);
        assertThrows(IllegalArgumentException.class, () -> serializer.deserialize(bad));
    }

    @Test
    void int64_wrong_length_throws() {
        HeapTuple bad1 = new HeapTuple(new byte[7], DataType.INT64);
        HeapTuple bad2 = new HeapTuple(new byte[9], DataType.INT64);
        assertThrows(IllegalArgumentException.class, () -> serializer.deserialize(bad1));
        assertThrows(IllegalArgumentException.class, () -> serializer.deserialize(bad2));
    }

    @Test
    void serialize_nulls_throw() {
        assertThrows(IllegalArgumentException.class, () -> serializer.serialize(1L, null));
        assertThrows(IllegalArgumentException.class, () -> serializer.serialize(null, DataType.INT64));
    }

    @Test
    void deserialize_nulls_throw() {
        assertThrows(IllegalArgumentException.class, () -> serializer.deserialize(null));
        HeapTuple withNullData = new HeapTuple(null, DataType.INT64);
        assertThrows(IllegalArgumentException.class, () -> serializer.deserialize(withNullData));
    }
}


