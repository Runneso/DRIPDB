package ru.open.cu.student.memory.page;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class HeapPageTest {
    @Test
    void new_page_is_valid_and_fixed_size() {
        Page page = new HeapPage(0);
        assertTrue(page.isValid());
        assertEquals(0, page.size());
        assertEquals(HeapPage.PAGE_SIZE, page.bytes().length);
    }

    @Test
    void write_and_read_multiple_records_in_order() {
        Page page = new HeapPage(5);
        byte[] a = new byte[]{1, 2, 3};
        byte[] b = new byte[]{4};
        byte[] c = new byte[]{10, 11, 12, 13, 14};

        page.write(a);
        page.write(b);
        page.write(c);

        assertEquals(3, page.size());
        assertArrayEquals(a, page.read(0));
        assertArrayEquals(b, page.read(1));
        assertArrayEquals(c, page.read(2));
    }

    @Test
    void fill_until_full_then_throw() {
        Page page = new HeapPage(7);
        int count = 0;
        byte[] one = new byte[]{42};
        try {
            while (true) {
                page.write(one);
                count++;
            }
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().toLowerCase().contains("not enough space"));
        }
        assertTrue(page.size() > 0);
        assertEquals(count, page.size());
    }

    @Test
    void supports_various_record_sizes() {
        Page page = new HeapPage(9);
        byte[] x = new byte[1];
        byte[] y = new byte[128];
        byte[] z = new byte[1024];
        for (int i = 0; i < y.length; i++) y[i] = (byte) i;
        for (int i = 0; i < z.length; i++) z[i] = (byte) (255 - (i % 256));

        page.write(x);
        page.write(y);
        page.write(z);

        assertEquals(3, page.size());
        assertArrayEquals(x, page.read(0));
        assertArrayEquals(y, page.read(1));
        assertArrayEquals(z, page.read(2));
    }

    @Test
    void constructor_null_buffer_throws() {
        assertThrows(IllegalArgumentException.class, () -> new HeapPage(1, null));
    }

    @Test
    void constructor_wrong_buffer_size_throws() {
        byte[] small = new byte[HeapPage.PAGE_SIZE - 1];
        byte[] big = new byte[HeapPage.PAGE_SIZE + 1];
        assertThrows(IllegalArgumentException.class, () -> new HeapPage(2, small));
        assertThrows(IllegalArgumentException.class, () -> new HeapPage(3, big));
    }

    @Test
    void constructor_with_invalid_signature_throws() {
        byte[] buf = new byte[HeapPage.PAGE_SIZE];
        putIntBE(buf, 0, 0xDEADBEEF);
        assertThrows(IllegalArgumentException.class, () -> new HeapPage(4, buf));
    }

    @Test
    void write_null_throws() {
        Page page = new HeapPage(10);
        assertThrows(IllegalArgumentException.class, () -> page.write(null));
    }

    @Test
    void read_index_out_of_bounds_throws() {
        Page page = new HeapPage(11);
        page.write(new byte[]{1, 2, 3});
        assertThrows(IndexOutOfBoundsException.class, () -> page.read(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> page.read(1));
    }

    @Test
    void exact_fit_write_succeeds_then_next_write_fails() {
        HeapPage page = new HeapPage(12);

        byte[] big = new byte[8000];
        Arrays.fill(big, (byte) 7);
        page.write(big);

        int successes = 0;
        byte[] one = new byte[]{1};
        while (true) {
            try {
                page.write(one);
                successes++;
            } catch (IllegalArgumentException ex) {
                assertTrue(ex.getMessage().toLowerCase().contains("not enough space"));
                break;
            }
        }
        assertTrue(successes > 0);
    }

    @Test
    void persists_when_reconstructed_from_bytes() {
        HeapPage original = new HeapPage(13);
        byte[] a = new byte[]{1, 2, 3};
        byte[] b = new byte[]{9, 8};
        original.write(a);
        original.write(b);

        byte[] snapshot = Arrays.copyOf(original.bytes(), original.bytes().length);
        HeapPage reloaded = new HeapPage(13, snapshot);

        assertTrue(reloaded.isValid());
        assertEquals(2, reloaded.size());
        assertArrayEquals(a, reloaded.read(0));
        assertArrayEquals(b, reloaded.read(1));
    }

    @Test
    void read_on_corrupted_slot_bounds_throws() {
        HeapPage page = new HeapPage(14);
        byte[] payload = new byte[]{10, 11, 12, 13};
        page.write(payload);

        byte[] buf = page.bytes();

        final int FIRST_SLOT_POSITION = 10;
        final int UPPER_BOUND_OFF = 8;

        int upper = getUShortBE(buf, UPPER_BOUND_OFF);

        putUShortBE(buf, FIRST_SLOT_POSITION, upper - 1);
        putUShortBE(buf, FIRST_SLOT_POSITION + 2, payload.length);

        assertThrows(IllegalStateException.class, () -> page.read(0));
    }

    @Test
    void operations_on_invalidated_signature_throw() {
        HeapPage page = new HeapPage(15);
        page.write(new byte[]{1, 2});

        putIntBE(page.bytes(), 0, 0xCAFEBABE);

        assertFalse(page.isValid());
        assertThrows(IllegalArgumentException.class, () -> page.write(new byte[]{3}));
        assertThrows(IllegalArgumentException.class, () -> page.read(0));
    }

    private static void putIntBE(byte[] buffer, int position, int value) {
        ByteBuffer.wrap(buffer, position, 4).order(ByteOrder.BIG_ENDIAN).putInt(value);
    }

    private static void putUShortBE(byte[] buffer, int position, int value) {
        ByteBuffer.wrap(buffer, position, 2).order(ByteOrder.BIG_ENDIAN).putShort((short) value);
    }

    private static int getUShortBE(byte[] buffer, int position) {
        return Short.toUnsignedInt(ByteBuffer.wrap(buffer, position, 2).order(ByteOrder.BIG_ENDIAN).getShort());
    }
}


