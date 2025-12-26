package ru.open.cu.student.memory.page;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;


public final class HeapPage implements Page {
    public static final int PAGE_SIZE = 8 * 1024;
    private static final int SIGNATURE = 0x00DBDB01;

    private static final int HEADER_SIZE = 10;
    private static final int SLOT_SIZE = 4;

    private static final int SIGNATURE_OFF = 0;
    private static final int SLOT_COUNT_OFF = 4;
    private static final int LOWER_BOUND_OFF = 6;
    private static final int UPPER_BOUND_OFF = 8;

    private static final int INT_SIZE = 4;
    private static final int SHORT_SIZE = 2;

    private final int pageId;
    private final byte[] buffer;

    public HeapPage(int pageId) {
        this.pageId = pageId;
        this.buffer = new byte[PAGE_SIZE];
        initHeaders();
    }

    public HeapPage(int pageId, byte[] buffer) {
        if (buffer == null) {
            throw new IllegalArgumentException("buffer is null");
        }
        if (buffer.length != PAGE_SIZE) {
            throw new IllegalArgumentException(
                    "buffer length must be exactly " + PAGE_SIZE + " bytes, got " + buffer.length
            );
        }
        this.pageId = pageId;
        this.buffer = buffer;
        verifySignature();
    }

    @Override
    public byte[] bytes() {
        return buffer;
    }

    @Override
    public int getPageId() {
        return pageId;
    }

    @Override
    public int size() {
        return readUShort(SLOT_COUNT_OFF);
    }

    @Override
    public boolean isValid() {
        return readInt(SIGNATURE_OFF) == SIGNATURE;
    }

    @Override
    public byte[] read(int index) {
        verifySignature();
        int slotCount = readUShort(SLOT_COUNT_OFF);

        if (index < 0 || index >= slotCount) {
            throw new IndexOutOfBoundsException(
                    "slot index out of range: " + index + " (slotCount=" + slotCount + ")"
            );
        }

        int slotPosition = HEADER_SIZE + index * SLOT_SIZE;
        int offset = readUShort(slotPosition);
        int length = readUShort(slotPosition + SHORT_SIZE);

        int upper = readUShort(UPPER_BOUND_OFF);
        if (offset < 0 || offset + length > PAGE_SIZE || offset < upper) {
            throw new IllegalStateException(
                    "corrupted page: invalid data bounds (offset=" + offset +
                            ", length=" + length + ", upperBound=" + upper + ", pageSize=" + PAGE_SIZE + ")"
            );
        }

        byte[] result = new byte[length];
        System.arraycopy(buffer, offset, result, 0, length);
        return result;
    }

    @Override
    public void write(byte[] data) {
        verifySignature();

        if (data == null) {
            throw new IllegalArgumentException("data is null");
        }

        int length = data.length;
        int slotCount = readUShort(SLOT_COUNT_OFF);
        int lower = readUShort(LOWER_BOUND_OFF);
        int upper = readUShort(UPPER_BOUND_OFF);

        int required = SLOT_SIZE + length;
        int freeSpace = upper - lower;

        if (required > freeSpace) {
            throw new IllegalArgumentException(
                    "not enough space: required=" + required +
                            " (data=" + length + " + slot=" + SLOT_SIZE + "), free=" + freeSpace +
                            " (upperLowerGap=" + upper + "-" + lower + ")"
            );
        }

        int newUpper = upper - length;
        System.arraycopy(data, 0, buffer, newUpper, length);

        int slotPosition = HEADER_SIZE + slotCount * SLOT_SIZE;
        writeShort(slotPosition, (short) newUpper);
        writeShort(slotPosition + SHORT_SIZE, (short) length);

        writeShort(SLOT_COUNT_OFF, (short) (slotCount + 1));
        writeShort(LOWER_BOUND_OFF, (short) (lower + SLOT_SIZE));
        writeShort(UPPER_BOUND_OFF, (short) newUpper);
    }

    private void verifySignature() {
        if (!isValid()) {
            throw new IllegalArgumentException("invalid page signature");
        }
    }

    private void initHeaders() {
        writeInt(SIGNATURE_OFF, SIGNATURE);
        writeShort(SLOT_COUNT_OFF, (short) 0);
        writeShort(LOWER_BOUND_OFF, (short) HEADER_SIZE);
        writeShort(UPPER_BOUND_OFF, (short) PAGE_SIZE);
    }

    private int readInt(int position) {
        return ByteBuffer.wrap(buffer, position, INT_SIZE)
                .order(ByteOrder.BIG_ENDIAN)
                .getInt();
    }

    private int readUShort(int position) {
        return Short.toUnsignedInt(
                ByteBuffer.wrap(buffer, position, SHORT_SIZE)
                        .order(ByteOrder.BIG_ENDIAN)
                        .getShort()
        );
    }

    private void writeInt(int position, int value) {
        ByteBuffer.wrap(buffer, position, INT_SIZE)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(value);
    }

    private void writeShort(int position, short value) {
        ByteBuffer.wrap(buffer, position, SHORT_SIZE)
                .order(ByteOrder.BIG_ENDIAN)
                .putShort(value);
    }
}


