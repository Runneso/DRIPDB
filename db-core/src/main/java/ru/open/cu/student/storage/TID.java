package ru.open.cu.student.storage;

import java.nio.ByteBuffer;


public record TID(int pageId, short slotId) {
    public static final int BYTES = Integer.BYTES + Short.BYTES;

    public void writeTo(ByteBuffer buf) {
        buf.putInt(pageId);
        buf.putShort(slotId);
    }

    public static TID readFrom(ByteBuffer buf) {
        int pageId = buf.getInt();
        short slotId = buf.getShort();
        return new TID(pageId, slotId);
    }
}


