package ru.open.cu.student.catalog.model;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public final class TypeDefinition {
    private static final int UINT16_MAX = 0xFFFF;

    private final int oid;
    private final String name;
    private final int byteLength; 

    public TypeDefinition(int oid, String name, int byteLength) {
        this.oid = oid;
        this.name = Objects.requireNonNull(name, "name");
        this.byteLength = byteLength;
    }

    public int getOid() {
        return oid;
    }

    public String getName() {
        return name;
    }

    public int getByteLength() {
        return byteLength;
    }

    public byte[] toBytes() {
        byte[] nameBytes = utf8(name);
        requireLengthFitsUInt16(nameBytes.length);

        ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES + Short.BYTES + nameBytes.length + Integer.BYTES);
        buf.putInt(oid);
        putUInt16(buf, nameBytes.length);
        buf.put(nameBytes);
        buf.putInt(byteLength);
        return buf.array();
    }

    public static TypeDefinition fromBytes(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);

        int oid = buf.getInt();
        String name = readString(buf);
        int byteLength = buf.getInt();

        return new TypeDefinition(oid, name, byteLength);
    }

    private static String readString(ByteBuffer buf) {
        int len = readUInt16(buf);
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static int readUInt16(ByteBuffer buf) {
        return buf.getShort() & UINT16_MAX;
    }

    private static void putUInt16(ByteBuffer buf, int value) {
        buf.putShort((short) value);
    }

    private static void requireLengthFitsUInt16(int len) {
        if (len > UINT16_MAX) {
            throw new IllegalArgumentException("Name too long");
        }
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TypeDefinition that)) return false;
        return oid == that.oid && byteLength == that.byteLength && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(oid, name, byteLength);
    }
}


