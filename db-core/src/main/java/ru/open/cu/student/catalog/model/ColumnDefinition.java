package ru.open.cu.student.catalog.model;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public final class ColumnDefinition {
    private static final int UINT16_MAX = 0xFFFF;

    private final int oid;
    private final int tableOid;
    private final int typeOid;
    private final String name;
    private final int position;

    public ColumnDefinition(int oid, int tableOid, int typeOid, String name, int position) {
        this.oid = oid;
        this.tableOid = tableOid;
        this.typeOid = typeOid;
        this.name = Objects.requireNonNull(name, "name");
        this.position = position;
    }

    
    public ColumnDefinition(int typeOid, String name, int position) {
        this(0, 0, typeOid, name, position);
    }

    public int getOid() {
        return oid;
    }

    public int getTableOid() {
        return tableOid;
    }

    public int getTypeOid() {
        return typeOid;
    }

    public String getName() {
        return name;
    }

    public int getPosition() {
        return position;
    }

    public byte[] toBytes() {
        byte[] nameBytes = utf8(name);
        requireLengthFitsUInt16(nameBytes.length);

        int capacity = Integer.BYTES + Integer.BYTES + Integer.BYTES + Short.BYTES + nameBytes.length + Integer.BYTES;
        ByteBuffer buf = ByteBuffer.allocate(capacity);

        buf.putInt(oid);
        buf.putInt(tableOid);
        buf.putInt(typeOid);
        putUInt16(buf, nameBytes.length);
        buf.put(nameBytes);
        buf.putInt(position);

        return buf.array();
    }

    public static ColumnDefinition fromBytes(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);

        int oid = buf.getInt();
        int tableOid = buf.getInt();
        int typeOid = buf.getInt();
        String name = readString(buf);
        int position = buf.getInt();

        return new ColumnDefinition(oid, tableOid, typeOid, name, position);
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
        if (!(o instanceof ColumnDefinition that)) return false;
        return oid == that.oid
                && tableOid == that.tableOid
                && typeOid == that.typeOid
                && position == that.position
                && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(oid, tableOid, typeOid, name, position);
    }
}


