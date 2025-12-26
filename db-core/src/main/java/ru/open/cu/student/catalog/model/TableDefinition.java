package ru.open.cu.student.catalog.model;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public final class TableDefinition {
    private static final int UINT16_MAX = 0xFFFF;

    private final int oid;
    private final String name;
    private final String type;
    private final String fileNode;
    private int pagesCount;

    public TableDefinition(int oid, String name, String type, String fileNode, int pagesCount) {
        this.oid = oid;
        this.name = requireNonBlank(name, "name");
        this.type = requireNonBlank(type, "type");
        this.fileNode = requireNonBlank(fileNode, "fileNode");
        this.pagesCount = pagesCount;
    }

    private static String requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field);
        }
        return value;
    }

    public int getOid() {
        return oid;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getFileNode() {
        return fileNode;
    }

    public int getPagesCount() {
        return pagesCount;
    }

    public void setPagesCount(int pagesCount) {
        this.pagesCount = pagesCount;
    }

    public byte[] toBytes() {
        byte[] nameBytes = utf8(name);
        byte[] typeBytes = utf8(type);
        byte[] fileBytes = utf8(fileNode);

        requireLengthFitsUInt16(nameBytes.length);
        requireLengthFitsUInt16(typeBytes.length);
        requireLengthFitsUInt16(fileBytes.length);

        int capacity = Integer.BYTES
                + Short.BYTES + nameBytes.length
                + Short.BYTES + typeBytes.length
                + Short.BYTES + fileBytes.length
                + Integer.BYTES;

        ByteBuffer buf = ByteBuffer.allocate(capacity);
        buf.putInt(oid);
        putString(buf, nameBytes);
        putString(buf, typeBytes);
        putString(buf, fileBytes);
        buf.putInt(pagesCount);
        return buf.array();
    }

    public static TableDefinition fromBytes(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);

        int oid = buf.getInt();
        String name = readString(buf);
        String type = readString(buf);
        String fileNode = readString(buf);
        int pages = buf.getInt();

        return new TableDefinition(oid, name, type, fileNode, pages);
    }

    private static void putString(ByteBuffer buf, byte[] utf8Bytes) {
        putUInt16(buf, utf8Bytes.length);
        buf.put(utf8Bytes);
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
            throw new IllegalArgumentException("String too long");
        }
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TableDefinition that)) return false;
        return oid == that.oid
                && pagesCount == that.pagesCount
                && name.equals(that.name)
                && type.equals(that.type)
                && fileNode.equals(that.fileNode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(oid, name, type, fileNode, pagesCount);
    }
}


