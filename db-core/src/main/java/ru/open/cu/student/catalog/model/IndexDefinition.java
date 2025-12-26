package ru.open.cu.student.catalog.model;

import ru.open.cu.student.index.IndexType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public final class IndexDefinition {
    private static final int UINT16_MAX = 0xFFFF;

    private final int oid;
    private final String name;
    private final int tableOid;
    private final int columnOid;
    private final int keyTypeOid;
    private final IndexType indexType;
    private final String fileNode;

    
    private final int metaPageId;
    private final int rootPageId;

    public IndexDefinition(
            int oid,
            String name,
            int tableOid,
            int columnOid,
            int keyTypeOid,
            IndexType indexType,
            String fileNode,
            int metaPageId,
            int rootPageId
    ) {
        this.oid = oid;
        this.name = Objects.requireNonNull(name, "name");
        this.tableOid = tableOid;
        this.columnOid = columnOid;
        this.keyTypeOid = keyTypeOid;
        this.indexType = Objects.requireNonNull(indexType, "indexType");
        this.fileNode = Objects.requireNonNull(fileNode, "fileNode");
        this.metaPageId = metaPageId;
        this.rootPageId = rootPageId;
    }

    public int getOid() {
        return oid;
    }

    public String getName() {
        return name;
    }

    public int getTableOid() {
        return tableOid;
    }

    public int getColumnOid() {
        return columnOid;
    }

    public int getKeyTypeOid() {
        return keyTypeOid;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public String getFileNode() {
        return fileNode;
    }

    public int getMetaPageId() {
        return metaPageId;
    }

    public int getRootPageId() {
        return rootPageId;
    }

    public byte[] toBytes() {
        byte[] nameBytes = utf8(name);
        byte[] fileBytes = utf8(fileNode);
        requireLengthFitsUInt16(nameBytes.length);
        requireLengthFitsUInt16(fileBytes.length);

        int capacity = Integer.BYTES * 7
                + Short.BYTES + nameBytes.length
                + Short.BYTES + fileBytes.length;

        ByteBuffer buf = ByteBuffer.allocate(capacity);
        buf.putInt(oid);
        buf.putInt(tableOid);
        buf.putInt(columnOid);
        buf.putInt(keyTypeOid);
        buf.putInt(indexType.ordinal());
        buf.putInt(metaPageId);
        buf.putInt(rootPageId);
        putString(buf, nameBytes);
        putString(buf, fileBytes);
        return buf.array();
    }

    public static IndexDefinition fromBytes(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);

        int oid = buf.getInt();
        int tableOid = buf.getInt();
        int columnOid = buf.getInt();
        int keyTypeOid = buf.getInt();
        int indexTypeOrd = buf.getInt();
        int metaPageId = buf.getInt();
        int rootPageId = buf.getInt();
        String name = readString(buf);
        String fileNode = readString(buf);

        IndexType type = IndexType.values()[indexTypeOrd];
        return new IndexDefinition(oid, name, tableOid, columnOid, keyTypeOid, type, fileNode, metaPageId, rootPageId);
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
        if (!(o instanceof IndexDefinition that)) return false;
        return oid == that.oid
                && tableOid == that.tableOid
                && columnOid == that.columnOid
                && keyTypeOid == that.keyTypeOid
                && metaPageId == that.metaPageId
                && rootPageId == that.rootPageId
                && name.equals(that.name)
                && indexType == that.indexType
                && fileNode.equals(that.fileNode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(oid, name, tableOid, columnOid, keyTypeOid, indexType, fileNode, metaPageId, rootPageId);
    }
}


