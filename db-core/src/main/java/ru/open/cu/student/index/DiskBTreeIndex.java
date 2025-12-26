package ru.open.cu.student.index;

import ru.open.cu.student.catalog.manager.CatalogManager;
import ru.open.cu.student.catalog.model.IndexDefinition;
import ru.open.cu.student.catalog.model.TypeDefinition;
import ru.open.cu.student.memory.buffer.BufferPoolManager;
import ru.open.cu.student.memory.buffer.PageKey;
import ru.open.cu.student.memory.page.HeapPage;
import ru.open.cu.student.memory.page.Page;
import ru.open.cu.student.storage.TID;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;


public final class DiskBTreeIndex implements Index {
    private static final ByteOrder ORDER = ByteOrder.BIG_ENDIAN;

    private static final int META_MAGIC = 0x42494458; 
    private static final int META_VERSION = 1;
    private static final int NODE_MAGIC = 0x424E4F44; 

    private static final int HEAP_HEADER_SIZE = 10;
    private static final int PAGE_CAPACITY = HeapPage.PAGE_SIZE - HEAP_HEADER_SIZE;

    
    private static final int META_MAGIC_OFF = HEAP_HEADER_SIZE;
    private static final int META_VERSION_OFF = HEAP_HEADER_SIZE + 4;
    private static final int META_ROOT_OFF = HEAP_HEADER_SIZE + 8;
    private static final int META_HEIGHT_OFF = HEAP_HEADER_SIZE + 12;
    private static final int META_LEFTMOST_LEAF_OFF = HEAP_HEADER_SIZE + 16;
    private static final int META_NEXT_PAGE_ID_OFF = HEAP_HEADER_SIZE + 20;

    
    private static final int NODE_MAGIC_OFF = HEAP_HEADER_SIZE;
    private static final int NODE_IS_LEAF_OFF = HEAP_HEADER_SIZE + 4;
    private static final int NODE_PARENT_OFF = HEAP_HEADER_SIZE + 8;
    private static final int NODE_LEFT_SIB_OFF = HEAP_HEADER_SIZE + 12;
    private static final int NODE_RIGHT_SIB_OFF = HEAP_HEADER_SIZE + 16;
    private static final int NODE_KEYCOUNT_OFF = HEAP_HEADER_SIZE + 20;
    private static final int NODE_HDR_SIZE = 24;
    private static final int NODE_DATA_START_OFF = HEAP_HEADER_SIZE + NODE_HDR_SIZE;

    private record Meta(int rootPageId, int height, int leftmostLeafPageId, int nextPageId) {
    }

    private static final class Node {
        final int pageId;
        boolean isLeaf;
        int parentPageId;
        int leftSiblingPageId;
        int rightSiblingPageId;
        final ArrayList<Comparable<?>> keys = new ArrayList<>();

        
        final ArrayList<List<TID>> values = new ArrayList<>();

        
        final ArrayList<Integer> children = new ArrayList<>();

        Node(int pageId) {
            this.pageId = pageId;
        }
    }

    private final Path root;
    private final BufferPoolManager bufferPool;
    private final CatalogManager catalog;
    private final IndexDefinition def;
    private final String fileId;
    private final TypeDefinition keyType;

    private Meta meta;

    public DiskBTreeIndex(Path root, BufferPoolManager bufferPool, CatalogManager catalog, IndexDefinition def) {
        this.root = Objects.requireNonNull(root, "root");
        this.bufferPool = Objects.requireNonNull(bufferPool, "bufferPool");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.def = Objects.requireNonNull(def, "def");
        if (def.getIndexType() != IndexType.BTREE) {
            throw new IllegalArgumentException("Not a BTREE index definition: " + def.getIndexType());
        }
        this.fileId = def.getFileNode();

        TypeDefinition t = catalog.getTypeByOid(def.getKeyTypeOid());
        if (t == null) {
            throw new IllegalStateException("Unknown key type oid: " + def.getKeyTypeOid());
        }
        this.keyType = t;

        initOrLoad();
    }

    @Override
    public IndexDefinition getDefinition() {
        return def;
    }

    @Override
    public synchronized void insert(Comparable<?> key, TID tid) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(tid, "tid");

        Deque<Integer> path = new ArrayDeque<>();
        Node leaf = findLeaf(key, path);

        insertIntoLeaf(leaf, key, tid);

        if (estimateSize(leaf) <= PAGE_CAPACITY) {
            writeNode(leaf);
            return;
        }

        splitLeaf(leaf, path);
    }

    @Override
    public synchronized List<TID> search(Comparable<?> key) {
        Objects.requireNonNull(key, "key");

        Node leaf = findLeaf(key, null);
        int pos = lowerBound(leaf.keys, key);
        if (pos >= leaf.keys.size() || cmp(leaf.keys.get(pos), key) != 0) {
            return List.of();
        }
        return List.copyOf(leaf.values.get(pos));
    }

    @Override
    public synchronized List<TID> rangeSearch(Comparable<?> from, boolean fromInclusive, Comparable<?> to, boolean toInclusive) {
        if (from != null && to != null) {
            if (cmp(from, to) > 0) return List.of();
        }

        int leafPageId;
        int startPos;

        if (from == null) {
            leafPageId = meta.leftmostLeafPageId;
            startPos = 0;
        } else {
            Node leaf = findLeaf(from, null);
            leafPageId = leaf.pageId;
            startPos = lowerBound(leaf.keys, from);
        }

        List<TID> out = new ArrayList<>();

        int currentLeafId = leafPageId;
        int pos = startPos;

        while (currentLeafId != -1) {
            Node leaf = readNode(currentLeafId);
            if (!leaf.isLeaf) {
                throw new IllegalStateException("Expected leaf, got internal: pageId=" + currentLeafId);
            }

            while (pos < leaf.keys.size()) {
                Comparable<?> k = leaf.keys.get(pos);

                if (from != null) {
                    int cFrom = cmp(k, from);
                    if (cFrom < 0 || (!fromInclusive && cFrom == 0)) {
                        pos++;
                        continue;
                    }
                }

                if (to != null) {
                    int cTo = cmp(k, to);
                    if (cTo > 0 || (!toInclusive && cTo == 0)) {
                        return out;
                    }
                }

                out.addAll(leaf.values.get(pos));
                pos++;
            }

            currentLeafId = leaf.rightSiblingPageId;
            pos = 0;
        }

        return out;
    }

    
    int debugHeight() {
        return meta.height;
    }

    int debugRootPageId() {
        return meta.rootPageId;
    }

    
    private void initOrLoad() {
        Path file = root.resolve(fileId);
        try {
            Files.createDirectories(root);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create index directory: " + root, e);
        }

        boolean needsInit = Files.notExists(file);
        if (!needsInit) {
            try {
                needsInit = Files.size(file) == 0;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to stat index file: " + file, e);
            }
        }

        if (needsInit) {
            initializeNew();
        } else {
            loadMeta();
        }
    }

    private void initializeNew() {
        
        Page metaPage = new HeapPage(0);
        bufferPool.newPage(key(0), metaPage);

        
        Page rootLeaf = new HeapPage(1);
        bufferPool.newPage(key(1), rootLeaf);

        meta = new Meta(1, 1, 1, 2);

        Node leaf = new Node(1);
        leaf.isLeaf = true;
        leaf.parentPageId = -1;
        leaf.leftSiblingPageId = -1;
        leaf.rightSiblingPageId = -1;
        writeNode(leaf);

        writeMeta();

        
        bufferPool.flushAllPages();
    }

    private void loadMeta() {
        Page page = bufferPool.getPage(key(0)).getPage();
        byte[] buf = page.bytes();

        int magic = readInt(buf, META_MAGIC_OFF);
        int version = readInt(buf, META_VERSION_OFF);
        if (magic != META_MAGIC) {
            throw new IllegalStateException("Not a BTREE index file (bad magic): " + Integer.toHexString(magic));
        }
        if (version != META_VERSION) {
            throw new IllegalStateException("Unsupported BTREE index version: " + version);
        }

        int rootPageId = readInt(buf, META_ROOT_OFF);
        int height = readInt(buf, META_HEIGHT_OFF);
        int leftmost = readInt(buf, META_LEFTMOST_LEAF_OFF);
        int nextPageId = readInt(buf, META_NEXT_PAGE_ID_OFF);

        meta = new Meta(rootPageId, height, leftmost, nextPageId);
    }

    private void writeMeta() {
        Page page = bufferPool.getPage(key(0)).getPage();
        byte[] buf = page.bytes();

        writeInt(buf, META_MAGIC_OFF, META_MAGIC);
        writeInt(buf, META_VERSION_OFF, META_VERSION);
        writeInt(buf, META_ROOT_OFF, meta.rootPageId);
        writeInt(buf, META_HEIGHT_OFF, meta.height);
        writeInt(buf, META_LEFTMOST_LEAF_OFF, meta.leftmostLeafPageId);
        writeInt(buf, META_NEXT_PAGE_ID_OFF, meta.nextPageId);

        bufferPool.updatePage(key(0), page);
    }

    
    private Node findLeaf(Comparable<?> key, Deque<Integer> pathOut) {
        int currentId = meta.rootPageId;
        while (true) {
            Node n = readNode(currentId);
            if (pathOut != null) pathOut.addLast(currentId);
            if (n.isLeaf) return n;

            int childIdx = childIndex(n.keys, key);
            currentId = n.children.get(childIdx);
        }
    }

    private void insertIntoLeaf(Node leaf, Comparable<?> key, TID tid) {
        int pos = lowerBound(leaf.keys, key);
        if (pos < leaf.keys.size() && cmp(leaf.keys.get(pos), key) == 0) {
            leaf.values.get(pos).add(tid);
            return;
        }
        leaf.keys.add(pos, key);
        List<TID> list = new ArrayList<>();
        list.add(tid);
        leaf.values.add(pos, list);
    }

    private void splitLeaf(Node leaf, Deque<Integer> path) {
        int splitPos = (leaf.keys.size() + 1) / 2;

        Node right = new Node(allocatePageId());
        right.isLeaf = true;
        right.parentPageId = leaf.parentPageId;

        right.keys.addAll(leaf.keys.subList(splitPos, leaf.keys.size()));
        right.values.addAll(leaf.values.subList(splitPos, leaf.values.size()));

        leaf.keys.subList(splitPos, leaf.keys.size()).clear();
        leaf.values.subList(splitPos, leaf.values.size()).clear();

        
        right.rightSiblingPageId = leaf.rightSiblingPageId;
        right.leftSiblingPageId = leaf.pageId;
        leaf.rightSiblingPageId = right.pageId;

        if (right.rightSiblingPageId != -1) {
            Node oldRight = readNode(right.rightSiblingPageId);
            oldRight.leftSiblingPageId = right.pageId;
            writeNode(oldRight);
        }

        writeNode(leaf);
        writeNode(right);

        Comparable<?> separator = right.keys.get(0);

        
        if (path != null && !path.isEmpty()) {
            path.removeLast();
        }

        if (leaf.parentPageId == -1 || path == null || path.isEmpty()) {
            
            Node root = new Node(allocatePageId());
            root.isLeaf = false;
            root.parentPageId = -1;
            root.leftSiblingPageId = -1;
            root.rightSiblingPageId = -1;
            root.keys.add(separator);
            root.children.add(leaf.pageId);
            root.children.add(right.pageId);

            leaf.parentPageId = root.pageId;
            right.parentPageId = root.pageId;
            writeNode(leaf);
            writeNode(right);
            writeNode(root);

            meta = new Meta(root.pageId, meta.height + 1, meta.leftmostLeafPageId, meta.nextPageId);
            writeMeta();
            return;
        }

        int parentId = leaf.parentPageId;
        Node parent = readNode(parentId);
        insertIntoInternal(parent, separator, right.pageId, leaf.pageId);
        writeNode(parent);

        if (estimateSize(parent) > PAGE_CAPACITY) {
            splitInternal(parent, path);
        }
    }

    private void insertIntoInternal(Node parent, Comparable<?> key, int rightChildId, int leftChildId) {
        int leftPos = parent.children.indexOf(leftChildId);
        if (leftPos < 0) {
            leftPos = childIndex(parent.keys, key);
        }

        int keyPos = leftPos;
        parent.keys.add(keyPos, key);
        parent.children.add(leftPos + 1, rightChildId);
    }

    private void splitInternal(Node node, Deque<Integer> path) {
        if (node.isLeaf) throw new IllegalStateException("splitInternal called on leaf");

        int mid = node.keys.size() / 2;
        Comparable<?> separator = node.keys.get(mid);

        Node right = new Node(allocatePageId());
        right.isLeaf = false;
        right.parentPageId = node.parentPageId;

        
        right.keys.addAll(node.keys.subList(mid + 1, node.keys.size()));
        right.children.addAll(node.children.subList(mid + 1, node.children.size()));

        
        node.keys.subList(mid, node.keys.size()).clear();
        node.children.subList(mid + 1, node.children.size()).clear();

        
        for (Integer childId : right.children) {
            Node child = readNode(childId);
            child.parentPageId = right.pageId;
            writeNode(child);
        }

        writeNode(node);
        writeNode(right);

        
        if (path != null && !path.isEmpty()) {
            path.removeLast();
        }

        if (node.parentPageId == -1 || path == null || path.isEmpty()) {
            Node newRoot = new Node(allocatePageId());
            newRoot.isLeaf = false;
            newRoot.parentPageId = -1;
            newRoot.keys.add(separator);
            newRoot.children.add(node.pageId);
            newRoot.children.add(right.pageId);

            node.parentPageId = newRoot.pageId;
            right.parentPageId = newRoot.pageId;
            writeNode(node);
            writeNode(right);
            writeNode(newRoot);

            meta = new Meta(newRoot.pageId, meta.height + 1, meta.leftmostLeafPageId, meta.nextPageId);
            writeMeta();
            return;
        }

        int parentId = node.parentPageId;
        Node parent = readNode(parentId);
        insertIntoInternal(parent, separator, right.pageId, node.pageId);
        writeNode(parent);

        if (estimateSize(parent) > PAGE_CAPACITY) {
            splitInternal(parent, path);
        }
    }

    private int allocatePageId() {
        int id = meta.nextPageId;
        meta = new Meta(meta.rootPageId, meta.height, meta.leftmostLeafPageId, id + 1);
        writeMeta();

        PageKey k = key(id);
        Page page = new HeapPage(id);
        bufferPool.newPage(k, page);
        bufferPool.updatePage(k, page);
        return id;
    }

    
    private Node readNode(int pageId) {
        Page page = bufferPool.getPage(key(pageId)).getPage();
        byte[] buf = page.bytes();

        int magic = readInt(buf, NODE_MAGIC_OFF);
        if (magic != NODE_MAGIC) {
            throw new IllegalStateException("Bad node magic at pageId=" + pageId + ": " + Integer.toHexString(magic));
        }

        Node n = new Node(pageId);
        n.isLeaf = readInt(buf, NODE_IS_LEAF_OFF) != 0;
        n.parentPageId = readInt(buf, NODE_PARENT_OFF);
        n.leftSiblingPageId = readInt(buf, NODE_LEFT_SIB_OFF);
        n.rightSiblingPageId = readInt(buf, NODE_RIGHT_SIB_OFF);
        int keyCount = readInt(buf, NODE_KEYCOUNT_OFF);

        int off = NODE_DATA_START_OFF;
        for (int i = 0; i < keyCount; i++) {
            int len = readUShort(buf, off);
            off += Short.BYTES;
            Comparable<?> key = decodeKey(buf, off, len);
            off += len;
            n.keys.add(key);
        }

        if (n.isLeaf) {
            for (int i = 0; i < keyCount; i++) {
                int tidCount = readInt(buf, off);
                off += Integer.BYTES;
                List<TID> tids = new ArrayList<>(tidCount);
                for (int t = 0; t < tidCount; t++) {
                    TID tid = TID.readFrom(ByteBuffer.wrap(buf, off, TID.BYTES).order(ORDER));
                    off += TID.BYTES;
                    tids.add(tid);
                }
                n.values.add(tids);
            }
        } else {
            int childCount = readInt(buf, off);
            off += Integer.BYTES;
            for (int i = 0; i < childCount; i++) {
                n.children.add(readInt(buf, off));
                off += Integer.BYTES;
            }
        }

        return n;
    }

    private void writeNode(Node node) {
        PageKey k = key(node.pageId);
        Page page = bufferPool.getPage(k).getPage();
        byte[] buf = page.bytes();

        
        java.util.Arrays.fill(buf, HEAP_HEADER_SIZE, HeapPage.PAGE_SIZE, (byte) 0);

        writeInt(buf, NODE_MAGIC_OFF, NODE_MAGIC);
        writeInt(buf, NODE_IS_LEAF_OFF, node.isLeaf ? 1 : 0);
        writeInt(buf, NODE_PARENT_OFF, node.parentPageId);
        writeInt(buf, NODE_LEFT_SIB_OFF, node.leftSiblingPageId);
        writeInt(buf, NODE_RIGHT_SIB_OFF, node.rightSiblingPageId);
        writeInt(buf, NODE_KEYCOUNT_OFF, node.keys.size());

        int off = NODE_DATA_START_OFF;
        for (Comparable<?> key : node.keys) {
            byte[] kb = encodeKey(key);
            if (kb.length > 0xFFFF) throw new IllegalStateException("key too large");
            writeUShort(buf, off, kb.length);
            off += Short.BYTES;
            System.arraycopy(kb, 0, buf, off, kb.length);
            off += kb.length;
        }

        if (node.isLeaf) {
            if (node.values.size() != node.keys.size()) {
                throw new IllegalStateException("leaf values/keys mismatch");
            }
            for (List<TID> tids : node.values) {
                writeInt(buf, off, tids.size());
                off += Integer.BYTES;
                for (TID tid : tids) {
                    ByteBuffer.wrap(buf, off, TID.BYTES).order(ORDER);
                    ByteBuffer b = ByteBuffer.wrap(buf, off, TID.BYTES).order(ORDER);
                    tid.writeTo(b);
                    off += TID.BYTES;
                }
            }
        } else {
            if (node.children.size() != node.keys.size() + 1) {
                throw new IllegalStateException("internal children/keys mismatch");
            }
            writeInt(buf, off, node.children.size());
            off += Integer.BYTES;
            for (Integer child : node.children) {
                writeInt(buf, off, child);
                off += Integer.BYTES;
            }
        }

        if ((off - HEAP_HEADER_SIZE) > PAGE_CAPACITY) {
            throw new IllegalStateException("node serialization exceeded page capacity");
        }

        bufferPool.updatePage(k, page);
    }

    private int estimateSize(Node node) {
        int size = NODE_HDR_SIZE; 
        for (Comparable<?> k : node.keys) {
            size += Short.BYTES + encodeKey(k).length;
        }

        if (node.isLeaf) {
            for (List<TID> tids : node.values) {
                size += Integer.BYTES + tids.size() * TID.BYTES;
            }
        } else {
            size += Integer.BYTES + node.children.size() * Integer.BYTES;
        }
        return size;
    }

    
    private byte[] encodeKey(Comparable<?> key) {
        if (keyType.getName().equals("INT64")) {
            if (!(key instanceof Number n)) {
                throw new IllegalArgumentException("INT64 key expects Number");
            }
            byte[] out = new byte[Long.BYTES];
            ByteBuffer.wrap(out).order(ORDER).putLong(n.longValue());
            return out;
        }
        if (keyType.getName().equals("VARCHAR")) {
            if (!(key instanceof String s)) {
                throw new IllegalArgumentException("VARCHAR key expects String");
            }
            return s.getBytes(StandardCharsets.UTF_8);
        }
        throw new IllegalStateException("Unsupported key type: " + keyType.getName());
    }

    private Comparable<?> decodeKey(byte[] buf, int offset, int len) {
        if (keyType.getName().equals("INT64")) {
            if (len != Long.BYTES) {
                throw new IllegalStateException("INT64 key must be 8 bytes, got " + len);
            }
            return readLong(buf, offset);
        }
        if (keyType.getName().equals("VARCHAR")) {
            return new String(buf, offset, len, StandardCharsets.UTF_8);
        }
        throw new IllegalStateException("Unsupported key type: " + keyType.getName());
    }

    
    private PageKey key(int pageId) {
        return new PageKey(fileId, pageId);
    }

    private static int childIndex(List<Comparable<?>> keys, Comparable<?> key) {
        int i = 0;
        while (i < keys.size() && cmp(key, keys.get(i)) >= 0) {
            i++;
        }
        return i;
    }

    private static int lowerBound(List<Comparable<?>> keys, Comparable<?> key) {
        int lo = 0;
        int hi = keys.size();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (cmp(keys.get(mid), key) < 0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    private static int readInt(byte[] buf, int offset) {
        return ByteBuffer.wrap(buf, offset, Integer.BYTES).order(ORDER).getInt();
    }

    private static long readLong(byte[] buf, int offset) {
        return ByteBuffer.wrap(buf, offset, Long.BYTES).order(ORDER).getLong();
    }

    private static int readUShort(byte[] buf, int offset) {
        return Short.toUnsignedInt(ByteBuffer.wrap(buf, offset, Short.BYTES).order(ORDER).getShort());
    }

    private static void writeInt(byte[] buf, int offset, int v) {
        ByteBuffer.wrap(buf, offset, Integer.BYTES).order(ORDER).putInt(v);
    }

    private static void writeUShort(byte[] buf, int offset, int v) {
        ByteBuffer.wrap(buf, offset, Short.BYTES).order(ORDER).putShort((short) v);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static int cmp(Comparable a, Comparable b) {
        return a.compareTo(b);
    }
}


