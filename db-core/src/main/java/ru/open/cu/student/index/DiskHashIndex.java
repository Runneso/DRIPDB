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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


public final class DiskHashIndex implements Index {
    private static final ByteOrder ORDER = ByteOrder.BIG_ENDIAN;
    private static final int MAGIC = 0x48494458; 
    private static final int VERSION = 1;

    
    private static final int HEAP_HEADER_SIZE = 10;
    private static final int DIR_PAGES = 64;
    private static final int DIR_ENTRY_BYTES = Integer.BYTES;
    private static final int DIR_ENTRIES_PER_PAGE = (HeapPage.PAGE_SIZE - HEAP_HEADER_SIZE) / DIR_ENTRY_BYTES;
    private static final int DATA_START_PAGE = 1 + DIR_PAGES;

    
    private static final int BUCKET_HDR_NEXT_OVERFLOW_OFF = HEAP_HEADER_SIZE;
    private static final int BUCKET_HDR_ENTRY_COUNT_OFF = HEAP_HEADER_SIZE + 4;
    private static final int BUCKET_HDR_FREE_OFF = HEAP_HEADER_SIZE + 8;
    private static final int BUCKET_HDR_SIZE = 12;
    private static final int BUCKET_DATA_START_OFF = HEAP_HEADER_SIZE + BUCKET_HDR_SIZE;

    
    private static final int META_MAGIC_OFF = HEAP_HEADER_SIZE;
    private static final int META_VERSION_OFF = HEAP_HEADER_SIZE + 4;
    private static final int META_BUCKET_COUNT_OFF = HEAP_HEADER_SIZE + 8;
    private static final int META_LOWMASK_OFF = HEAP_HEADER_SIZE + 12;
    private static final int META_HIGHMASK_OFF = HEAP_HEADER_SIZE + 16;
    private static final int META_SPLITPTR_OFF = HEAP_HEADER_SIZE + 20;
    private static final int META_MAXBUCKET_OFF = HEAP_HEADER_SIZE + 24;
    private static final int META_RECORDCOUNT_OFF = HEAP_HEADER_SIZE + 28; 
    private static final int META_NEXT_PAGE_ID_OFF = HEAP_HEADER_SIZE + 36;

    private static final double MAX_LOAD_FACTOR = 0.75;
    private static final int TARGET_BUCKET_ENTRIES = 64;

    private final Path root;
    private final BufferPoolManager bufferPool;
    private final CatalogManager catalog;
    private final IndexDefinition def;

    private final String fileId;
    private final TypeDefinition keyType;

    private Meta meta;

    private record Meta(int bucketCount, int lowmask, int highmask, int splitPointer, int maxBucket, long recordCount, int nextPageId) {
    }

    private record Entry(int hash, Comparable<?> key, TID tid) {
    }

    public DiskHashIndex(Path root, BufferPoolManager bufferPool, CatalogManager catalog, IndexDefinition def) {
        this.root = Objects.requireNonNull(root, "root");
        this.bufferPool = Objects.requireNonNull(bufferPool, "bufferPool");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.def = Objects.requireNonNull(def, "def");
        if (def.getIndexType() != IndexType.HASH) {
            throw new IllegalArgumentException("Not a HASH index definition: " + def.getIndexType());
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

        int hash = hashFunction(key);
        int bucketId = computeBucket(hash);
        insertIntoBucketChain(bucketId, new Entry(hash, key, tid));

        meta = new Meta(meta.bucketCount, meta.lowmask, meta.highmask, meta.splitPointer, meta.maxBucket, meta.recordCount + 1, meta.nextPageId);
        writeMeta();

        while (loadFactor() > MAX_LOAD_FACTOR) {
            performSplit();
        }
    }

    @Override
    public synchronized List<TID> search(Comparable<?> key) {
        Objects.requireNonNull(key, "key");

        int hash = hashFunction(key);
        int bucketId = computeBucket(hash);

        List<TID> out = new ArrayList<>();
        forEachEntry(bucketId, e -> {
            if (e.hash == hash && cmp(key, e.key) == 0) {
                out.add(e.tid);
            }
        });
        return out;
    }

    @Override
    public List<TID> rangeSearch(Comparable<?> from, boolean fromInclusive, Comparable<?> to, boolean toInclusive) {
        throw new UnsupportedOperationException("HASH index does not support rangeSearch");
    }

    
    int debugBucketCount() {
        return meta.bucketCount;
    }

    long debugRecordCount() {
        return meta.recordCount;
    }

    
    private void initOrLoad() {
        Path file = root.resolve(fileId);
        try {
            Files.createDirectories(root);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create index directory: " + root, e);
        }

        if (Files.notExists(file)) {
            initializeNew();
            return;
        }

        try {
            if (Files.size(file) == 0) {
                initializeNew();
                return;
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to stat index file: " + file, e);
        }

        loadMeta();
    }

    private void initializeNew() {
        
        newAndFlushPage(0, page -> {
            writeInt(page.bytes(), META_MAGIC_OFF, MAGIC);
            writeInt(page.bytes(), META_VERSION_OFF, VERSION);
        });

        for (int i = 1; i <= DIR_PAGES; i++) {
            newAndFlushPage(i, ignored -> {
                
            });
        }

        
        int lowmask = 16 - 1;
        int highmask = lowmask;
        int splitPointer = 0;
        int maxBucket = 16 - 1;
        int bucketCount = maxBucket + 1;
        long recordCount = 0L;
        int nextPageId = DATA_START_PAGE;
        meta = new Meta(bucketCount, lowmask, highmask, splitPointer, maxBucket, recordCount, nextPageId);

        
        for (int bucketId = 0; bucketId < bucketCount; bucketId++) {
            int headPageId = allocateDataPage();
            initBucketPage(headPageId);
            setBucketHeadPageId(bucketId, headPageId);
        }

        writeMeta();
    }

    private void loadMeta() {
        Page metaPage = bufferPool.getPage(key(0)).getPage();
        byte[] buf = metaPage.bytes();

        int magic = readInt(buf, META_MAGIC_OFF);
        int version = readInt(buf, META_VERSION_OFF);
        if (magic != MAGIC) {
            throw new IllegalStateException("Not a HASH index file (bad magic): " + Integer.toHexString(magic));
        }
        if (version != VERSION) {
            throw new IllegalStateException("Unsupported HASH index version: " + version);
        }

        int bucketCount = readInt(buf, META_BUCKET_COUNT_OFF);
        int lowmask = readInt(buf, META_LOWMASK_OFF);
        int highmask = readInt(buf, META_HIGHMASK_OFF);
        int splitPointer = readInt(buf, META_SPLITPTR_OFF);
        int maxBucket = readInt(buf, META_MAXBUCKET_OFF);
        long recordCount = readLong(buf, META_RECORDCOUNT_OFF);
        int nextPageId = readInt(buf, META_NEXT_PAGE_ID_OFF);

        meta = new Meta(bucketCount, lowmask, highmask, splitPointer, maxBucket, recordCount, nextPageId);
    }

    private void writeMeta() {
        Page page = bufferPool.getPage(key(0)).getPage();
        byte[] buf = page.bytes();

        writeInt(buf, META_MAGIC_OFF, MAGIC);
        writeInt(buf, META_VERSION_OFF, VERSION);
        writeInt(buf, META_BUCKET_COUNT_OFF, meta.bucketCount);
        writeInt(buf, META_LOWMASK_OFF, meta.lowmask);
        writeInt(buf, META_HIGHMASK_OFF, meta.highmask);
        writeInt(buf, META_SPLITPTR_OFF, meta.splitPointer);
        writeInt(buf, META_MAXBUCKET_OFF, meta.maxBucket);
        writeLong(buf, META_RECORDCOUNT_OFF, meta.recordCount);
        writeInt(buf, META_NEXT_PAGE_ID_OFF, meta.nextPageId);

        bufferPool.updatePage(key(0), page);
    }

    private double loadFactor() {
        return meta.recordCount / (double) (Math.max(1, meta.bucketCount) * TARGET_BUCKET_ENTRIES);
    }

    private int hashFunction(Comparable<?> key) {
        return key.hashCode() & 0x7fffffff;
    }

    private int computeBucket(int hash) {
        int bucket = hash & meta.highmask;
        if (bucket > meta.maxBucket) {
            bucket = hash & meta.lowmask;
        }
        return bucket;
    }

    private void performSplit() {
        int splitBucketId = meta.splitPointer;
        int newBucketId = meta.maxBucket + 1;

        if (newBucketId >= DIR_PAGES * DIR_ENTRIES_PER_PAGE) {
            throw new IllegalStateException("Bucket directory capacity exceeded: bucketId=" + newBucketId);
        }

        int newHeadPageId = allocateDataPage();
        initBucketPage(newHeadPageId);
        setBucketHeadPageId(newBucketId, newHeadPageId);

        int newMaxBucket = meta.maxBucket + 1;
        int newBucketCount = newMaxBucket + 1;

        int newHighmask = meta.highmask;
        if (newMaxBucket > newHighmask) {
            newHighmask = (newHighmask << 1) | 1;
        }

        meta = new Meta(newBucketCount, meta.lowmask, newHighmask, meta.splitPointer, newMaxBucket, meta.recordCount, meta.nextPageId);
        writeMeta();

        
        List<Entry> moved = drainBucketChain(splitBucketId);
        for (Entry e : moved) {
            int target = computeBucket(e.hash);
            insertIntoBucketChain(target, e, false);
        }

        int nextSplitPointer = meta.splitPointer + 1;
        int newLowmask = meta.lowmask;

        if (nextSplitPointer == meta.lowmask + 1) {
            
            newLowmask = meta.highmask;
            nextSplitPointer = 0;
        }

        meta = new Meta(meta.bucketCount, newLowmask, meta.highmask, nextSplitPointer, meta.maxBucket, meta.recordCount, meta.nextPageId);
        writeMeta();
    }

    private List<Entry> drainBucketChain(int bucketId) {
        List<Entry> out = new ArrayList<>();

        int head = getBucketHeadPageId(bucketId);
        int current = head;
        while (current != -1 && current != 0) {
            Page page = bufferPool.getPage(key(current)).getPage();
            readAllEntriesFromPage(page, out);
            int next = readInt(page.bytes(), BUCKET_HDR_NEXT_OVERFLOW_OFF);
            current = next;
        }

        
        resetBucketPage(head);
        return out;
    }

    private void insertIntoBucketChain(int bucketId, Entry entry) {
        insertIntoBucketChain(bucketId, entry, true);
    }

    private void insertIntoBucketChain(int bucketId, Entry entry, boolean updateRecordCount) {
        int current = getBucketHeadPageId(bucketId);
        if (current == 0) {
            throw new IllegalStateException("Bucket head is not initialized: bucketId=" + bucketId);
        }

        byte[] entryBytes = encodeEntry(entry);

        while (true) {
            PageKey k = key(current);
            Page page = bufferPool.getPage(k).getPage();

            int free = readInt(page.bytes(), BUCKET_HDR_FREE_OFF);
            if (free == 0) {
                initBucketPage(current);
                page = bufferPool.getPage(k).getPage();
                free = readInt(page.bytes(), BUCKET_HDR_FREE_OFF);
            }

            if (free + entryBytes.length <= HeapPage.PAGE_SIZE) {
                System.arraycopy(entryBytes, 0, page.bytes(), free, entryBytes.length);
                writeInt(page.bytes(), BUCKET_HDR_FREE_OFF, free + entryBytes.length);
                int cnt = readInt(page.bytes(), BUCKET_HDR_ENTRY_COUNT_OFF);
                writeInt(page.bytes(), BUCKET_HDR_ENTRY_COUNT_OFF, cnt + 1);

                bufferPool.updatePage(k, page);
                return;
            }

            int next = readInt(page.bytes(), BUCKET_HDR_NEXT_OVERFLOW_OFF);
            if (next == -1) {
                int overflowPageId = allocateDataPage();
                initBucketPage(overflowPageId);
                writeInt(page.bytes(), BUCKET_HDR_NEXT_OVERFLOW_OFF, overflowPageId);
                bufferPool.updatePage(k, page);
                current = overflowPageId;
            } else {
                current = next;
            }
        }
    }

    private void forEachEntry(int bucketId, java.util.function.Consumer<Entry> consumer) {
        int head = getBucketHeadPageId(bucketId);
        int current = head;
        while (current != -1 && current != 0) {
            Page page = bufferPool.getPage(key(current)).getPage();
            forEachEntryInPage(page, consumer);
            int next = readInt(page.bytes(), BUCKET_HDR_NEXT_OVERFLOW_OFF);
            current = next;
        }
    }

    private void forEachEntryInPage(Page page, java.util.function.Consumer<Entry> consumer) {
        byte[] buf = page.bytes();
        int cnt = readInt(buf, BUCKET_HDR_ENTRY_COUNT_OFF);
        int off = BUCKET_DATA_START_OFF;
        for (int i = 0; i < cnt; i++) {
            Decoded decoded = decodeEntry(buf, off);
            consumer.accept(decoded.entry);
            off = decoded.nextOffset;
        }
    }

    private void readAllEntriesFromPage(Page page, List<Entry> out) {
        forEachEntryInPage(page, out::add);
    }

    private byte[] encodeEntry(Entry entry) {
        Objects.requireNonNull(entry, "entry");
        if (keyType.getName().equals("INT64")) {
            if (!(entry.key instanceof Number n)) {
                throw new IllegalArgumentException("INT64 key expects Number");
            }
            ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES + Long.BYTES + TID.BYTES).order(ORDER);
            buf.putInt(entry.hash);
            buf.putLong(n.longValue());
            entry.tid.writeTo(buf);
            return buf.array();
        }
        if (keyType.getName().equals("VARCHAR")) {
            if (!(entry.key instanceof String s)) {
                throw new IllegalArgumentException("VARCHAR key expects String");
            }
            byte[] utf8 = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            if (utf8.length > 0xFFFF) {
                throw new IllegalArgumentException("VARCHAR key too large: " + utf8.length);
            }
            ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES + Short.BYTES + utf8.length + TID.BYTES).order(ORDER);
            buf.putInt(entry.hash);
            buf.putShort((short) utf8.length);
            buf.put(utf8);
            entry.tid.writeTo(buf);
            return buf.array();
        }
        throw new IllegalStateException("Unsupported key type: " + keyType.getName());
    }

    private record Decoded(Entry entry, int nextOffset) {
    }

    private Decoded decodeEntry(byte[] buf, int offset) {
        int off = offset;
        int hash = readInt(buf, off);
        off += Integer.BYTES;

        Comparable<?> key;
        if (keyType.getName().equals("INT64")) {
            long v = readLong(buf, off);
            off += Long.BYTES;
            key = v;
        } else if (keyType.getName().equals("VARCHAR")) {
            int len = readUShort(buf, off);
            off += Short.BYTES;
            String s = new String(buf, off, len, java.nio.charset.StandardCharsets.UTF_8);
            off += len;
            key = s;
        } else {
            throw new IllegalStateException("Unsupported key type: " + keyType.getName());
        }

        TID tid = TID.readFrom(ByteBuffer.wrap(buf, off, TID.BYTES).order(ORDER));
        off += TID.BYTES;

        return new Decoded(new Entry(hash, key, tid), off);
    }

    private void initBucketPage(int pageId) {
        PageKey k = key(pageId);
        Page page;
        try {
            page = bufferPool.getPage(k).getPage();
        } catch (Exception ignored) {
            page = new HeapPage(pageId);
            bufferPool.newPage(k, page);
        }

        writeInt(page.bytes(), BUCKET_HDR_NEXT_OVERFLOW_OFF, -1);
        writeInt(page.bytes(), BUCKET_HDR_ENTRY_COUNT_OFF, 0);
        writeInt(page.bytes(), BUCKET_HDR_FREE_OFF, BUCKET_DATA_START_OFF);

        bufferPool.updatePage(k, page);
    }

    private void resetBucketPage(int pageId) {
        PageKey k = key(pageId);
        Page page = bufferPool.getPage(k).getPage();
        writeInt(page.bytes(), BUCKET_HDR_NEXT_OVERFLOW_OFF, -1);
        writeInt(page.bytes(), BUCKET_HDR_ENTRY_COUNT_OFF, 0);
        writeInt(page.bytes(), BUCKET_HDR_FREE_OFF, BUCKET_DATA_START_OFF);
        bufferPool.updatePage(k, page);
    }

    private int allocateDataPage() {
        int id = meta.nextPageId;
        meta = new Meta(meta.bucketCount, meta.lowmask, meta.highmask, meta.splitPointer, meta.maxBucket, meta.recordCount, id + 1);
        writeMeta();

        PageKey k = key(id);
        Page page = new HeapPage(id);
        bufferPool.newPage(k, page);
        bufferPool.updatePage(k, page);
        return id;
    }

    private void setBucketHeadPageId(int bucketId, int headPageId) {
        int dirPageId = 1 + (bucketId / DIR_ENTRIES_PER_PAGE);
        int slot = bucketId % DIR_ENTRIES_PER_PAGE;
        int off = HEAP_HEADER_SIZE + slot * Integer.BYTES;

        PageKey k = key(dirPageId);
        Page page = bufferPool.getPage(k).getPage();
        writeInt(page.bytes(), off, headPageId);
        bufferPool.updatePage(k, page);
    }

    private int getBucketHeadPageId(int bucketId) {
        int dirPageId = 1 + (bucketId / DIR_ENTRIES_PER_PAGE);
        int slot = bucketId % DIR_ENTRIES_PER_PAGE;
        int off = HEAP_HEADER_SIZE + slot * Integer.BYTES;

        Page page = bufferPool.getPage(key(dirPageId)).getPage();
        return readInt(page.bytes(), off);
    }

    private void newAndFlushPage(int pageId, java.util.function.Consumer<Page> init) {
        PageKey k = key(pageId);
        Page page = new HeapPage(pageId);
        bufferPool.newPage(k, page);
        init.accept(page);
        bufferPool.updatePage(k, page);
        bufferPool.flushPage(k);
    }

    private PageKey key(int pageId) {
        return new PageKey(fileId, pageId);
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

    private static void writeLong(byte[] buf, int offset, long v) {
        ByteBuffer.wrap(buf, offset, Long.BYTES).order(ORDER).putLong(v);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static int cmp(Comparable a, Comparable b) {
        return a.compareTo(b);
    }
}


