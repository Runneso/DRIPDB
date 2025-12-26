package ru.open.cu.student.storage;

import ru.open.cu.student.catalog.manager.CatalogManager;
import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.catalog.model.TypeDefinition;
import ru.open.cu.student.memory.buffer.BufferPoolManager;
import ru.open.cu.student.memory.buffer.PageKey;
import ru.open.cu.student.memory.model.DataType;
import ru.open.cu.student.memory.page.HeapPage;
import ru.open.cu.student.memory.page.Page;
import ru.open.cu.student.memory.serializer.HeapTupleSerializer;
import ru.open.cu.student.memory.serializer.TupleSerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;


public final class TableHeap {
    
    private static final ConcurrentHashMap<String, Object> FILE_LOCKS = new ConcurrentHashMap<>();

    private final Path root;
    private final BufferPoolManager bufferPool;
    private final CatalogManager catalog;
    private final TupleSerializer serializer;

    private final TableDefinition table;
    private final List<ColumnDefinition> columns;
    private final List<DataType> types;
    private final Object fileLock;

    public TableHeap(Path root, BufferPoolManager bufferPool, CatalogManager catalog, TableDefinition table) {
        this(root, bufferPool, catalog, table, new HeapTupleSerializer());
    }

    public TableHeap(Path root, BufferPoolManager bufferPool, CatalogManager catalog, TableDefinition table, TupleSerializer serializer) {
        this.root = Objects.requireNonNull(root, "root");
        this.bufferPool = Objects.requireNonNull(bufferPool, "bufferPool");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.table = Objects.requireNonNull(table, "table");
        this.serializer = Objects.requireNonNull(serializer, "serializer");

        this.columns = List.copyOf(catalog.getColumns(table));
        this.types = resolveTypes(this.columns);
        this.fileLock = FILE_LOCKS.computeIfAbsent(table.getFileNode(), ignored -> new Object());
    }

    public TableDefinition getTable() {
        return table;
    }

    public List<ColumnDefinition> getColumns() {
        return columns;
    }

    public TID insertRow(List<Object> values) {
        Objects.requireNonNull(values, "values");
        synchronized (fileLock) {
            byte[] rowBytes = RowCodec.encodeRow(values, types, serializer);

            String fileId = table.getFileNode();
            int pages = pageCount(fileId);

            if (pages == 0) {
                PageKey key = new PageKey(fileId, 0);
                Page page = new HeapPage(0);
                bufferPool.newPage(key, page);
                short slotId = (short) page.size();
                page.write(rowBytes);
                bufferPool.updatePage(key, page);
                bufferPool.flushPage(key);
                updatePagesCountIfNeeded(fileId);
                return new TID(0, slotId);
            }

            PageKey lastKey = new PageKey(fileId, pages - 1);
            Page last = bufferPool.getPage(lastKey).getPage();
            try {
                short slotId = (short) last.size();
                last.write(rowBytes);
                bufferPool.updatePage(lastKey, last);
                bufferPool.flushPage(lastKey);
                updatePagesCountIfNeeded(fileId);
                return new TID(pages - 1, slotId);
            } catch (IllegalArgumentException noSpace) {
                int newPageId = pages;
                PageKey newKey = new PageKey(fileId, newPageId);
                Page page = new HeapPage(newPageId);
                bufferPool.newPage(newKey, page);
                short slotId = (short) page.size();
                page.write(rowBytes);
                bufferPool.updatePage(newKey, page);
                bufferPool.flushPage(newKey);
                updatePagesCountIfNeeded(fileId);
                return new TID(newPageId, slotId);
            }
        }
    }

    public List<Object> readRow(TID tid) {
        Objects.requireNonNull(tid, "tid");
        synchronized (fileLock) {
            String fileId = table.getFileNode();
            Page page = bufferPool.getPage(new PageKey(fileId, tid.pageId())).getPage();
            byte[] rowBytes = page.read(Short.toUnsignedInt(tid.slotId()));
            return RowCodec.decodeRow(rowBytes, types, serializer);
        }
    }

    public Iterable<TID> scanTids() {
        String fileId = table.getFileNode();
        int pages;
        synchronized (fileLock) {
            pages = pageCount(fileId);
        }

        return () -> new Iterator<>() {
            int pageId = 0;
            int slotId = 0;
            int slotCount = 0;

            @Override
            public boolean hasNext() {
                advanceToNextNonEmptyPage();
                return pageId < pages && slotId < slotCount;
            }

            @Override
            public TID next() {
                if (!hasNext()) throw new NoSuchElementException();
                TID tid = new TID(pageId, (short) slotId);
                slotId++;
                if (slotId >= slotCount) {
                    pageId++;
                    slotId = 0;
                    slotCount = 0;
                }
                return tid;
            }

            private void advanceToNextNonEmptyPage() {
                while (pageId < pages && slotCount == 0) {
                    Page page;
                    synchronized (fileLock) {
                        page = bufferPool.getPage(new PageKey(fileId, pageId)).getPage();
                        slotCount = page.size();
                    }
                    if (slotCount == 0) {
                        pageId++;
                    }
                }
            }
        };
    }

    private void updatePagesCountIfNeeded(String fileId) {
        int actual = pageCount(fileId);
        if (actual > table.getPagesCount()) {
            catalog.updatePagesCount(table, actual);
            table.setPagesCount(actual);
        }
    }

    private int pageCount(String fileId) {
        Path file = root.resolve(fileId);
        try {
            if (Files.notExists(file)) return 0;
            long size = Files.size(file);
            if (size == 0) return 0;
            if (size % HeapPage.PAGE_SIZE != 0) {
                throw new IllegalStateException("Corrupted heap file (size is not multiple of page size): " + file);
            }
            return (int) (size / HeapPage.PAGE_SIZE);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read file size: " + file, e);
        }
    }

    private List<DataType> resolveTypes(List<ColumnDefinition> columns) {
        List<DataType> out = new ArrayList<>(columns.size());
        for (ColumnDefinition c : columns) {
            TypeDefinition t = catalog.getTypeByOid(c.getTypeOid());
            if (t == null) {
                throw new IllegalStateException("Unknown type oid: " + c.getTypeOid());
            }
            out.add(toDataType(t));
        }
        return out;
    }

    private static DataType toDataType(TypeDefinition type) {
        return switch (type.getName()) {
            case "INT64" -> DataType.INT64;
            case "VARCHAR" -> DataType.VARCHAR;
            default -> throw new IllegalArgumentException("Unsupported type: " + type.getName());
        };
    }
}


