package ru.open.cu.student.catalog.manager;

import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.catalog.model.IndexDefinition;
import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.catalog.model.TypeDefinition;
import ru.open.cu.student.index.IndexType;
import ru.open.cu.student.memory.buffer.BufferPoolManager;
import ru.open.cu.student.memory.buffer.PageKey;
import ru.open.cu.student.memory.page.HeapPage;
import ru.open.cu.student.memory.page.Page;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;


public final class DefaultCatalogManager implements CatalogManager {
    private static final String BUILTIN_INT64 = "INT64";
    private static final String BUILTIN_VARCHAR = "VARCHAR";

    private static final String TABLES_FILE = "table_definitions.dat";
    private static final String COLUMNS_FILE = "column_definitions.dat";
    private static final String TYPES_FILE = "types_definitions.dat";
    private static final String INDEXES_FILE = "index_definitions.dat";

    private final Path root;
    private final BufferPoolManager bufferPool;

    private final Map<Integer, TableDefinition> tablesByOid = new HashMap<>();
    private final Map<String, TableDefinition> tablesByName = new HashMap<>();

    private final Map<Integer, ColumnDefinition> columnsByOid = new HashMap<>();
    private final Map<Integer, List<ColumnDefinition>> columnsByTableOid = new HashMap<>();

    private final Map<Integer, TypeDefinition> typesByOid = new HashMap<>();
    private final Map<String, TypeDefinition> typesByName = new HashMap<>();

    private final Map<Integer, IndexDefinition> indexesByOid = new HashMap<>();
    private final Map<String, IndexDefinition> indexesByName = new HashMap<>();
    private final Map<Integer, List<IndexDefinition>> indexesByTableOid = new HashMap<>();

    private int nextTableOid = 1;
    private int nextColumnOid = 1;
    private int nextTypeOid = 1;
    private int nextIndexOid = 1;

    public DefaultCatalogManager(Path root, BufferPoolManager bufferPool) {
        this.root = Objects.requireNonNull(root, "root");
        this.bufferPool = Objects.requireNonNull(bufferPool, "bufferPool");

        ensureCatalogDirectory();
        loadFromDisk();
        ensureBuiltinTypesPresent();
    }

    private void ensureCatalogDirectory() {
        try {
            Files.createDirectories(root);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create catalog directory: " + root, e);
        }
    }

    private void loadFromDisk() {
        readAllRecords(TYPES_FILE, bytes -> indexType(TypeDefinition.fromBytes(bytes)));
        readAllRecords(TABLES_FILE, bytes -> indexTable(TableDefinition.fromBytes(bytes)));
        readAllRecords(COLUMNS_FILE, bytes -> indexColumn(ColumnDefinition.fromBytes(bytes)));
        readAllRecords(INDEXES_FILE, bytes -> indexIndex(IndexDefinition.fromBytes(bytes)));

        recomputeNextOids();
    }

    private void recomputeNextOids() {
        nextTableOid = nextIdAfterMax(tablesByOid.keySet());
        nextColumnOid = nextIdAfterMax(columnsByOid.keySet());
        nextTypeOid = nextIdAfterMax(typesByOid.keySet());
        nextIndexOid = nextIdAfterMax(indexesByOid.keySet());
    }

    private static int nextIdAfterMax(Collection<Integer> ids) {
        if (ids.isEmpty()) return 1;
        return Collections.max(ids) + 1;
    }

    private interface BytesConsumer {
        void accept(byte[] bytes);
    }

    private void readAllRecords(String fileId, BytesConsumer consumer) {
        Path file = root.resolve(fileId);
        if (Files.notExists(file)) return;

        long size;
        try {
            size = Files.size(file);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read file size: " + file, e);
        }

        if (size == 0) return;
        if (size % HeapPage.PAGE_SIZE != 0) {
            throw new IllegalStateException("Corrupted catalog file (size is not multiple of page size): " + file);
        }

        int pages = (int) (size / HeapPage.PAGE_SIZE);
        for (int pageId = 0; pageId < pages; pageId++) {
            Page page = bufferPool.getPage(new PageKey(fileId, pageId)).getPage();
            for (int recordId = 0; recordId < page.size(); recordId++) {
                consumer.accept(page.read(recordId));
            }
        }
    }

    private int pageCount(String fileId) {
        Path file = root.resolve(fileId);
        try {
            if (Files.notExists(file)) return 0;
            long size = Files.size(file);
            if (size == 0) return 0;
            if (size % HeapPage.PAGE_SIZE != 0) {
                throw new IllegalStateException("Corrupted file (size is not multiple of page size): " + file);
            }
            return (int) (size / HeapPage.PAGE_SIZE);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read file size: " + file, e);
        }
    }

    private void appendRecord(String fileId, byte[] bytes) {
        int pages = pageCount(fileId);

        if (pages == 0) {
            PageKey key = new PageKey(fileId, 0);
            Page page = new HeapPage(0);
            bufferPool.newPage(key, page);
            page.write(bytes);
            bufferPool.updatePage(key, page);
            bufferPool.flushPage(key);
            return;
        }

        PageKey lastKey = new PageKey(fileId, pages - 1);
        Page last = bufferPool.getPage(lastKey).getPage();
        try {
            last.write(bytes);
            bufferPool.updatePage(lastKey, last);
            bufferPool.flushPage(lastKey);
        } catch (IllegalArgumentException noSpace) {
            PageKey newKey = new PageKey(fileId, pages);
            Page page = new HeapPage(pages);
            bufferPool.newPage(newKey, page);
            page.write(bytes);
            bufferPool.updatePage(newKey, page);
            bufferPool.flushPage(newKey);
        }
    }

    private void indexType(TypeDefinition type) {
        typesByOid.put(type.getOid(), type);
        typesByName.put(type.getName(), type);
    }

    private void indexTable(TableDefinition table) {
        tablesByOid.put(table.getOid(), table);
        tablesByName.put(table.getName(), table);
    }

    private void indexColumn(ColumnDefinition column) {
        columnsByOid.put(column.getOid(), column);

        List<ColumnDefinition> list = columnsByTableOid.computeIfAbsent(column.getTableOid(), ignored -> new ArrayList<>());
        list.removeIf(existing -> existing.getOid() == column.getOid());
        list.add(column);
        list.sort(Comparator.comparingInt(ColumnDefinition::getPosition));
    }

    private void indexIndex(IndexDefinition index) {
        indexesByOid.put(index.getOid(), index);
        indexesByName.put(index.getName(), index);

        List<IndexDefinition> list = indexesByTableOid.computeIfAbsent(index.getTableOid(), ignored -> new ArrayList<>());
        list.removeIf(existing -> existing.getOid() == index.getOid());
        list.add(index);
    }

    private void ensureBuiltinTypesPresent() {
        boolean changed = false;

        if (!typesByName.containsKey(BUILTIN_INT64)) {
            TypeDefinition t = new TypeDefinition(nextTypeOid++, BUILTIN_INT64, 8);
            indexType(t);
            appendRecord(TYPES_FILE, t.toBytes());
            changed = true;
        }

        if (!typesByName.containsKey(BUILTIN_VARCHAR)) {
            TypeDefinition t = new TypeDefinition(nextTypeOid++, BUILTIN_VARCHAR, -1);
            indexType(t);
            appendRecord(TYPES_FILE, t.toBytes());
            changed = true;
        }

        if (changed) {
            recomputeNextOids();
        }
    }

    private void requireTableDoesNotExist(String name) {
        if (tablesByName.containsKey(name)) {
            throw new IllegalArgumentException("Table exists: " + name);
        }
    }

    @Override
    public synchronized TableDefinition createTable(String name, List<ColumnDefinition> columns) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(columns, "columns");
        requireTableDoesNotExist(name);

        int oid = nextTableOid++;
        TableDefinition table = new TableDefinition(oid, name, "TABLE", oid + ".dat", 0);
        indexTable(table);
        appendRecord(TABLES_FILE, table.toBytes());

        int position = 0;
        for (ColumnDefinition proto : columns) {
            int colOid = nextColumnOid++;
            ColumnDefinition stored = new ColumnDefinition(
                    colOid,
                    table.getOid(),
                    proto.getTypeOid(),
                    proto.getName(),
                    position++
            );
            indexColumn(stored);
            appendRecord(COLUMNS_FILE, stored.toBytes());
        }

        return table;
    }

    @Override
    public synchronized TableDefinition getTable(String tableName) {
        return tablesByName.get(tableName);
    }

    @Override
    public synchronized List<TableDefinition> listTables() {
        return new ArrayList<>(tablesByOid.values());
    }

    @Override
    public synchronized List<ColumnDefinition> getColumns(TableDefinition table) {
        Objects.requireNonNull(table, "table");
        return columnsByTableOid.getOrDefault(table.getOid(), List.of());
    }

    @Override
    public synchronized ColumnDefinition getColumn(TableDefinition table, String columnName) {
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(columnName, "columnName");
        for (ColumnDefinition c : getColumns(table)) {
            if (c.getName().equals(columnName)) return c;
        }
        return null;
    }

    @Override
    public synchronized TypeDefinition getTypeByOid(int oid) {
        return typesByOid.get(oid);
    }

    @Override
    public synchronized TypeDefinition getTypeByName(String typeName) {
        return typesByName.get(typeName);
    }

    @Override
    public synchronized void updatePagesCount(TableDefinition table, int pagesCount) {
        Objects.requireNonNull(table, "table");

        TableDefinition updated = new TableDefinition(
                table.getOid(),
                table.getName(),
                table.getType(),
                table.getFileNode(),
                pagesCount
        );
        indexTable(updated);
        appendRecord(TABLES_FILE, updated.toBytes());
    }

    @Override
    public synchronized IndexDefinition createIndex(String indexName, String tableName, String columnName, IndexType indexType) {
        Objects.requireNonNull(indexName, "indexName");
        Objects.requireNonNull(tableName, "tableName");
        Objects.requireNonNull(columnName, "columnName");
        Objects.requireNonNull(indexType, "indexType");

        if (indexesByName.containsKey(indexName)) {
            throw new IllegalArgumentException("Index exists: " + indexName);
        }

        TableDefinition table = getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }

        ColumnDefinition column = getColumn(table, columnName);
        if (column == null) {
            throw new IllegalArgumentException("Column not found: " + tableName + "." + columnName);
        }

        int oid = nextIndexOid++;
        String fileNode = oid + ".idx";

        IndexDefinition def = new IndexDefinition(
                oid,
                indexName,
                table.getOid(),
                column.getOid(),
                column.getTypeOid(),
                indexType,
                fileNode,
                0,
                0
        );

        indexIndex(def);
        appendRecord(INDEXES_FILE, def.toBytes());
        return def;
    }

    @Override
    public synchronized IndexDefinition getIndex(String indexName) {
        return indexesByName.get(indexName);
    }

    @Override
    public synchronized List<IndexDefinition> listIndexes(TableDefinition table) {
        Objects.requireNonNull(table, "table");
        return indexesByTableOid.getOrDefault(table.getOid(), List.of());
    }
}


