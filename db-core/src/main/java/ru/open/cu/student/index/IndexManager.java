package ru.open.cu.student.index;

import ru.open.cu.student.catalog.manager.CatalogManager;
import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.catalog.model.IndexDefinition;
import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.memory.buffer.BufferPoolManager;
import ru.open.cu.student.storage.TableHeap;
import ru.open.cu.student.storage.TID;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;


public final class IndexManager {
    private final Path root;
    private final BufferPoolManager bufferPool;
    private final CatalogManager catalog;
    private final Map<String, Index> byName = new HashMap<>();

    public IndexManager(Path root, BufferPoolManager bufferPool, CatalogManager catalog) {
        this.root = Objects.requireNonNull(root, "root");
        this.bufferPool = Objects.requireNonNull(bufferPool, "bufferPool");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
    }

    public synchronized Index getOrCreate(IndexDefinition def) {
        Index existing = byName.get(def.getName());
        if (existing != null) return existing;

        Index created = switch (def.getIndexType()) {
            case HASH -> new DiskHashIndex(root, bufferPool, catalog, def);
            case BTREE -> new DiskBTreeIndex(root, bufferPool, catalog, def);
        };
        byName.put(def.getName(), created);
        return created;
    }

    public synchronized Index get(String indexName) {
        Index idx = byName.get(indexName);
        if (idx == null) {
            throw new IllegalStateException("Index instance not loaded: " + indexName);
        }
        return idx;
    }

    public synchronized List<Index> getIndexesForTable(TableDefinition table) {
        List<IndexDefinition> defs = catalog.listIndexes(table);
        List<Index> out = new ArrayList<>(defs.size());
        for (IndexDefinition def : defs) {
            out.add(getOrCreate(def));
        }
        return out;
    }

    public synchronized Index createAndBuild(IndexDefinition def, TableHeap tableHeap, ColumnDefinition indexedColumn) {
        Index idx = getOrCreate(def);

        int pos = indexedColumn.getPosition();
        for (TID tid : tableHeap.scanTids()) {
            List<Object> row = tableHeap.readRow(tid);
            Object key = row.get(pos);
            idx.insert((Comparable<?>) key, tid);
        }

        return idx;
    }
}


