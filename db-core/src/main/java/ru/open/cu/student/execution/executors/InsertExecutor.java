package ru.open.cu.student.execution.executors;

import ru.open.cu.student.catalog.manager.CatalogManager;
import ru.open.cu.student.catalog.model.IndexDefinition;
import ru.open.cu.student.execution.Executor;
import ru.open.cu.student.index.Index;
import ru.open.cu.student.index.IndexManager;
import ru.open.cu.student.memory.buffer.BufferPoolManager;
import ru.open.cu.student.sql.semantic.InsertQueryTree;
import ru.open.cu.student.storage.TableHeap;
import ru.open.cu.student.storage.TID;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

public final class InsertExecutor implements Executor {
    private final Path root;
    private final BufferPoolManager bufferPool;
    private final CatalogManager catalog;
    private final IndexManager indexManager;
    private final InsertQueryTree query;

    private boolean executed;

    public InsertExecutor(Path root, BufferPoolManager bufferPool, CatalogManager catalog, IndexManager indexManager, InsertQueryTree query) {
        this.root = Objects.requireNonNull(root, "root");
        this.bufferPool = Objects.requireNonNull(bufferPool, "bufferPool");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.indexManager = Objects.requireNonNull(indexManager, "indexManager");
        this.query = Objects.requireNonNull(query, "query");
    }

    @Override
    public void open() {
        
    }

    @Override
    public List<Object> next() {
        if (executed) return null;
        executed = true;

        TableHeap tableHeap = new TableHeap(root, bufferPool, catalog, query.table());
        TID tid = tableHeap.insertRow(query.values());

        
        List<IndexDefinition> defs = catalog.listIndexes(query.table());
        for (IndexDefinition def : defs) {
            Index idx = indexManager.getOrCreate(def);
            int colPos = query.columns().stream()
                    .filter(c -> c.getOid() == def.getColumnOid())
                    .findFirst()
                    .map(c -> c.getPosition())
                    .orElse(-1);
            if (colPos < 0) continue;
            Object key = query.values().get(colPos);
            idx.insert((Comparable<?>) key, tid);
        }

        return null;
    }

    @Override
    public void close() {
        
    }
}


