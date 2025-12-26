package ru.open.cu.student.execution.executors;

import ru.open.cu.student.catalog.manager.CatalogManager;
import ru.open.cu.student.catalog.model.IndexDefinition;
import ru.open.cu.student.execution.Executor;
import ru.open.cu.student.index.IndexManager;
import ru.open.cu.student.memory.buffer.BufferPoolManager;
import ru.open.cu.student.sql.semantic.CreateIndexQueryTree;
import ru.open.cu.student.storage.TableHeap;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

public final class CreateIndexExecutor implements Executor {
    private final Path root;
    private final BufferPoolManager bufferPool;
    private final CatalogManager catalog;
    private final IndexManager indexManager;
    private final CreateIndexQueryTree query;

    private boolean executed;

    public CreateIndexExecutor(Path root, BufferPoolManager bufferPool, CatalogManager catalog, IndexManager indexManager, CreateIndexQueryTree query) {
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

        IndexDefinition def = catalog.createIndex(
                query.indexName().text(),
                query.table().getName(),
                query.column().getName(),
                query.indexType()
        );

        TableHeap tableHeap = new TableHeap(root, bufferPool, catalog, query.table());
        indexManager.createAndBuild(def, tableHeap, query.column());

        return null;
    }

    @Override
    public void close() {
        
    }
}


