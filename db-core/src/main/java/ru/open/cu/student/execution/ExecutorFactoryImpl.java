package ru.open.cu.student.execution;

import ru.open.cu.student.catalog.manager.CatalogManager;
import ru.open.cu.student.execution.executors.*;
import ru.open.cu.student.index.Index;
import ru.open.cu.student.index.IndexManager;
import ru.open.cu.student.memory.buffer.BufferPoolManager;
import ru.open.cu.student.optimizer.node.*;
import ru.open.cu.student.storage.TableHeap;

import java.nio.file.Path;
import java.util.Objects;

public final class ExecutorFactoryImpl implements ExecutorFactory {
    private final Path root;
    private final BufferPoolManager bufferPool;
    private final CatalogManager catalog;
    private final IndexManager indexManager;

    public ExecutorFactoryImpl(Path root, BufferPoolManager bufferPool, CatalogManager catalog, IndexManager indexManager) {
        this.root = Objects.requireNonNull(root, "root");
        this.bufferPool = Objects.requireNonNull(bufferPool, "bufferPool");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.indexManager = Objects.requireNonNull(indexManager, "indexManager");
    }

    @Override
    public Executor createExecutor(PhysicalPlanNode plan) {
        Objects.requireNonNull(plan, "plan");

        if (plan instanceof PhysicalExplainNode ex) {
            
            return createExecutor(ex.inner());
        }

        if (plan instanceof PhysicalCreateTableNode ct) {
            return new CreateTableExecutor(catalog, ct.query());
        }

        if (plan instanceof PhysicalCreateIndexNode ci) {
            return new CreateIndexExecutor(root, bufferPool, catalog, indexManager, ci.query());
        }

        if (plan instanceof PhysicalInsertNode ins) {
            return new InsertExecutor(root, bufferPool, catalog, indexManager, ins.query());
        }

        if (plan instanceof PhysicalProjectNode p) {
            return new ProjectExecutor(createExecutor(p.child()), p.columns());
        }

        if (plan instanceof PhysicalFilterNode f) {
            return new FilterExecutor(createExecutor(f.child()), f.predicate());
        }

        if (plan instanceof PhysicalSeqScanNode scan) {
            TableHeap tableHeap = new TableHeap(root, bufferPool, catalog, scan.table());
            return new SeqScanExecutor(tableHeap);
        }

        if (plan instanceof PhysicalHashIndexScanNode scan) {
            TableHeap tableHeap = new TableHeap(root, bufferPool, catalog, scan.table());
            Index idx = indexManager.getOrCreate(scan.index());
            return new HashIndexScanExecutor(idx, (Comparable<?>) scan.value(), tableHeap);
        }

        if (plan instanceof PhysicalBTreeIndexScanNode scan) {
            TableHeap tableHeap = new TableHeap(root, bufferPool, catalog, scan.table());
            Index idx = indexManager.getOrCreate(scan.index());
            return new BTreeIndexScanExecutor(
                    idx,
                    (Comparable<?>) scan.from(),
                    scan.fromInclusive(),
                    (Comparable<?>) scan.to(),
                    scan.toInclusive(),
                    tableHeap
            );
        }

        throw new UnsupportedOperationException("Unsupported physical node: " + plan.getClass().getSimpleName());
    }
}


