package ru.open.cu.student.execution.executors;

import ru.open.cu.student.execution.Executor;
import ru.open.cu.student.index.Index;
import ru.open.cu.student.storage.TableHeap;
import ru.open.cu.student.storage.TID;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public final class BTreeIndexScanExecutor implements Executor {
    private final Index index;
    private final Comparable<?> from;
    private final boolean fromInclusive;
    private final Comparable<?> to;
    private final boolean toInclusive;
    private final TableHeap table;

    private Iterator<TID> tidIterator;
    private boolean isOpen;

    public BTreeIndexScanExecutor(Index index, Comparable<?> from, boolean fromInclusive, Comparable<?> to, boolean toInclusive, TableHeap table) {
        this.index = Objects.requireNonNull(index, "index");
        this.from = from;
        this.fromInclusive = fromInclusive;
        this.to = to;
        this.toInclusive = toInclusive;
        this.table = Objects.requireNonNull(table, "table");
    }

    @Override
    public void open() {
        List<TID> tids = index.rangeSearch(from, fromInclusive, to, toInclusive);
        this.tidIterator = tids.iterator();
        this.isOpen = true;
    }

    @Override
    public List<Object> next() {
        if (!isOpen) throw new IllegalStateException("Executor is not open");
        if (tidIterator == null || !tidIterator.hasNext()) return null;
        TID tid = tidIterator.next();
        return table.readRow(tid);
    }

    @Override
    public void close() {
        isOpen = false;
        tidIterator = null;
    }
}


