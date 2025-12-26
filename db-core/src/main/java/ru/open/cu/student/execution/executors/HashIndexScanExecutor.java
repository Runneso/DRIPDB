package ru.open.cu.student.execution.executors;

import ru.open.cu.student.execution.Executor;
import ru.open.cu.student.index.Index;
import ru.open.cu.student.storage.TableHeap;
import ru.open.cu.student.storage.TID;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public final class HashIndexScanExecutor implements Executor {
    private final Index index;
    private final Comparable<?> searchKey;
    private final TableHeap table;

    private Iterator<TID> tidIterator;
    private boolean isOpen;

    public HashIndexScanExecutor(Index index, Comparable<?> searchKey, TableHeap table) {
        this.index = Objects.requireNonNull(index, "index");
        this.searchKey = Objects.requireNonNull(searchKey, "searchKey");
        this.table = Objects.requireNonNull(table, "table");
    }

    @Override
    public void open() {
        List<TID> tids = index.search(searchKey);
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


