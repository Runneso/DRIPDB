package ru.open.cu.student.execution.executors;

import ru.open.cu.student.execution.Executor;
import ru.open.cu.student.storage.TableHeap;
import ru.open.cu.student.storage.TID;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public final class SeqScanExecutor implements Executor {
    private final TableHeap table;

    private Iterator<TID> iterator;
    private boolean isOpen;

    public SeqScanExecutor(TableHeap table) {
        this.table = Objects.requireNonNull(table, "table");
    }

    @Override
    public void open() {
        this.iterator = table.scanTids().iterator();
        this.isOpen = true;
    }

    @Override
    public List<Object> next() {
        if (!isOpen) throw new IllegalStateException("Executor is not open");
        if (iterator == null || !iterator.hasNext()) return null;
        TID tid = iterator.next();
        return table.readRow(tid);
    }

    @Override
    public void close() {
        isOpen = false;
        iterator = null;
    }
}


