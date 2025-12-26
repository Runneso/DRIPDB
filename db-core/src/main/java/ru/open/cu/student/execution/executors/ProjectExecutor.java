package ru.open.cu.student.execution.executors;

import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.execution.Executor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class ProjectExecutor implements Executor {
    private final Executor child;
    private final List<Integer> positions;

    private boolean isOpen;

    public ProjectExecutor(Executor child, List<ColumnDefinition> columns) {
        this.child = Objects.requireNonNull(child, "child");
        Objects.requireNonNull(columns, "columns");
        this.positions = columns.stream().map(ColumnDefinition::getPosition).toList();
    }

    @Override
    public void open() {
        child.open();
        isOpen = true;
    }

    @Override
    public List<Object> next() {
        if (!isOpen) throw new IllegalStateException("Executor is not open");
        List<Object> row = child.next();
        if (row == null) return null;

        List<Object> out = new ArrayList<>(positions.size());
        for (int pos : positions) {
            out.add(row.get(pos));
        }
        return out;
    }

    @Override
    public void close() {
        isOpen = false;
        child.close();
    }
}


