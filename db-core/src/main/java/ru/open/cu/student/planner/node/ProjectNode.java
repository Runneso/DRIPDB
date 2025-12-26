package ru.open.cu.student.planner.node;

import ru.open.cu.student.catalog.model.ColumnDefinition;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public record ProjectNode(LogicalPlanNode child, List<ColumnDefinition> columns) implements LogicalPlanNode {
    public ProjectNode {
        Objects.requireNonNull(child, "child");
        Objects.requireNonNull(columns, "columns");
    }

    @Override
    public String displayName() {
        String cols = columns.stream().map(ColumnDefinition::getName).collect(Collectors.joining(","));
        return "Project(" + cols + ")";
    }

    @Override
    public List<LogicalPlanNode> children() {
        return List.of(child);
    }
}


