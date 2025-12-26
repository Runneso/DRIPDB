package ru.open.cu.student.optimizer.node;

import ru.open.cu.student.catalog.model.ColumnDefinition;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public record PhysicalProjectNode(PhysicalPlanNode child, List<ColumnDefinition> columns) implements PhysicalPlanNode {
    public PhysicalProjectNode {
        Objects.requireNonNull(child, "child");
        Objects.requireNonNull(columns, "columns");
    }

    @Override
    public String displayName() {
        String cols = columns.stream().map(ColumnDefinition::getName).collect(Collectors.joining(","));
        return "Project(" + cols + ")";
    }

    @Override
    public List<PhysicalPlanNode> children() {
        return List.of(child);
    }
}


