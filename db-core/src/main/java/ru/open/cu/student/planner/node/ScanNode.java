package ru.open.cu.student.planner.node;

import ru.open.cu.student.catalog.model.TableDefinition;

import java.util.List;
import java.util.Objects;

public record ScanNode(TableDefinition table) implements LogicalPlanNode {
    public ScanNode {
        Objects.requireNonNull(table, "table");
    }

    @Override
    public String displayName() {
        return "Scan(" + table.getName() + ")";
    }

    @Override
    public List<LogicalPlanNode> children() {
        return List.of();
    }
}


