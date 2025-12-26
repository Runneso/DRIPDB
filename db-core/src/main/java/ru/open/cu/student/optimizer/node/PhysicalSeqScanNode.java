package ru.open.cu.student.optimizer.node;

import ru.open.cu.student.catalog.model.TableDefinition;

import java.util.List;
import java.util.Objects;

public record PhysicalSeqScanNode(TableDefinition table) implements PhysicalPlanNode {
    public PhysicalSeqScanNode {
        Objects.requireNonNull(table, "table");
    }

    @Override
    public String displayName() {
        return "SeqScan(" + table.getName() + ")";
    }

    @Override
    public List<PhysicalPlanNode> children() {
        return List.of();
    }
}


