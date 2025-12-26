package ru.open.cu.student.optimizer.node;

import ru.open.cu.student.catalog.model.IndexDefinition;
import ru.open.cu.student.catalog.model.TableDefinition;

import java.util.List;
import java.util.Objects;

public record PhysicalHashIndexScanNode(TableDefinition table, IndexDefinition index, Object value) implements PhysicalPlanNode {
    public PhysicalHashIndexScanNode {
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(index, "index");
        Objects.requireNonNull(value, "value");
    }

    @Override
    public String displayName() {
        return "HashIndexScan(" + table.getName() + ", idx=" + index.getName() + ", value=" + value + ")";
    }

    @Override
    public List<PhysicalPlanNode> children() {
        return List.of();
    }
}


