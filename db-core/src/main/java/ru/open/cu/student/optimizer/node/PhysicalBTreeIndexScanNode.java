package ru.open.cu.student.optimizer.node;

import ru.open.cu.student.catalog.model.IndexDefinition;
import ru.open.cu.student.catalog.model.TableDefinition;

import java.util.List;
import java.util.Objects;

public record PhysicalBTreeIndexScanNode(
        TableDefinition table,
        IndexDefinition index,
        Object from,
        boolean fromInclusive,
        Object to,
        boolean toInclusive
) implements PhysicalPlanNode {
    public PhysicalBTreeIndexScanNode {
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(index, "index");
        
    }

    @Override
    public String displayName() {
        return "BTreeIndexScan(" + table.getName() + ", idx=" + index.getName() +
                ", from=" + from + (fromInclusive ? " (inc)" : " (exc)") +
                ", to=" + to + (toInclusive ? " (inc)" : " (exc)") +
                ")";
    }

    @Override
    public List<PhysicalPlanNode> children() {
        return List.of();
    }
}


