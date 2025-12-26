package ru.open.cu.student.optimizer.node;

import ru.open.cu.student.sql.semantic.InsertQueryTree;

import java.util.List;
import java.util.Objects;

public record PhysicalInsertNode(InsertQueryTree query) implements PhysicalPlanNode {
    public PhysicalInsertNode {
        Objects.requireNonNull(query, "query");
    }

    @Override
    public String displayName() {
        return "Insert(" + query.table().getName() + ")";
    }

    @Override
    public List<PhysicalPlanNode> children() {
        return List.of();
    }
}


