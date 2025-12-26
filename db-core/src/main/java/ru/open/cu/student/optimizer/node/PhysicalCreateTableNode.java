package ru.open.cu.student.optimizer.node;

import ru.open.cu.student.sql.semantic.CreateTableQueryTree;

import java.util.List;
import java.util.Objects;

public record PhysicalCreateTableNode(CreateTableQueryTree query) implements PhysicalPlanNode {
    public PhysicalCreateTableNode {
        Objects.requireNonNull(query, "query");
    }

    @Override
    public String displayName() {
        return "CreateTable(" + query.tableName().text() + ")";
    }

    @Override
    public List<PhysicalPlanNode> children() {
        return List.of();
    }
}


