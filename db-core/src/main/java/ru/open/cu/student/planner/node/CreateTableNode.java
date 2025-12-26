package ru.open.cu.student.planner.node;

import ru.open.cu.student.sql.semantic.CreateTableQueryTree;

import java.util.List;
import java.util.Objects;

public record CreateTableNode(CreateTableQueryTree query) implements LogicalPlanNode {
    public CreateTableNode {
        Objects.requireNonNull(query, "query");
    }

    @Override
    public String displayName() {
        return "CreateTable(" + query.tableName().text() + ")";
    }

    @Override
    public List<LogicalPlanNode> children() {
        return List.of();
    }
}


