package ru.open.cu.student.planner.node;

import ru.open.cu.student.sql.semantic.InsertQueryTree;

import java.util.List;
import java.util.Objects;

public record InsertNode(InsertQueryTree query) implements LogicalPlanNode {
    public InsertNode {
        Objects.requireNonNull(query, "query");
    }

    @Override
    public String displayName() {
        return "Insert(" + query.table().getName() + ")";
    }

    @Override
    public List<LogicalPlanNode> children() {
        return List.of();
    }
}


