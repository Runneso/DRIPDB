package ru.open.cu.student.planner.node;

import ru.open.cu.student.sql.semantic.CreateIndexQueryTree;

import java.util.List;
import java.util.Objects;

public record CreateIndexNode(CreateIndexQueryTree query) implements LogicalPlanNode {
    public CreateIndexNode {
        Objects.requireNonNull(query, "query");
    }

    @Override
    public String displayName() {
        return "CreateIndex(" + query.indexName().text() + ")";
    }

    @Override
    public List<LogicalPlanNode> children() {
        return List.of();
    }
}


