package ru.open.cu.student.optimizer.node;

import ru.open.cu.student.sql.semantic.CreateIndexQueryTree;

import java.util.List;
import java.util.Objects;

public record PhysicalCreateIndexNode(CreateIndexQueryTree query) implements PhysicalPlanNode {
    public PhysicalCreateIndexNode {
        Objects.requireNonNull(query, "query");
    }

    @Override
    public String displayName() {
        return "CreateIndex(" + query.indexName().text() + ")";
    }

    @Override
    public List<PhysicalPlanNode> children() {
        return List.of();
    }
}


