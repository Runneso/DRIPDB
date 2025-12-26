package ru.open.cu.student.optimizer.node;

import ru.open.cu.student.sql.semantic.ResolvedExpr;

import java.util.List;
import java.util.Objects;

public record PhysicalFilterNode(PhysicalPlanNode child, ResolvedExpr predicate) implements PhysicalPlanNode {
    public PhysicalFilterNode {
        Objects.requireNonNull(child, "child");
        Objects.requireNonNull(predicate, "predicate");
    }

    @Override
    public String displayName() {
        return "Filter(" + predicate + ")";
    }

    @Override
    public List<PhysicalPlanNode> children() {
        return List.of(child);
    }
}


