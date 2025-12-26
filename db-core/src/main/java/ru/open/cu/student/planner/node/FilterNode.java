package ru.open.cu.student.planner.node;

import ru.open.cu.student.sql.semantic.ResolvedExpr;

import java.util.List;
import java.util.Objects;

public record FilterNode(LogicalPlanNode child, ResolvedExpr predicate) implements LogicalPlanNode {
    public FilterNode {
        Objects.requireNonNull(child, "child");
        Objects.requireNonNull(predicate, "predicate");
    }

    @Override
    public String displayName() {
        return "Filter(" + predicate + ")";
    }

    @Override
    public List<LogicalPlanNode> children() {
        return List.of(child);
    }
}


