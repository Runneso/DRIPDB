package ru.open.cu.student.planner.node;

import java.util.List;
import java.util.Objects;

public record ExplainNode(LogicalPlanNode inner) implements LogicalPlanNode {
    public ExplainNode {
        Objects.requireNonNull(inner, "inner");
    }

    @Override
    public String displayName() {
        return "Explain";
    }

    @Override
    public List<LogicalPlanNode> children() {
        return List.of(inner);
    }
}


