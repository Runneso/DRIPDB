package ru.open.cu.student.optimizer.node;

import java.util.List;
import java.util.Objects;

public record PhysicalExplainNode(PhysicalPlanNode inner) implements PhysicalPlanNode {
    public PhysicalExplainNode {
        Objects.requireNonNull(inner, "inner");
    }

    @Override
    public String displayName() {
        return "Explain";
    }

    @Override
    public List<PhysicalPlanNode> children() {
        return List.of(inner);
    }
}


