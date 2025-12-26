package ru.open.cu.student.optimizer.node;

import java.util.List;

public interface PhysicalPlanNode {
    String displayName();

    List<PhysicalPlanNode> children();

    default String pretty() {
        StringBuilder sb = new StringBuilder();
        PhysicalPlanPrinter.print(sb, this, 0);
        return sb.toString();
    }
}


