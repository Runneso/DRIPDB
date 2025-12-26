package ru.open.cu.student.planner.node;

import java.util.List;

public interface LogicalPlanNode {
    String displayName();

    List<LogicalPlanNode> children();

    default String pretty() {
        StringBuilder sb = new StringBuilder();
        LogicalPlanPrinter.print(sb, this, 0);
        return sb.toString();
    }
}


