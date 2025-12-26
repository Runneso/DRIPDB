package ru.open.cu.student.optimizer;

import ru.open.cu.student.optimizer.node.PhysicalPlanNode;
import ru.open.cu.student.planner.node.LogicalPlanNode;

public interface Optimizer {
    PhysicalPlanNode optimize(LogicalPlanNode logicalPlan);
}


