package ru.open.cu.student.execution;

import ru.open.cu.student.optimizer.node.PhysicalPlanNode;

public interface ExecutorFactory {
    Executor createExecutor(PhysicalPlanNode plan);
}


