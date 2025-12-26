package ru.open.cu.student.planner;

import ru.open.cu.student.planner.node.LogicalPlanNode;
import ru.open.cu.student.sql.semantic.QueryTree;

public interface Planner {
    LogicalPlanNode plan(QueryTree queryTree);
}


