package ru.open.cu.student.planner;

import ru.open.cu.student.planner.node.*;
import ru.open.cu.student.sql.semantic.*;

import java.util.Objects;

public final class PlannerImpl implements Planner {
    @Override
    public LogicalPlanNode plan(QueryTree queryTree) {
        Objects.requireNonNull(queryTree, "queryTree");

        return switch (queryTree.getType()) {
            case CREATE_TABLE -> new CreateTableNode((CreateTableQueryTree) queryTree);
            case INSERT -> new InsertNode((InsertQueryTree) queryTree);
            case CREATE_INDEX -> new CreateIndexNode((CreateIndexQueryTree) queryTree);
            case SELECT -> planSelect((SelectQueryTree) queryTree);
            case EXPLAIN -> new ExplainNode(plan(((ExplainQueryTree) queryTree).inner()));
        };
    }

    private LogicalPlanNode planSelect(SelectQueryTree q) {
        LogicalPlanNode node = new ScanNode(q.table());
        if (q.filter() != null) {
            node = new FilterNode(node, q.filter());
        }
        node = new ProjectNode(node, q.targetColumns());
        return node;
    }
}


